"""CLI workflow for validation, remediation and dedupe operations."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Sequence, Tuple
from urllib import error as urllib_error
from urllib import request as urllib_request

from scripts.validation_collectors import (
    collect_expected_from_webhook_objects,
    collect_stage_records_from_bigquery_rows,
    collect_tiny_api_stage_records,
)
from scripts.validation_deduplication import DuplicateCandidate, build_dedupe_report
from scripts.validation_identity import EventIdentity, StageRecord, normalize_timestamp
from scripts.validation_policy import build_action_payload, resolve_policy
from scripts.validation_reconciliation import build_dual_reconciliation_reports
from scripts.validation_remediation import RemediationAction, RemediationRunResult, run_remediation_loop

RuntimeLoader = Callable[[argparse.Namespace], tuple[List[EventIdentity], Dict[str, List[StageRecord]], Dict[str, Any]]]


def _vprint(args: argparse.Namespace, message: str) -> None:
    if getattr(args, "verbose", False):
        print(f"[validation-cli] {message}", file=sys.stderr, flush=True)


def _load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _ensure_output_dir(path: str) -> Path:
    output = Path(path)
    output.mkdir(parents=True, exist_ok=True)
    return output


def _write_json(path: Path, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def _report_to_dict(report: Any) -> Any:
    if isinstance(report, list):
        return [_report_to_dict(item) for item in report]
    if isinstance(report, dict):
        return {k: _report_to_dict(v) for k, v in report.items()}
    if hasattr(report, "__dataclass_fields__"):
        return {k: _report_to_dict(v) for k, v in asdict(report).items()}
    return report


def _filter_identity(identity: EventIdentity, *, store_prefix: str | None, start_date: str | None, end_date: str | None) -> bool:
    if store_prefix and identity.store_prefix != store_prefix:
        return False

    if start_date or end_date:
        dt = datetime.fromisoformat(normalize_timestamp(identity.event_timestamp).replace("Z", "+00:00"))
        if start_date and dt < datetime.fromisoformat(normalize_timestamp(start_date).replace("Z", "+00:00")):
            return False
        if end_date and dt > datetime.fromisoformat(normalize_timestamp(end_date).replace("Z", "+00:00")):
            return False

    return True


def _parse_expected_identities(items: Sequence[Mapping[str, Any]], *, store_prefix: str | None, start_date: str | None, end_date: str | None) -> List[EventIdentity]:
    out: List[EventIdentity] = []
    for item in items:
        identity = EventIdentity(
            store_prefix=str(item["store_prefix"]),
            uuid=str(item["uuid"]),
            dados_id=str(item["dados_id"]),
            pedido_id=(str(item["pedido_id"]) if item.get("pedido_id") not in (None, "") else None),
            event_timestamp=str(item["event_timestamp"]),
        )
        if _filter_identity(identity, store_prefix=store_prefix, start_date=start_date, end_date=end_date):
            out.append(identity)
    return out


def _parse_stage_records(data: Mapping[str, Sequence[Mapping[str, Any]]], *, store_prefix: str | None, start_date: str | None, end_date: str | None) -> Dict[str, List[StageRecord]]:
    out: Dict[str, List[StageRecord]] = {}
    for stage_name, rows in data.items():
        stage_items: List[StageRecord] = []
        for row in rows:
            identity = EventIdentity(
                store_prefix=str(row["store_prefix"]),
                uuid=str(row["uuid"]),
                dados_id=str(row["dados_id"]),
                pedido_id=(str(row["pedido_id"]) if row.get("pedido_id") not in (None, "") else None),
                event_timestamp=str(row["event_timestamp"]),
            )
            if _filter_identity(identity, store_prefix=store_prefix, start_date=start_date, end_date=end_date):
                stage_items.append(
                    StageRecord(
                        identity=identity,
                        stage_name=stage_name,
                        exists=bool(row.get("exists", True)),
                        metadata={str(k): str(v) for k, v in row.get("metadata", {}).items()},
                    )
                )
        out[stage_name] = stage_items
    return out


def _parse_stage_query_specs(values: Sequence[str] | None) -> List[Tuple[str, str]]:
    specs: List[Tuple[str, str]] = []
    for item in values or []:
        if "::" not in item:
            raise ValueError(f"Invalid --stage-query value (expected stage::SQL): {item}")
        stage, query = item.split("::", 1)
        if not stage.strip() or not query.strip():
            raise ValueError(f"Invalid --stage-query value (blank stage or SQL): {item}")
        specs.append((stage.strip(), query.strip()))
    return specs


def _parse_stage_value_map(values: Sequence[str] | None, *, option_name: str) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for item in values or []:
        if "::" not in item:
            raise ValueError(f"Invalid {option_name} value (expected stage::value): {item}")
        stage, value = item.split("::", 1)
        if not stage.strip() or not value.strip():
            raise ValueError(f"Invalid {option_name} value (blank stage or value): {item}")
        mapping[stage.strip()] = value.strip()
    return mapping


def _filter_expected_identities(identities: Sequence[EventIdentity], *, store_prefix: str | None, start_date: str | None, end_date: str | None) -> List[EventIdentity]:
    return [i for i in identities if _filter_identity(i, store_prefix=store_prefix, start_date=start_date, end_date=end_date)]


def _filter_stage_records(records_by_stage: Mapping[str, Sequence[StageRecord]], *, store_prefix: str | None, start_date: str | None, end_date: str | None) -> Dict[str, List[StageRecord]]:
    out: Dict[str, List[StageRecord]] = {}
    for stage_name, records in records_by_stage.items():
        out[stage_name] = [
            r for r in records if _filter_identity(r.identity, store_prefix=store_prefix, start_date=start_date, end_date=end_date)
        ]
    return out


def _load_live_data(args: argparse.Namespace) -> tuple[List[EventIdentity], Dict[str, List[StageRecord]], Dict[str, Any]]:
    _vprint(args, "Starting live data load")
    if not args.webhook_bucket:
        raise ValueError("--webhook-bucket is required in --mode live")
    if not args.tiny_api_bucket:
        raise ValueError("--tiny-api-bucket is required in --mode live")

    stage_queries = _parse_stage_query_specs(args.stage_query)
    if not stage_queries:
        raise ValueError("At least one --stage-query stage::SQL is required in --mode live")

    from google.cloud import bigquery, storage

    _vprint(args, f"Initializing GCP clients (project={args.gcp_project or 'default'})")
    storage_client = storage.Client(project=args.gcp_project)
    bigquery_client = bigquery.Client(project=args.gcp_project)

    webhook_objects: List[Dict[str, Any]] = []
    _vprint(args, f"Listing webhook objects: bucket={args.webhook_bucket}, prefix={args.webhook_prefix}")
    for idx, blob in enumerate(storage_client.list_blobs(args.webhook_bucket, prefix=args.webhook_prefix), start=1):
        webhook_objects.append({"name": blob.name, "payload_bytes": blob.download_as_bytes()})
        if args.verbose and idx % args.verbose_every == 0:
            _vprint(args, f"Webhook objects downloaded: {idx}")
    _vprint(args, f"Webhook objects total: {len(webhook_objects)}")
    expected, webhook_issues = collect_expected_from_webhook_objects(webhook_objects)
    _vprint(args, f"Expected identities parsed: {len(expected)} (issues: {len(webhook_issues)})")

    _vprint(args, f"Listing tiny-api objects: bucket={args.tiny_api_bucket}, prefix={args.tiny_api_prefix}")
    tiny_api_names: List[str] = []
    for idx, blob in enumerate(storage_client.list_blobs(args.tiny_api_bucket, prefix=args.tiny_api_prefix), start=1):
        tiny_api_names.append(blob.name)
        if args.verbose and idx % args.verbose_every == 0:
            _vprint(args, f"Tiny-api objects listed: {idx}")
    _vprint(args, f"Tiny-api objects total: {len(tiny_api_names)}")
    tiny_stage_records, tiny_api_issues = collect_tiny_api_stage_records(
        tiny_api_names,
        default_store_prefix=args.store_prefix or "unknown",
    )
    _vprint(args, f"Tiny-api stage records: {len(tiny_stage_records)} (issues: {len(tiny_api_issues)})")

    observed: Dict[str, List[StageRecord]] = {
        "tiny_api_pdv": [r for r in tiny_stage_records if r.stage_name == "tiny_api_pdv"],
        "tiny_api_pesquisa": [r for r in tiny_stage_records if r.stage_name == "tiny_api_pesquisa"],
        "tiny_api_produto": [r for r in tiny_stage_records if r.stage_name == "tiny_api_produto"],
    }

    bq_issues: List[Dict[str, str]] = []
    required_stages = set(args.require_pedido_stage or [])
    for stage_name, query_sql in stage_queries:
        _vprint(args, f"Running BigQuery stage query: {stage_name}")
        rows = [dict(row.items()) for row in bigquery_client.query(query_sql).result()]
        _vprint(args, f"Stage {stage_name}: fetched {len(rows)} rows")
        stage_records, issues = collect_stage_records_from_bigquery_rows(
            rows,
            stage_name=stage_name,
            require_pedido_id=stage_name in required_stages,
        )
        observed[stage_name] = stage_records
        bq_issues.extend([asdict(issue) for issue in issues])
        _vprint(args, f"Stage {stage_name}: normalized records={len(stage_records)}, issues={len(issues)}")

    diagnostics = {
        "mode": "live",
        "webhook_issues": [_report_to_dict(issue) for issue in webhook_issues],
        "tiny_api_issues": [_report_to_dict(issue) for issue in tiny_api_issues],
        "bq_issues": bq_issues,
    }

    expected = _filter_expected_identities(expected, store_prefix=args.store_prefix, start_date=args.start_date, end_date=args.end_date)
    observed = _filter_stage_records(observed, store_prefix=args.store_prefix, start_date=args.start_date, end_date=args.end_date)
    _vprint(args, f"Filtered expected identities: {len(expected)}")
    _vprint(args, "Filtered stage record counts: " + ", ".join(f"{k}={len(v)}" for k, v in observed.items()))

    return expected, observed, diagnostics


def _load_runtime_data(args: argparse.Namespace) -> tuple[List[EventIdentity], Dict[str, List[StageRecord]], Dict[str, Any]]:
    if args.mode == "file":
        expected = _parse_expected_identities(
            _load_json(args.expected_json),
            store_prefix=args.store_prefix,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        observed = _parse_stage_records(
            _load_json(args.stages_json),
            store_prefix=args.store_prefix,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        return expected, observed, {"mode": "file"}

    return _load_live_data(args)


def _build_context_maps(expected: Sequence[EventIdentity], observed: Mapping[str, Sequence[StageRecord]]) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    uuid_map: Dict[str, Dict[str, Any]] = {}
    pedido_map: Dict[str, Dict[str, Any]] = {}

    def _merge(identity: EventIdentity, stage_name: str | None = None) -> None:
        uuid_key = f"{identity.store_prefix}|{identity.uuid}"
        uuid_map.setdefault(
            uuid_key,
            {
                "store_prefix": identity.store_prefix,
                "uuid": identity.uuid,
                "dados_id": identity.dados_id,
                "pedido_id": identity.pedido_id,
                "event_timestamp": identity.event_timestamp,
                "observed_stages": [],
            },
        )
        if stage_name and stage_name not in uuid_map[uuid_key]["observed_stages"]:
            uuid_map[uuid_key]["observed_stages"].append(stage_name)

        if identity.pedido_id:
            pedido_key = f"{identity.store_prefix}|{identity.pedido_id}"
            pedido_map.setdefault(
                pedido_key,
                {
                    "store_prefix": identity.store_prefix,
                    "pedido_id": identity.pedido_id,
                    "dados_ids": [],
                    "uuids": [],
                },
            )
            if identity.dados_id not in pedido_map[pedido_key]["dados_ids"]:
                pedido_map[pedido_key]["dados_ids"].append(identity.dados_id)
            if identity.uuid not in pedido_map[pedido_key]["uuids"]:
                pedido_map[pedido_key]["uuids"].append(identity.uuid)

    for identity in expected:
        _merge(identity)
    for stage_name, records in observed.items():
        for record in records:
            if record.exists:
                _merge(record.identity, stage_name)

    return uuid_map, pedido_map


def _build_remediation_executor(
    *,
    backend: str,
    webhook_url: str | None,
    webhook_timeout_s: float,
    action_log: List[Dict[str, Any]],
    stage_webhook_map: Mapping[str, str],
    context_maps_ref: Dict[str, Dict[str, Dict[str, Any]]],
    policy_counts: Dict[str, int],
) -> Callable[[RemediationAction], None]:
    def _build_payload(action: RemediationAction) -> Dict[str, Any]:
        decision = resolve_policy(action)
        policy_counts[decision.category] = policy_counts.get(decision.category, 0) + 1
        context = context_maps_ref["uuid"].get(action.target_key) if action.target_type == "uuid" else context_maps_ref["pedido"].get(action.target_key)
        payload = build_action_payload(action=action, decision=decision, identity_context=context)
        payload["emitted_at"] = datetime.utcnow().isoformat() + "Z"
        return payload

    if backend == "noop":
        def _noop(action: RemediationAction) -> None:
            payload = _build_payload(action)
            action_log.append({"status": "noop", "payload": payload})

        return _noop

    if backend == "http_webhook":
        if not webhook_url:
            raise ValueError("--webhook-url is required when --remediation-backend=http_webhook")

        def _http_webhook(action: RemediationAction) -> None:
            payload = _build_payload(action)
            target_url = stage_webhook_map.get(action.target_stage, webhook_url)
            body = json.dumps(payload).encode("utf-8")
            req = urllib_request.Request(target_url, data=body, headers={"Content-Type": "application/json"}, method="POST")
            try:
                with urllib_request.urlopen(req, timeout=webhook_timeout_s) as response:
                    action_log.append(
                        {
                            "status": "success",
                            "target_url": target_url,
                            "http_status": response.status,
                            "response": response.read().decode("utf-8", errors="replace"),
                            "payload": payload,
                        }
                    )
            except urllib_error.URLError as exc:
                action_log.append({"status": "error", "target_url": target_url, "error": str(exc), "payload": payload})
                raise RuntimeError(f"Failed remediation webhook call: {exc}") from exc

        return _http_webhook

    raise ValueError(f"Unsupported remediation backend: {backend}")


def _run_remediation_command(*, args: argparse.Namespace, runtime_loader: RuntimeLoader) -> tuple[RemediationRunResult, Dict[str, Any]]:
    diagnostics_per_attempt: List[Dict[str, Any]] = []
    action_log: List[Dict[str, Any]] = []
    policy_counts: Dict[str, int] = {}
    context_maps_ref: Dict[str, Dict[str, Dict[str, Any]]] = {"uuid": {}, "pedido": {}}

    stage_webhook_map = _parse_stage_value_map(args.stage_webhook_url, option_name="--stage-webhook-url")

    executor = _build_remediation_executor(
        backend=args.remediation_backend,
        webhook_url=args.webhook_url,
        webhook_timeout_s=args.webhook_timeout_s,
        action_log=action_log,
        stage_webhook_map=stage_webhook_map,
        context_maps_ref=context_maps_ref,
        policy_counts=policy_counts,
    )

    allowed_stages = set(args.remediate_stage or [])

    def _action_filter(action: RemediationAction) -> bool:
        if allowed_stages and action.target_stage not in allowed_stages:
            return False
        return True

    def _validate_func(_prev_uuid: Any, _prev_pedido: Any):
        _vprint(args, "Remediation revalidation cycle started")
        expected, observed, diagnostics = runtime_loader(args)
        context_maps_ref["uuid"], context_maps_ref["pedido"] = _build_context_maps(expected, observed)
        uuid_report, pedido_report = build_dual_reconciliation_reports(expected, observed)
        diagnostics_per_attempt.append(
            {
                "expected_count": len(expected),
                "stage_record_counts": {stage: len(rows) for stage, rows in observed.items()},
                "uuid_missing_counts": {stage: len(v) for stage, v in uuid_report.missing_by_stage.items()},
                "pedido_missing_counts": {stage: len(v) for stage, v in pedido_report.missing_pedidos_by_stage.items()},
                "runtime": diagnostics,
            }
        )
        _vprint(args, "Revalidation summary: " + ", ".join(f"{s}:uuid_missing={len(v)}" for s, v in uuid_report.missing_by_stage.items()))
        return uuid_report, pedido_report

    result = run_remediation_loop(
        validate_func=_validate_func,
        execute_action_func=executor,
        max_attempts=args.max_retries,
        dry_run=args.dry_run,
        wait_seconds=args.retry_wait_seconds,
        action_filter_func=_action_filter,
        max_actions_per_attempt=args.max_actions_per_attempt,
    )

    diagnostics = {
        "backend": args.remediation_backend,
        "dry_run": args.dry_run,
        "allowed_stages": sorted(allowed_stages),
        "max_actions_per_attempt": args.max_actions_per_attempt,
        "stage_webhook_map": stage_webhook_map,
        "policy_counts": policy_counts,
        "attempt_diagnostics": diagnostics_per_attempt,
        "action_log": action_log,
    }
    return result, diagnostics


def _build_manual_intervention_queue(result: RemediationRunResult, diagnostics: Mapping[str, Any]) -> Dict[str, Any]:
    entries: List[Dict[str, Any]] = []
    for stage, keys in result.final_unresolved_uuids.items():
        for key in keys:
            action = RemediationAction(target_type="uuid", target_key=key, target_stage=stage, reason=f"missing_in_stage:{stage}")
            decision = resolve_policy(action)
            entries.append(
                {
                    "target_type": action.target_type,
                    "target_key": key,
                    "target_stage": stage,
                    "policy": asdict(decision),
                    "recommended_action": decision.retry_strategy,
                }
            )
    for stage, keys in result.final_unresolved_pedidos.items():
        for key in keys:
            action = RemediationAction(target_type="pedido", target_key=key, target_stage=stage, reason=f"missing_in_stage:{stage}")
            decision = resolve_policy(action)
            entries.append(
                {
                    "target_type": action.target_type,
                    "target_key": key,
                    "target_stage": stage,
                    "policy": asdict(decision),
                    "recommended_action": decision.retry_strategy,
                }
            )

    return {
        "entries": entries,
        "attempts_run": result.attempts_run,
        "converged": result.converged,
        "policy_counts": diagnostics.get("policy_counts", {}),
    }


def _build_run_verdict(result: RemediationRunResult, diagnostics: Mapping[str, Any]) -> Dict[str, Any]:
    remaining = sum(len(v) for v in result.final_unresolved_uuids.values()) + sum(len(v) for v in result.final_unresolved_pedidos.values())
    return {
        "status": "pass" if result.converged else "fail",
        "converged": result.converged,
        "attempts_run": result.attempts_run,
        "remaining_unresolved": remaining,
        "policy_counts": diagnostics.get("policy_counts", {}),
        "criteria": {
            "must_converge": True,
            "remaining_unresolved_must_be_zero": True,
        },
    }


def command_validate(args: argparse.Namespace) -> int:
    _vprint(args, f"Command validate started (mode={args.mode})")
    output_dir = _ensure_output_dir(args.output_dir)
    expected, observed, diagnostics = _load_runtime_data(args)
    uuid_report, pedido_report = build_dual_reconciliation_reports(expected, observed)
    _write_json(output_dir / "uuid_propagation_report.json", _report_to_dict(uuid_report))
    _write_json(output_dir / "pedido_completeness_report.json", _report_to_dict(pedido_report))
    _write_json(output_dir / "validation_runtime_diagnostics.json", _report_to_dict(diagnostics))
    _vprint(args, f"Validate complete. Output written to {output_dir}")
    return 0


def command_remediate(args: argparse.Namespace) -> int:
    _vprint(args, f"Command remediate started (mode={args.mode}, dry_run={args.dry_run})")
    output_dir = _ensure_output_dir(args.output_dir)
    result, diagnostics = _run_remediation_command(args=args, runtime_loader=_load_runtime_data)
    _write_json(output_dir / "remediation_result.json", _report_to_dict(result))
    _write_json(output_dir / "remediation_runtime_diagnostics.json", _report_to_dict(diagnostics))

    manual_queue = _build_manual_intervention_queue(result, diagnostics)
    _write_json(output_dir / "manual_intervention_queue.json", manual_queue)

    verdict = _build_run_verdict(result, diagnostics)
    _write_json(output_dir / "remediation_run_verdict.json", verdict)
    _vprint(args, f"Remediate complete. Output written to {output_dir}")
    return 0


def _parse_dedupe_candidates(data: Iterable[Mapping[str, Any]], *, store_prefix: str | None, start_date: str | None, end_date: str | None) -> List[DuplicateCandidate]:
    out: List[DuplicateCandidate] = []
    for row in data:
        candidate = DuplicateCandidate(
            record_id=str(row["record_id"]),
            store_prefix=str(row["store_prefix"]),
            pedido_id=str(row["pedido_id"]),
            stage_name=str(row["stage_name"]),
            uuid=str(row["uuid"]),
            event_timestamp=str(row["event_timestamp"]),
            ingestion_timestamp=(str(row["ingestion_timestamp"]) if row.get("ingestion_timestamp") not in (None, "") else None),
            product_key=(str(row["product_key"]) if row.get("product_key") not in (None, "") else None),
        )
        identity = EventIdentity(store_prefix=candidate.store_prefix, uuid=candidate.uuid, dados_id="0", pedido_id=candidate.pedido_id, event_timestamp=candidate.event_timestamp)
        if _filter_identity(identity, store_prefix=store_prefix, start_date=start_date, end_date=end_date):
            out.append(candidate)
    return out


def command_dedupe_report(args: argparse.Namespace) -> int:
    output_dir = _ensure_output_dir(args.output_dir)
    candidates = _parse_dedupe_candidates(_load_json(args.candidates_json), store_prefix=args.store_prefix, start_date=args.start_date, end_date=args.end_date)
    report = build_dedupe_report(candidates, stage_name=args.stage_name)
    _write_json(output_dir / f"dedupe_report_{args.stage_name}.json", _report_to_dict(report))
    return 0


def command_dedupe_apply(args: argparse.Namespace) -> int:
    output_dir = _ensure_output_dir(args.output_dir)
    candidates = _parse_dedupe_candidates(_load_json(args.candidates_json), store_prefix=args.store_prefix, start_date=args.start_date, end_date=args.end_date)
    report = build_dedupe_report(candidates, stage_name=args.stage_name)
    delete_plan: List[str] = []
    for group in report.groups:
        delete_plan.extend(group.loser_record_ids)
    _write_json(
        output_dir / f"dedupe_apply_plan_{args.stage_name}.json",
        {
            "stage_name": args.stage_name,
            "dry_run": args.dry_run,
            "delete_count": len(delete_plan),
            "delete_record_ids": sorted(delete_plan),
        },
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validation workflow CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    def _add_common_flags(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--store-prefix", default=None)
        subparser.add_argument("--verbose", action="store_true", default=False)
        subparser.add_argument("--verbose-every", type=int, default=500, help="Progress print interval for object listing/downloading")
        subparser.add_argument("--start-date", default=None, help="ISO timestamp or YYYYMMDDTHHMMSS")
        subparser.add_argument("--end-date", default=None, help="ISO timestamp or YYYYMMDDTHHMMSS")
        subparser.add_argument("--output-dir", required=True)

    def _add_source_flags(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("--mode", choices=("file", "live"), default="file")
        subparser.add_argument("--expected-json", default=None)
        subparser.add_argument("--stages-json", default=None)
        subparser.add_argument("--gcp-project", default=None)
        subparser.add_argument("--webhook-bucket", default=None)
        subparser.add_argument("--webhook-prefix", default="vendas/")
        subparser.add_argument("--tiny-api-bucket", default=None)
        subparser.add_argument("--tiny-api-prefix", default="")
        subparser.add_argument("--stage-query", action="append", default=None, help="Repeatable format: stage_name::SELECT ...")
        subparser.add_argument("--require-pedido-stage", action="append", default=[], help="Repeat stage names that must contain pedido_id")

    validate = subparsers.add_parser("validate", help="Read-only reconciliation reports")
    _add_common_flags(validate)
    _add_source_flags(validate)
    validate.set_defaults(func=command_validate)

    remediate = subparsers.add_parser("remediate", help="Targeted remediation loop")
    _add_common_flags(remediate)
    _add_source_flags(remediate)
    remediate.add_argument("--dry-run", action="store_true", default=False)
    remediate.add_argument("--max-retries", type=int, default=3)
    remediate.add_argument("--retry-wait-seconds", type=float, default=0.0)
    remediate.add_argument("--remediation-backend", choices=("noop", "http_webhook"), default="noop")
    remediate.add_argument("--webhook-url", default=None)
    remediate.add_argument("--webhook-timeout-s", type=float, default=10.0)
    remediate.add_argument("--remediate-stage", action="append", default=[])
    remediate.add_argument("--max-actions-per-attempt", type=int, default=None)
    remediate.add_argument("--stage-webhook-url", action="append", default=[])
    remediate.set_defaults(func=command_remediate)

    dedupe_report = subparsers.add_parser("dedupe-report", help="Generate duplicate preview report")
    _add_common_flags(dedupe_report)
    dedupe_report.add_argument("--candidates-json", required=True)
    dedupe_report.add_argument("--stage-name", required=True)
    dedupe_report.set_defaults(func=command_dedupe_report)

    dedupe_apply = subparsers.add_parser("dedupe-apply", help="Generate dedupe delete plan")
    _add_common_flags(dedupe_apply)
    dedupe_apply.add_argument("--candidates-json", required=True)
    dedupe_apply.add_argument("--stage-name", required=True)
    dedupe_apply.add_argument("--dry-run", action="store_true", default=False)
    dedupe_apply.set_defaults(func=command_dedupe_apply)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if getattr(args, "mode", "file") == "file" and args.command in {"validate", "remediate"}:
        if getattr(args, "expected_json", None) is None or getattr(args, "stages_json", None) is None:
            parser.error("In --mode file, --expected-json and --stages-json are required")

    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
