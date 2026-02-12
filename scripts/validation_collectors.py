"""Read-only collectors for pipeline validation (Chunk 2).

Collectors normalize observed records from webhook objects, tiny-api artifacts,
and BigQuery rows into shared identity/stage structures.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from scripts.validation_identity import (
    EventIdentity,
    IdentityValidationError,
    StageRecord,
    build_identity_from_webhook,
    normalize_id,
    normalize_optional_id,
    normalize_timestamp,
    normalize_uuid,
)


TINY_API_FOLDER_PATTERN = re.compile(
    r"(?P<timestamp>\d{8}T\d{6})-(?P<dados_id>\d+)-(?P<uuid>[0-9a-fA-F-]{36})"
)


@dataclass(frozen=True)
class CollectorIssue:
    stage_name: str
    record_ref: str
    reason: str


def _safe_parse_json(payload_bytes: bytes, record_ref: str, stage_name: str) -> dict:
    try:
        decoded = payload_bytes.decode("utf-8")
        return json.loads(decoded)
    except Exception as exc:  # noqa: BLE001 - explicit collector capture
        raise IdentityValidationError(
            f"Invalid JSON payload at {record_ref}: {exc}"
        ) from exc


def collect_expected_from_webhook_objects(
    objects: Iterable[Mapping[str, Any]],
) -> tuple[List[EventIdentity], List[CollectorIssue]]:
    """Collect expected identities from webhook objects.

    Each object mapping must provide:
      - name: object path
      - payload_bytes: raw JSON payload bytes
    """

    identities: List[EventIdentity] = []
    issues: List[CollectorIssue] = []

    for obj in objects:
        record_ref = str(obj.get("name", "unknown"))
        try:
            payload_raw = obj.get("payload_bytes")
            if not isinstance(payload_raw, (bytes, bytearray)):
                raise IdentityValidationError("payload_bytes must be bytes")
            payload = _safe_parse_json(payload_raw, record_ref, "webhook")
            identity = build_identity_from_webhook(record_ref, payload)
            identities.append(identity)
        except IdentityValidationError as exc:
            issues.append(
                CollectorIssue(
                    stage_name="webhook",
                    record_ref=record_ref,
                    reason=str(exc),
                )
            )

    return identities, issues


def collect_tiny_api_stage_records(
    object_names: Iterable[str],
    *,
    default_store_prefix: str = "unknown",
) -> tuple[List[StageRecord], List[CollectorIssue]]:
    """Collect tiny-api artifact presence by UUID/dados folder lineage.

    The collector is read-only and path-based. It expects names that include
    folder pattern `YYYYMMDDTHHMMSS-<dados_id>-<uuid>` and artifact filenames
    containing `pdv`, `pesquisa`, and/or `produto`.
    """

    by_identity: Dict[tuple[str, str, str], Dict[str, str]] = {}
    issues: List[CollectorIssue] = []

    for name in object_names:
        record_ref = str(name)
        folder_match = TINY_API_FOLDER_PATTERN.search(record_ref)
        if not folder_match:
            issues.append(
                CollectorIssue(
                    stage_name="tiny_api",
                    record_ref=record_ref,
                    reason="Could not parse timestamp/dados_id/uuid from object path",
                )
            )
            continue

        try:
            timestamp = normalize_timestamp(folder_match.group("timestamp"))
            dados_id = normalize_id(folder_match.group("dados_id"), field_name="dados_id")
            uuid_value = normalize_uuid(folder_match.group("uuid"))
        except IdentityValidationError as exc:
            issues.append(
                CollectorIssue("tiny_api", record_ref, str(exc))
            )
            continue

        stage_key = "unknown"
        lowered = record_ref.lower()
        if "pdv" in lowered:
            stage_key = "tiny_api_pdv"
        elif "pesquisa" in lowered:
            stage_key = "tiny_api_pesquisa"
        elif "produto" in lowered:
            stage_key = "tiny_api_produto"

        key = (uuid_value, dados_id, timestamp)
        by_identity.setdefault(key, {})[stage_key] = record_ref

    stage_records: List[StageRecord] = []
    for (uuid_value, dados_id, timestamp), stages in by_identity.items():
        identity = EventIdentity(
            store_prefix=default_store_prefix,
            uuid=uuid_value,
            dados_id=dados_id,
            pedido_id=None,
            event_timestamp=timestamp,
        )
        for stage_name in ("tiny_api_pdv", "tiny_api_pesquisa", "tiny_api_produto"):
            stage_records.append(
                StageRecord(
                    identity=identity,
                    stage_name=stage_name,
                    exists=stage_name in stages,
                    metadata={"artifact": stages.get(stage_name, "")},
                )
            )

    return stage_records, issues


def collect_stage_records_from_bigquery_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    stage_name: str,
    require_pedido_id: bool = False,
) -> tuple[List[StageRecord], List[CollectorIssue]]:
    """Collect normalized stage records from BigQuery-like row mappings.

    Expected row keys:
      - store_prefix
      - uuid
      - dados_id (optional)
      - pedido_id (optional unless require_pedido_id=True)
      - timestamp (or event_timestamp)
    """

    records: List[StageRecord] = []
    issues: List[CollectorIssue] = []

    for idx, row in enumerate(rows):
        record_ref = f"{stage_name}[{idx}]"
        try:
            uuid_value = normalize_uuid(row.get("uuid"))
            store_prefix = normalize_id(
                row.get("store_prefix", "unknown"),
                field_name="store_prefix",
            )
            dados_id = normalize_id(
                row.get("dados_id", "unknown"),
                field_name="dados_id",
            )
            pedido_id = normalize_optional_id(row.get("pedido_id"))
            if require_pedido_id and not pedido_id:
                raise IdentityValidationError("pedido_id is required for this stage")

            raw_ts = row.get("timestamp", row.get("event_timestamp"))
            event_timestamp = normalize_timestamp(raw_ts)

            identity = EventIdentity(
                store_prefix=store_prefix,
                uuid=uuid_value,
                dados_id=dados_id,
                pedido_id=pedido_id,
                event_timestamp=event_timestamp,
            )
            records.append(
                StageRecord(
                    identity=identity,
                    stage_name=stage_name,
                    exists=True,
                    metadata={"row_index": str(idx)},
                )
            )
        except IdentityValidationError as exc:
            issues.append(CollectorIssue(stage_name=stage_name, record_ref=record_ref, reason=str(exc)))

    return records, issues
