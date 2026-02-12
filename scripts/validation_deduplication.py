"""Duplicate detection and deterministic winner selection helpers (Chunk 6)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from scripts.validation_identity import EventIdentity, StageRecord, normalize_timestamp, normalize_uuid


@dataclass(frozen=True)
class DuplicateCandidate:
    """Represents one row/file candidate that can participate in dedupe selection."""

    record_id: str
    store_prefix: str
    pedido_id: str
    stage_name: str
    uuid: str
    event_timestamp: str
    ingestion_timestamp: Optional[str] = None
    product_key: Optional[str] = None


@dataclass(frozen=True)
class DuplicateGroupResult:
    group_key: str
    winner_record_id: str
    loser_record_ids: List[str]


@dataclass(frozen=True)
class DedupeReport:
    stage_name: str
    total_groups: int
    duplicate_groups: int
    groups: List[DuplicateGroupResult]


def _as_utc_datetime(value: str) -> datetime:
    normalized = normalize_timestamp(value)
    return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(timezone.utc)


def _safe_ingestion_ts(value: Optional[str]) -> datetime:
    if value in (None, ""):
        return datetime.min.replace(tzinfo=timezone.utc)
    return _as_utc_datetime(value)


def _candidate_sort_key(candidate: DuplicateCandidate) -> Tuple[datetime, datetime, str]:
    return (
        _as_utc_datetime(candidate.event_timestamp),
        _safe_ingestion_ts(candidate.ingestion_timestamp),
        normalize_uuid(candidate.uuid),
    )


def _normalize_candidate(candidate: DuplicateCandidate) -> DuplicateCandidate:
    return DuplicateCandidate(
        record_id=str(candidate.record_id),
        store_prefix=str(candidate.store_prefix).strip(),
        pedido_id=str(candidate.pedido_id).strip(),
        stage_name=str(candidate.stage_name).strip(),
        uuid=normalize_uuid(candidate.uuid),
        event_timestamp=normalize_timestamp(candidate.event_timestamp),
        ingestion_timestamp=(
            normalize_timestamp(candidate.ingestion_timestamp)
            if candidate.ingestion_timestamp not in (None, "")
            else None
        ),
        product_key=(str(candidate.product_key).strip() if candidate.product_key not in (None, "") else None),
    )


def group_key_for_candidate(candidate: DuplicateCandidate) -> str:
    """Build stage-specific dedupe grouping key."""

    c = _normalize_candidate(candidate)

    if c.stage_name in {"raw_pdv", "raw_pesquisa", "raw_produto"}:
        return f"{c.stage_name}|{c.store_prefix}|{c.pedido_id}"

    if c.stage_name in {"transform_pedidos", "points_sales"}:
        return f"{c.stage_name}|{c.store_prefix}|{c.pedido_id}"

    if c.stage_name in {"transform_itens", "points_sales_items"}:
        product = c.product_key or ""
        return f"{c.stage_name}|{c.store_prefix}|{c.pedido_id}|{product}"

    # Default behavior for unknown stages: keep dedupe bounded by stage + pedido.
    return f"{c.stage_name}|{c.store_prefix}|{c.pedido_id}"


def select_group_winner(candidates: Sequence[DuplicateCandidate]) -> DuplicateCandidate:
    if not candidates:
        raise ValueError("candidates must not be empty")

    normalized = [_normalize_candidate(item) for item in candidates]
    return max(normalized, key=_candidate_sort_key)


def build_dedupe_report(
    candidates: Iterable[DuplicateCandidate],
    *,
    stage_name: str,
) -> DedupeReport:
    normalized_candidates = [_normalize_candidate(item) for item in candidates if item.stage_name == stage_name]

    grouped: Dict[str, List[DuplicateCandidate]] = {}
    for candidate in normalized_candidates:
        key = group_key_for_candidate(candidate)
        grouped.setdefault(key, []).append(candidate)

    group_results: List[DuplicateGroupResult] = []
    duplicate_groups = 0

    for key in sorted(grouped.keys()):
        group = grouped[key]
        winner = select_group_winner(group)
        losers = sorted([item.record_id for item in group if item.record_id != winner.record_id])
        if losers:
            duplicate_groups += 1
        group_results.append(
            DuplicateGroupResult(
                group_key=key,
                winner_record_id=winner.record_id,
                loser_record_ids=losers,
            )
        )

    return DedupeReport(
        stage_name=stage_name,
        total_groups=len(grouped),
        duplicate_groups=duplicate_groups,
        groups=group_results,
    )


def stage_records_to_candidates(
    records: Iterable[StageRecord],
    *,
    default_record_prefix: str,
) -> List[DuplicateCandidate]:
    """Convert StageRecord entries into DuplicateCandidate entries.

    This helper keeps Chunk 6 integrated with the data contract introduced in Chunk 1.
    """

    out: List[DuplicateCandidate] = []
    for idx, record in enumerate(records):
        identity: EventIdentity = record.identity
        if not record.exists or not identity.pedido_id:
            continue

        out.append(
            DuplicateCandidate(
                record_id=f"{default_record_prefix}:{idx}",
                store_prefix=identity.store_prefix,
                pedido_id=identity.pedido_id,
                stage_name=record.stage_name,
                uuid=identity.uuid,
                event_timestamp=identity.event_timestamp,
                ingestion_timestamp=record.metadata.get("ingestion_timestamp"),
                product_key=record.metadata.get("product_key"),
            )
        )

    return out


def build_stage_dedupe_reports(
    candidates_by_stage: Mapping[str, Sequence[DuplicateCandidate]],
) -> Dict[str, DedupeReport]:
    """Build dedupe reports per stage from provided candidate collections."""

    reports: Dict[str, DedupeReport] = {}
    for stage_name in sorted(candidates_by_stage.keys()):
        reports[stage_name] = build_dedupe_report(candidates_by_stage[stage_name], stage_name=stage_name)
    return reports
