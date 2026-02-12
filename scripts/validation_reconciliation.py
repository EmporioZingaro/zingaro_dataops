"""Dual reconciliation engine for validation workflow (Chunk 3).

This module builds:
1) UUID propagation report: expected UUID visibility across stages.
2) Pedido completeness report: expected pedido coverage across stages.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set

from scripts.validation_identity import EventIdentity, StageRecord


@dataclass(frozen=True)
class UUIDPropagationEntry:
    store_prefix: str
    uuid: str
    dados_id: str
    pedido_id: Optional[str]
    event_timestamp: str
    stage_presence: Dict[str, bool]


@dataclass(frozen=True)
class PedidoCompletenessEntry:
    store_prefix: str
    pedido_id: str
    expected_count: int
    stage_counts: Dict[str, int]


@dataclass(frozen=True)
class UUIDPropagationReport:
    entries: List[UUIDPropagationEntry]
    missing_by_stage: Dict[str, List[str]]
    uuid_orphans_by_stage: Dict[str, List[str]]


@dataclass(frozen=True)
class PedidoCompletenessReport:
    entries: List[PedidoCompletenessEntry]
    missing_pedidos_by_stage: Dict[str, List[str]]
    overrepresented_pedidos_by_stage: Dict[str, List[str]]


def _identity_key(identity: EventIdentity) -> str:
    return f"{identity.store_prefix}|{identity.uuid}"


def _pedido_key(store_prefix: str, pedido_id: str) -> str:
    return f"{store_prefix}|{pedido_id}"


def build_uuid_propagation_report(
    expected_identities: Sequence[EventIdentity],
    observed_stage_records: Mapping[str, Sequence[StageRecord]],
) -> UUIDPropagationReport:
    """Build UUID propagation report comparing expected UUIDs vs stage records."""

    expected_by_key: Dict[str, EventIdentity] = {
        _identity_key(identity): identity for identity in expected_identities
    }
    expected_keys = set(expected_by_key.keys())

    stage_present_keys: Dict[str, Set[str]] = {}
    for stage_name, records in observed_stage_records.items():
        keys: Set[str] = set()
        for record in records:
            if not record.exists:
                continue
            keys.add(_identity_key(record.identity))
        stage_present_keys[stage_name] = keys

    entries: List[UUIDPropagationEntry] = []
    missing_by_stage: Dict[str, List[str]] = {stage: [] for stage in observed_stage_records}

    for key in sorted(expected_keys):
        identity = expected_by_key[key]
        stage_presence: Dict[str, bool] = {}
        for stage_name, keys in stage_present_keys.items():
            present = key in keys
            stage_presence[stage_name] = present
            if not present:
                missing_by_stage[stage_name].append(key)

        entries.append(
            UUIDPropagationEntry(
                store_prefix=identity.store_prefix,
                uuid=identity.uuid,
                dados_id=identity.dados_id,
                pedido_id=identity.pedido_id,
                event_timestamp=identity.event_timestamp,
                stage_presence=stage_presence,
            )
        )

    uuid_orphans_by_stage: Dict[str, List[str]] = {}
    for stage_name, keys in stage_present_keys.items():
        orphans = sorted(keys - expected_keys)
        uuid_orphans_by_stage[stage_name] = orphans

    return UUIDPropagationReport(
        entries=entries,
        missing_by_stage=missing_by_stage,
        uuid_orphans_by_stage=uuid_orphans_by_stage,
    )


def build_pedido_completeness_report(
    expected_identities: Sequence[EventIdentity],
    observed_stage_records: Mapping[str, Sequence[StageRecord]],
) -> PedidoCompletenessReport:
    """Build pedido completeness report across stages.

    This report only includes expected identities where `pedido_id` is present.
    """

    expected_counts: Dict[str, int] = {}
    expected_pedidos: Dict[str, tuple[str, str]] = {}

    for identity in expected_identities:
        if not identity.pedido_id:
            continue
        pedido_key = _pedido_key(identity.store_prefix, identity.pedido_id)
        expected_counts[pedido_key] = expected_counts.get(pedido_key, 0) + 1
        expected_pedidos[pedido_key] = (identity.store_prefix, identity.pedido_id)

    stage_counts: Dict[str, Dict[str, int]] = {stage: {} for stage in observed_stage_records}
    for stage_name, records in observed_stage_records.items():
        for record in records:
            if not record.exists:
                continue
            pedido_id = record.identity.pedido_id
            if not pedido_id:
                continue
            pedido_key = _pedido_key(record.identity.store_prefix, pedido_id)
            stage_counts[stage_name][pedido_key] = stage_counts[stage_name].get(pedido_key, 0) + 1

    entries: List[PedidoCompletenessEntry] = []
    for pedido_key in sorted(expected_counts.keys()):
        store_prefix, pedido_id = expected_pedidos[pedido_key]
        counts = {
            stage_name: stage_counts[stage_name].get(pedido_key, 0)
            for stage_name in observed_stage_records
        }
        entries.append(
            PedidoCompletenessEntry(
                store_prefix=store_prefix,
                pedido_id=pedido_id,
                expected_count=expected_counts[pedido_key],
                stage_counts=counts,
            )
        )

    missing_pedidos_by_stage: Dict[str, List[str]] = {stage: [] for stage in observed_stage_records}
    overrepresented_pedidos_by_stage: Dict[str, List[str]] = {stage: [] for stage in observed_stage_records}

    expected_keys = set(expected_counts.keys())
    for stage_name in observed_stage_records:
        for pedido_key in sorted(expected_keys):
            observed_count = stage_counts[stage_name].get(pedido_key, 0)
            expected_count = expected_counts[pedido_key]
            if observed_count < expected_count:
                missing_pedidos_by_stage[stage_name].append(pedido_key)
            elif observed_count > expected_count:
                overrepresented_pedidos_by_stage[stage_name].append(pedido_key)

    return PedidoCompletenessReport(
        entries=entries,
        missing_pedidos_by_stage=missing_pedidos_by_stage,
        overrepresented_pedidos_by_stage=overrepresented_pedidos_by_stage,
    )


def build_dual_reconciliation_reports(
    expected_identities: Sequence[EventIdentity],
    observed_stage_records: Mapping[str, Sequence[StageRecord]],
) -> tuple[UUIDPropagationReport, PedidoCompletenessReport]:
    """Build both UUID propagation and pedido completeness reports."""

    uuid_report = build_uuid_propagation_report(expected_identities, observed_stage_records)
    pedido_report = build_pedido_completeness_report(expected_identities, observed_stage_records)
    return uuid_report, pedido_report
