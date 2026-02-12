import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from scripts.validation_identity import EventIdentity, StageRecord
from scripts.validation_reconciliation import (
    build_dual_reconciliation_reports,
    build_pedido_completeness_report,
    build_uuid_propagation_report,
)


def _identity(store_prefix: str, uuid: str, dados_id: str, pedido_id: str | None):
    return EventIdentity(
        store_prefix=store_prefix,
        uuid=uuid,
        dados_id=dados_id,
        pedido_id=pedido_id,
        event_timestamp="2025-02-11T01:02:03Z",
    )


def _stage(stage_name: str, identity: EventIdentity, exists: bool = True):
    return StageRecord(identity=identity, stage_name=stage_name, exists=exists, metadata={})


def test_uuid_propagation_report_missing_and_orphans():
    expected = [
        _identity("z316", "00000000-0000-0000-0000-000000000001", "101", "501"),
        _identity("z316", "00000000-0000-0000-0000-000000000002", "102", "502"),
    ]

    stage_records = {
        "raw": [
            _stage("raw", expected[0]),
            _stage("raw", expected[1]),
            _stage("raw", _identity("z316", "00000000-0000-0000-0000-0000000000aa", "900", "999")),
        ],
        "points": [
            _stage("points", expected[0]),
        ],
    }

    report = build_uuid_propagation_report(expected, stage_records)

    assert len(report.entries) == 2
    assert report.missing_by_stage["raw"] == []
    assert report.missing_by_stage["points"] == ["z316|00000000-0000-0000-0000-000000000002"]
    assert report.uuid_orphans_by_stage["raw"] == ["z316|00000000-0000-0000-0000-0000000000aa"]
    assert report.uuid_orphans_by_stage["points"] == []


def test_pedido_completeness_report_missing_and_overrepresented():
    expected = [
        _identity("z316", "00000000-0000-0000-0000-000000000001", "101", "501"),
        _identity("z316", "00000000-0000-0000-0000-000000000002", "102", "502"),
    ]

    stage_records = {
        "raw": [
            _stage("raw", expected[0]),
            _stage("raw", expected[0]),
            _stage("raw", expected[1]),
        ],
        "points": [
            _stage("points", expected[0]),
        ],
    }

    report = build_pedido_completeness_report(expected, stage_records)

    assert len(report.entries) == 2
    assert report.missing_pedidos_by_stage["raw"] == []
    assert report.missing_pedidos_by_stage["points"] == ["z316|502"]
    assert report.overrepresented_pedidos_by_stage["raw"] == ["z316|501"]
    assert report.overrepresented_pedidos_by_stage["points"] == []


def test_dual_report_builder_returns_both_reports():
    expected = [_identity("z316", "00000000-0000-0000-0000-000000000001", "101", "501")]
    stage_records = {"raw": [_stage("raw", expected[0])]}

    uuid_report, pedido_report = build_dual_reconciliation_reports(expected, stage_records)

    assert len(uuid_report.entries) == 1
    assert len(pedido_report.entries) == 1
