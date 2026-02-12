import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from scripts.validation_deduplication import (
    DuplicateCandidate,
    build_dedupe_report,
    build_stage_dedupe_reports,
    select_group_winner,
    stage_records_to_candidates,
)
from scripts.validation_identity import EventIdentity, StageRecord


def test_select_group_winner_prefers_latest_event_timestamp_then_ingestion_then_uuid():
    candidates = [
        DuplicateCandidate(
            record_id="r1",
            store_prefix="z316",
            pedido_id="100",
            stage_name="raw_pdv",
            uuid="00000000-0000-0000-0000-000000000001",
            event_timestamp="2025-01-01T00:00:00Z",
            ingestion_timestamp="2025-01-01T00:00:10Z",
        ),
        DuplicateCandidate(
            record_id="r2",
            store_prefix="z316",
            pedido_id="100",
            stage_name="raw_pdv",
            uuid="00000000-0000-0000-0000-000000000002",
            event_timestamp="2025-01-02T00:00:00Z",
            ingestion_timestamp="2025-01-01T00:00:05Z",
        ),
    ]

    winner = select_group_winner(candidates)
    assert winner.record_id == "r2"


def test_build_dedupe_report_detects_duplicates_and_picks_winner():
    candidates = [
        DuplicateCandidate(
            record_id="a",
            store_prefix="z316",
            pedido_id="501",
            stage_name="points_sales",
            uuid="00000000-0000-0000-0000-000000000001",
            event_timestamp="2025-01-01T00:00:00Z",
        ),
        DuplicateCandidate(
            record_id="b",
            store_prefix="z316",
            pedido_id="501",
            stage_name="points_sales",
            uuid="00000000-0000-0000-0000-000000000002",
            event_timestamp="2025-01-02T00:00:00Z",
        ),
        DuplicateCandidate(
            record_id="c",
            store_prefix="z316",
            pedido_id="999",
            stage_name="points_sales",
            uuid="00000000-0000-0000-0000-000000000003",
            event_timestamp="2025-01-03T00:00:00Z",
        ),
    ]

    report = build_dedupe_report(candidates, stage_name="points_sales")

    assert report.total_groups == 2
    assert report.duplicate_groups == 1
    group_501 = next(g for g in report.groups if g.group_key.endswith("|501"))
    assert group_501.winner_record_id == "b"
    assert group_501.loser_record_ids == ["a"]


def test_stage_records_to_candidates_skips_non_existing_and_missing_pedido():
    records = [
        StageRecord(
            identity=EventIdentity(
                store_prefix="z316",
                uuid="00000000-0000-0000-0000-000000000001",
                dados_id="10",
                pedido_id="501",
                event_timestamp="2025-01-01T00:00:00Z",
            ),
            stage_name="raw_pdv",
            exists=True,
            metadata={"ingestion_timestamp": "2025-01-01T00:00:10Z"},
        ),
        StageRecord(
            identity=EventIdentity(
                store_prefix="z316",
                uuid="00000000-0000-0000-0000-000000000002",
                dados_id="11",
                pedido_id=None,
                event_timestamp="2025-01-01T00:00:00Z",
            ),
            stage_name="raw_pdv",
            exists=True,
            metadata={},
        ),
        StageRecord(
            identity=EventIdentity(
                store_prefix="z316",
                uuid="00000000-0000-0000-0000-000000000003",
                dados_id="12",
                pedido_id="503",
                event_timestamp="2025-01-01T00:00:00Z",
            ),
            stage_name="raw_pdv",
            exists=False,
            metadata={},
        ),
    ]

    candidates = stage_records_to_candidates(records, default_record_prefix="raw")

    assert len(candidates) == 1
    assert candidates[0].record_id == "raw:0"


def test_build_stage_dedupe_reports_multiple_stages():
    reports = build_stage_dedupe_reports(
        {
            "raw_pdv": [
                DuplicateCandidate(
                    record_id="a",
                    store_prefix="z316",
                    pedido_id="1",
                    stage_name="raw_pdv",
                    uuid="00000000-0000-0000-0000-000000000001",
                    event_timestamp="2025-01-01T00:00:00Z",
                ),
                DuplicateCandidate(
                    record_id="b",
                    store_prefix="z316",
                    pedido_id="1",
                    stage_name="raw_pdv",
                    uuid="00000000-0000-0000-0000-000000000002",
                    event_timestamp="2025-01-02T00:00:00Z",
                ),
            ],
            "points_sales": [
                DuplicateCandidate(
                    record_id="c",
                    store_prefix="z316",
                    pedido_id="10",
                    stage_name="points_sales",
                    uuid="00000000-0000-0000-0000-000000000010",
                    event_timestamp="2025-01-03T00:00:00Z",
                )
            ],
        }
    )

    assert set(reports.keys()) == {"points_sales", "raw_pdv"}
    assert reports["raw_pdv"].duplicate_groups == 1
    assert reports["points_sales"].duplicate_groups == 0
