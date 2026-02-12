import json
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from scripts.validation_collectors import (
    collect_expected_from_webhook_objects,
    collect_stage_records_from_bigquery_rows,
    collect_tiny_api_stage_records,
)


def test_collect_expected_from_webhook_objects_success_and_issue():
    objects = [
        {
            "name": "vendas/z316-tiny-webhook-vendas-123-20250211T010203-abcdefab-1234-5678-9abc-defabcdefabc.json",
            "payload_bytes": json.dumps({"dados": {"id": "123"}}).encode("utf-8"),
        },
        {
            "name": "vendas/bad-name.json",
            "payload_bytes": b"{}",
        },
    ]

    identities, issues = collect_expected_from_webhook_objects(objects)

    assert len(identities) == 1
    assert identities[0].dados_id == "123"
    assert identities[0].store_prefix == "z316"
    assert len(issues) == 1
    assert issues[0].stage_name == "webhook"


def test_collect_tiny_api_stage_records_presence_map():
    object_names = [
        "20250211T010203-123-abcdefab-1234-5678-9abc-defabcdefabc/x-pdv-foo.json",
        "20250211T010203-123-abcdefab-1234-5678-9abc-defabcdefabc/x-pesquisa-foo.json",
        "20250211T010203-123-abcdefab-1234-5678-9abc-defabcdefabc/x-produto-1.json",
        "bad/object/name.json",
    ]

    records, issues = collect_tiny_api_stage_records(object_names, default_store_prefix="z316")

    assert len(records) == 3
    assert all(record.exists for record in records)
    assert all(record.identity.store_prefix == "z316" for record in records)
    assert {r.stage_name for r in records} == {
        "tiny_api_pdv",
        "tiny_api_pesquisa",
        "tiny_api_produto",
    }
    assert len(issues) == 1


def test_collect_stage_records_from_bigquery_rows_success_and_invalid():
    rows = [
        {
            "store_prefix": "z316",
            "uuid": "abcdefab-1234-5678-9abc-defabcdefabc",
            "dados_id": "123",
            "pedido_id": "999",
            "timestamp": "2025-02-11T01:02:03Z",
        },
        {
            "store_prefix": "z316",
            "uuid": "bad-uuid",
            "dados_id": "123",
            "timestamp": "2025-02-11T01:02:03Z",
        },
    ]

    records, issues = collect_stage_records_from_bigquery_rows(
        rows,
        stage_name="raw_pdv",
        require_pedido_id=False,
    )

    assert len(records) == 1
    assert records[0].stage_name == "raw_pdv"
    assert records[0].identity.uuid == "abcdefab-1234-5678-9abc-defabcdefabc"
    assert len(issues) == 1


def test_collect_stage_records_from_bigquery_rows_enforce_pedido_id():
    rows = [
        {
            "store_prefix": "z316",
            "uuid": "abcdefab-1234-5678-9abc-defabcdefabc",
            "dados_id": "123",
            "timestamp": "2025-02-11T01:02:03Z",
        }
    ]

    records, issues = collect_stage_records_from_bigquery_rows(
        rows,
        stage_name="points_sales",
        require_pedido_id=True,
    )

    assert records == []
    assert len(issues) == 1
    assert "pedido_id is required" in issues[0].reason
