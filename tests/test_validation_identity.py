import os
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from datetime import datetime, timezone

import pytest

from scripts.validation_identity import (
    EventIdentity,
    IdentityValidationError,
    build_identity_from_webhook,
    extract_pedido_id_from_pdv_payload,
    extract_pedido_ids_from_pesquisa_payload,
    normalize_timestamp,
    normalize_uuid,
    parse_webhook_filename,
)


def test_parse_webhook_filename_normalizes_uuid_and_timestamp():
    parsed = parse_webhook_filename(
        "vendas/z316-tiny-webhook-vendas-12345-20250211T010203-"
        "ABCDEFAB-1234-5678-9ABC-DEFABCDEFABC.json"
    )

    assert parsed["store_prefix"] == "z316"
    assert parsed["dados_id"] == "12345"
    assert parsed["uuid"] == "abcdefab-1234-5678-9abc-defabcdefabc"
    assert parsed["event_timestamp"] == "2025-02-11T01:02:03Z"


def test_parse_webhook_filename_rejects_invalid_format():
    with pytest.raises(IdentityValidationError):
        parse_webhook_filename("vendas/not-valid-name.json")


def test_build_identity_from_webhook_enforces_dados_id_match():
    payload = {"dados": {"id": "10", "idPedido": "999"}}
    with pytest.raises(IdentityValidationError):
        build_identity_from_webhook(
            "vendas/z316-tiny-webhook-vendas-11-20250211T010203-"
            "abcdefab-1234-5678-9abc-defabcdefabc.json",
            payload,
        )


def test_build_identity_from_webhook_returns_event_identity():
    payload = {"dados": {"id": "11", "idPedido": "999"}}
    identity = build_identity_from_webhook(
        "vendas/z316-tiny-webhook-vendas-11-20250211T010203-"
        "abcdefab-1234-5678-9abc-defabcdefabc.json",
        payload,
    )

    assert isinstance(identity, EventIdentity)
    assert identity.store_prefix == "z316"
    assert identity.uuid == "abcdefab-1234-5678-9abc-defabcdefabc"
    assert identity.dados_id == "11"
    assert identity.pedido_id == "999"


def test_extract_pedido_id_from_pdv_payload():
    payload = {"retorno": {"pedido": {"id": 123}}}
    assert extract_pedido_id_from_pdv_payload(payload) == "123"


def test_extract_pedido_ids_from_pesquisa_payload_all_ids():
    payload = {
        "retorno": {
            "pedidos": [
                {"pedido": {"id": "1"}},
                {"pedido": {"id": 2}},
                {"pedido": {}},
            ]
        }
    }
    assert extract_pedido_ids_from_pesquisa_payload(payload) == {"1", "2"}


def test_normalize_uuid_rejects_invalid():
    with pytest.raises(IdentityValidationError):
        normalize_uuid("bad-uuid")


def test_normalize_timestamp_from_datetime_and_iso():
    dt = datetime(2025, 2, 11, 1, 2, 3, tzinfo=timezone.utc)
    assert normalize_timestamp(dt) == "2025-02-11T01:02:03Z"
    assert normalize_timestamp("2025-02-11T01:02:03Z") == "2025-02-11T01:02:03Z"
