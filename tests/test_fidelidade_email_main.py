import importlib
import base64
import json
import os
import sys
import types

import pytest


class StubBQClient:
    def query(self, *args, **kwargs):
        raise AssertionError("query should not be called in this unit test")


class StubSecretManagerClient:
    def access_secret_version(self, name):
        class Payload:
            data = b"test-key"

        class Response:
            payload = Payload()

        return Response()


class StubEmail:
    def __init__(self, email, name=None):
        self.email = email
        self.name = name


class StubAsm:
    def __init__(self, group_id=None, groups_to_display=None):
        self.group_id = group_id
        self.groups_to_display = groups_to_display


class StubMail:
    def __init__(self, from_email=None, to_emails=None):
        self.from_email = from_email
        self.to_emails = to_emails
        self.template_id = None
        self.dynamic_template_data = None
        self.asm = None


class StubSendGridClient:
    def __init__(self, api_key):
        self.api_key = api_key

    def send(self, message):
        class Response:
            status_code = 202

        return Response()


def install_stubs(monkeypatch):
    google_module = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")

    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.Client = StubBQClient

    class ScalarQueryParameter:
        def __init__(self, *args, **kwargs):
            pass

    class QueryJobConfig:
        def __init__(self, *args, **kwargs):
            pass

    bigquery_module.ScalarQueryParameter = ScalarQueryParameter
    bigquery_module.QueryJobConfig = QueryJobConfig

    secretmanager_module = types.ModuleType("google.cloud.secretmanager")
    secretmanager_module.SecretManagerServiceClient = StubSecretManagerClient

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)
    monkeypatch.setitem(sys.modules, "google.cloud.secretmanager", secretmanager_module)

    sendgrid_module = types.ModuleType("sendgrid")
    sendgrid_module.SendGridAPIClient = StubSendGridClient
    monkeypatch.setitem(sys.modules, "sendgrid", sendgrid_module)

    helpers_module = types.ModuleType("sendgrid.helpers")
    mail_module = types.ModuleType("sendgrid.helpers.mail")
    mail_module.Asm = StubAsm
    mail_module.Email = StubEmail
    mail_module.Mail = StubMail
    monkeypatch.setitem(sys.modules, "sendgrid.helpers", helpers_module)
    monkeypatch.setitem(sys.modules, "sendgrid.helpers.mail", mail_module)


def load_module(monkeypatch):
    env = {
        "PROJECT_ID": "proj",
        "DATASET_ID": "dataset",
        "FROM_EMAIL": "sender@example.com",
        "EMAIL_SENDER_NAME": "Sender",
        "SENDGRID_TEMPLATE_ID": "d-template",
        "SENDGRID_SECRET_PATH": "projects/p/secrets/k/versions/latest",
        "TEST_MODE": "True",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    install_stubs(monkeypatch)

    repo_root = os.path.dirname(os.path.dirname(__file__))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    module_name = "fidelidade_email.main"
    if module_name in importlib.sys.modules:
        del importlib.sys.modules[module_name]
    import fidelidade_email.main

    return importlib.reload(fidelidade_email.main)


def test_prepare_email_data_handles_missing_current_tier(monkeypatch):
    main = load_module(monkeypatch)

    monkeypatch.setattr(main, "get_current_tier", lambda cpf: (None, None))
    monkeypatch.setattr(main, "get_past_tier", lambda cpf: "Bronze")
    monkeypatch.setattr(main, "get_min_max_points", lambda: {})

    payload = {
        "store_prefix": "z316",
        "sales_data": {
            "cliente_cpf": "000.000.000-00",
            "cliente_nome": "Cliente",
            "cliente_email": "cliente@example.com",
            "vendedor_nome": "Vendedor",
            "pedido_id": "P1",
            "pedido_dia": "2026-01-15",
        },
        "items_data": [{"produto_pontos_total": 10}],
    }

    result = main.prepare_email_data(payload)

    assert result["pedido_pontos"] == 10
    assert "registramos" in result["tier_message"]


def test_send_email_requires_test_email_in_test_mode(monkeypatch):
    main = load_module(monkeypatch)
    main.TEST_EMAIL = None

    with pytest.raises(ValueError) as exc:
        main.send_email({"cliente_email": "real@example.com"})

    assert "TEST_MODE is enabled" in str(exc.value)


def test_select_tier_message_handles_none_current_tier(monkeypatch):
    main = load_module(monkeypatch)

    msg = main.select_tier_message(
        None,
        "Bronze",
        {"purchase_points": 12, "current_points": None, "remaining_days": 10},
    )

    assert "classificação" in msg


class FakeRequest:
    def __init__(self, payload):
        self._payload = payload

    def get_json(self, silent=False):
        return self._payload


def test_main_accepts_http_pubsub_envelope(monkeypatch):
    main = load_module(monkeypatch)

    monkeypatch.setattr(main, "update_current_tier_data", lambda: None)
    monkeypatch.setattr(main, "prepare_email_data", lambda data: {"cliente_email": "a@b.com"})
    called = {"send": False}
    monkeypatch.setattr(main, "send_email", lambda email_data: called.__setitem__("send", True))

    payload = base64.b64encode(json.dumps({"store_prefix": "z316"}).encode("utf-8")).decode("utf-8")
    request = FakeRequest({"message": {"data": payload}})

    main.main(request)

    assert called["send"] is True


def test_main_accepts_background_signature_with_optional_context(monkeypatch):
    main = load_module(monkeypatch)

    monkeypatch.setattr(main, "update_current_tier_data", lambda: None)
    monkeypatch.setattr(main, "prepare_email_data", lambda data: {"cliente_email": "a@b.com"})
    called = {"send": False}
    monkeypatch.setattr(main, "send_email", lambda email_data: called.__setitem__("send", True))

    payload = base64.b64encode(json.dumps({"store_prefix": "z316"}).encode("utf-8")).decode("utf-8")
    event = {"data": payload}

    main.main(event, context={"event_id": "1"})

    assert called["send"] is True
