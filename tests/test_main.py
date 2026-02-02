import importlib
import json
import os
import sys
import types

import pytest


class FakeResponse:
    def __init__(self, payload=None, status=None):
        self.json = payload
        self.status = status


def fake_jsonify(**kwargs):
    return FakeResponse(payload=kwargs)


def fake_make_response(message, status):
    return FakeResponse(payload={"message": message}, status=status)


def fake_abort(status, description=None):
    raise Exception(description or f"Abort {status}")


def build_flask_stub():
    flask_stub = types.ModuleType("flask")
    flask_stub.abort = fake_abort
    flask_stub.jsonify = fake_jsonify
    flask_stub.make_response = fake_make_response
    return flask_stub


def build_storage_stub():
    storage_stub = types.ModuleType("google.cloud.storage")

    class StubClient:
        def __init__(self, *args, **kwargs):
            pass

    storage_stub.Client = StubClient
    return storage_stub


def install_google_storage_stub(monkeypatch):
    google_module = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")
    storage_module = build_storage_stub()

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_module)


def build_tenacity_stub():
    tenacity_stub = types.ModuleType("tenacity")

    def retry(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    def wait_exponential(*args, **kwargs):
        return None

    def stop_after_attempt(*args, **kwargs):
        return None

    def before_log(*args, **kwargs):
        return None

    tenacity_stub.retry = retry
    tenacity_stub.wait_exponential = wait_exponential
    tenacity_stub.stop_after_attempt = stop_after_attempt
    tenacity_stub.before_log = before_log
    return tenacity_stub


def load_main_with_env(monkeypatch, env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setitem(sys.modules, "flask", build_flask_stub())
    install_google_storage_stub(monkeypatch)
    monkeypatch.setitem(sys.modules, "tenacity", build_tenacity_stub())
    repo_root = os.path.dirname(os.path.dirname(__file__))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    if "main" in importlib.sys.modules:
        del importlib.sys.modules["main"]
    import main

    return importlib.reload(main)


class FakeBlob:
    def __init__(self, name):
        self.name = name
        self.uploaded = None
        self.content_type = None

    def upload_from_string(self, data, content_type=None):
        self.uploaded = data
        self.content_type = content_type


class FakeBucket:
    def __init__(self, name):
        self.name = name
        self.last_blob = None

    def blob(self, name):
        self.last_blob = FakeBlob(name)
        return self.last_blob


class FakeStorageClient:
    def __init__(self):
        self.last_bucket = None

    def bucket(self, name):
        self.last_bucket = FakeBucket(name)
        return self.last_bucket


def make_payload(tipo="inclusao_pedido", situacao="Faturado"):
    return {
        "versao": "1.0.1",
        "cnpj": "22.945.440/0001-54",
        "tipo": tipo,
        "dados": {
            "id": "885188291",
            "descricaoSituacao": situacao,
        },
    }


class FakeRequest:
    def __init__(self, payload, method="POST"):
        self._payload = payload
        self.method = method
        self.data = json.dumps(payload).encode("utf-8") if payload else b""

    def get_json(self, silent=False):
        return self._payload


def build_request(payload):
    return FakeRequest(payload)


def test_accepts_faturado_payload(monkeypatch):
    env = {
        "CNPJ_PREFIXES": json.dumps({"22945440000154": "Z316"}),
        "BUCKET_NAME_TEMPLATE": "{prefix}-tiny-webhook",
    }
    main = load_main_with_env(monkeypatch, env)
    main.storage_client = FakeStorageClient()
    request = build_request(make_payload())
    response, status = main.erp_webhook_handler(request)

    assert status == 200
    assert response.json["filename"].startswith("vendas/Z316-")
    assert main.storage_client.last_bucket.name == "Z316-tiny-webhook"
    assert main.storage_client.last_bucket.last_blob.uploaded
    assert (
        main.storage_client.last_bucket.last_blob.content_type
        == "application/json"
    )


def test_ignores_non_faturado_situacao(monkeypatch):
    env = {
        "CNPJ_PREFIXES": json.dumps({"22945440000154": "Z316"}),
    }
    main = load_main_with_env(monkeypatch, env)
    main.storage_client = FakeStorageClient()
    request = build_request(make_payload(situacao="Em andamento"))
    response, status = main.erp_webhook_handler(request)

    assert status == 200
    assert "Ignored payload" in response.json["message"]
    assert main.storage_client.last_bucket is None


def test_rejects_unknown_cnpj(monkeypatch):
    env = {
        "CNPJ_PREFIXES": json.dumps({"11111111111111": "Z999"}),
    }
    main = load_main_with_env(monkeypatch, env)
    request = build_request(make_payload())
    with pytest.raises(Exception) as excinfo:
        main.erp_webhook_handler(request)

    assert "Unknown CNPJ" in str(excinfo.value)


def test_ignores_non_inclusao_tipo(monkeypatch):
    env = {
        "CNPJ_PREFIXES": json.dumps({"22945440000154": "Z316"}),
    }
    main = load_main_with_env(monkeypatch, env)
    request = build_request(make_payload(tipo="atualizacao_pedido"))
    response, status = main.erp_webhook_handler(request)

    assert status == 200
    assert "Ignored payload" in response.json["message"]
