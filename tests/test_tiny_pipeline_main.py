import importlib
import json
import os
import sys
import types


def install_google_cloud_stubs(monkeypatch):
    google_module = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")

    storage_module = types.ModuleType("google.cloud.storage")
    pubsub_module = types.ModuleType("google.cloud.pubsub_v1")
    secret_module = types.ModuleType("google.cloud.secretmanager")

    class StubStorageClient:
        def __init__(self, *args, **kwargs):
            pass

    class StubPublisherClient:
        def __init__(self, *args, **kwargs):
            pass

    class StubSecretManagerClient:
        def __init__(self, *args, **kwargs):
            pass

    storage_module.Client = StubStorageClient
    pubsub_module.PublisherClient = StubPublisherClient
    secret_module.SecretManagerServiceClient = StubSecretManagerClient

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.storage", storage_module)
    monkeypatch.setitem(sys.modules, "google.cloud.pubsub_v1", pubsub_module)
    monkeypatch.setitem(sys.modules, "google.cloud.secretmanager", secret_module)


def load_tiny_pipeline_main(monkeypatch):
    store_configs = {
        "Z1": {
            "base_url": "https://api.tiny.com.br/api2/",
            "secret_path": "projects/p/secrets/s/versions/latest",
            "target_bucket_name": "bucket",
            "folder_name": "folder/{timestamp}",
            "file_prefix": "prefix-",
            "pdv_filename": "pdv-{dados_id}",
            "pesquisa_filename": "pesquisa-{dados_id}",
            "produto_filename": "produto-{dados_id}",
            "nfce_filename": "nfce-{dados_id}",
            "project_id": "project",
            "source_identifier": "source",
            "version_control": "v1",
        }
    }
    monkeypatch.setenv("STORE_CONFIGS", json.dumps(store_configs))
    monkeypatch.setenv("PUBSUB_TOPIC", "projects/p/topics/t")
    install_google_cloud_stubs(monkeypatch)

    repo_root = os.path.dirname(os.path.dirname(__file__))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    module_name = "tiny_pipeline.main"
    if module_name in importlib.sys.modules:
        del importlib.sys.modules[module_name]

    import tiny_pipeline.main

    return importlib.reload(tiny_pipeline.main)


def test_fetch_nota_fiscal_link_retries_with_id_notafiscal(monkeypatch):
    main = load_tiny_pipeline_main(monkeypatch)
    called_urls = []

    def fake_make_api_call(url):
        called_urls.append(url)
        if "&id=" in url:
            raise main.ValidationError("Invalid query parameter.")
        return {"retorno": {"status_processamento": "3", "link_nfe": "ok"}}

    monkeypatch.setattr(main, "make_api_call", fake_make_api_call)

    response = main.fetch_nota_fiscal_link("https://base/", "123", "token")

    assert response["retorno"]["link_nfe"] == "ok"
    assert len(called_urls) == 2
    assert "&id=123" in called_urls[0]
    assert "&idNotaFiscal=123" in called_urls[1]


def test_fetch_nota_fiscal_data_raises_original_validation_error(monkeypatch):
    main = load_tiny_pipeline_main(monkeypatch)

    def fake_make_api_call(url):
        if "&id=" in url:
            raise main.ValidationError("Invalid query parameter.")
        raise main.ValidationError("Different validation error")

    monkeypatch.setattr(main, "make_api_call", fake_make_api_call)

    try:
        main.fetch_nota_fiscal_data("https://base/", "456", "token")
        assert False, "Expected ValidationError"
    except main.ValidationError as exc:
        assert str(exc) == "Different validation error"
