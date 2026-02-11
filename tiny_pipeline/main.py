import hashlib
import json
import logging
import os
import re
import uuid
import base64
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from google.cloud import pubsub_v1, secretmanager, storage
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

STORE_CONFIGS_ENV = "STORE_CONFIGS"
PUBSUB_TOPIC_ENV = "PUBSUB_TOPIC"
WEBHOOK_BUCKET_SUFFIX = "-tiny-webhook"
WEBHOOK_FILENAME_PATTERN = re.compile(
    r"vendas/(?P<prefix>.+?)-tiny-webhook-vendas-.*-"
    r"(?P<timestamp>\d{8}T\d{6})-(?P<uuid>[0-9a-fA-F-]{36})\.json$"
)
FILENAME_SUFFIX_PATTERN = re.compile(
    r"-(?P<timestamp>\d{8}T\d{6})-(?P<uuid>[0-9a-fA-F-]{36})\.json$"
)
REQUEST_TIMEOUT_SECONDS = 30

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
secret_manager_client = secretmanager.SecretManagerServiceClient()

token_cache: Dict[str, str] = {}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


class InvalidTokenError(Exception):
    pass


class RetryableError(Exception):
    pass


@dataclass(frozen=True)
class StoreConfig:
    base_url: str
    secret_path: str
    target_bucket_name: str
    folder_name: str
    file_prefix: str
    pdv_filename: str
    pesquisa_filename: str
    produto_filename: str
    nfce_filename: str
    project_id: str
    source_identifier: str
    version_control: str


def load_store_configs() -> Dict[str, StoreConfig]:
    raw_mapping = os.getenv(STORE_CONFIGS_ENV)
    if not raw_mapping:
        raise ValueError(f"Missing required env var {STORE_CONFIGS_ENV}")

    try:
        mapping = json.loads(raw_mapping)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid {STORE_CONFIGS_ENV} JSON: {exc}") from exc

    if not isinstance(mapping, dict):
        raise ValueError(f"{STORE_CONFIGS_ENV} must be a JSON object")

    configs: Dict[str, StoreConfig] = {}
    for prefix, config in mapping.items():
        if not isinstance(config, dict):
            raise ValueError(f"Store config for '{prefix}' must be a JSON object")
        configs[prefix] = StoreConfig(
            base_url=config["base_url"],
            secret_path=config["secret_path"],
            target_bucket_name=config["target_bucket_name"],
            folder_name=config["folder_name"],
            file_prefix=config["file_prefix"],
            pdv_filename=config["pdv_filename"],
            pesquisa_filename=config["pesquisa_filename"],
            produto_filename=config["produto_filename"],
            nfce_filename=config["nfce_filename"],
            project_id=config["project_id"],
            source_identifier=config["source_identifier"],
            version_control=config["version_control"],
        )

    if not configs:
        raise ValueError(f"{STORE_CONFIGS_ENV} must include at least one store config")

    return configs


STORE_CONFIGS = load_store_configs()
PUBSUB_TOPIC = os.getenv(PUBSUB_TOPIC_ENV)
if not PUBSUB_TOPIC:
    raise ValueError(f"Missing required env var {PUBSUB_TOPIC_ENV}")


def resolve_store_prefix(event: dict) -> str:
    bucket_name = event.get("bucket")
    if bucket_name and bucket_name.endswith(WEBHOOK_BUCKET_SUFFIX):
        prefix = bucket_name[: -len(WEBHOOK_BUCKET_SUFFIX)]
        if prefix:
            return prefix

    filename = event.get("name")
    if filename:
        match = WEBHOOK_FILENAME_PATTERN.search(filename)
        if match:
            return match.group("prefix")

    raise ValueError("Unable to resolve store prefix from GCS event")


def get_store_config(prefix: str) -> StoreConfig:
    try:
        return STORE_CONFIGS[prefix]
    except KeyError as exc:
        raise ValueError(f"No store config found for prefix '{prefix}'") from exc


def get_api_token(prefix: str, secret_path: str) -> str:
    if prefix in token_cache:
        return token_cache[prefix]

    logger.debug("Accessing API token from Secret Manager for prefix '%s'", prefix)
    response = secret_manager_client.access_secret_version(
        request={"name": secret_path}
    )
    token = response.payload.data.decode("UTF-8")
    token_cache[prefix] = token
    return token


@retry(
    wait=wait_exponential(multiplier=2.5, min=30, max=187.5),
    stop=stop_after_attempt(4),
    retry=retry_if_exception_type((requests.exceptions.RequestException, RetryableError)),
)
def make_api_call(url: str) -> dict:
    sanitized_url = url.split("?token=")[0]
    logger.debug("Making API call to: %s", sanitized_url)
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    json_data = response.json()

    validate_json_payload(json_data)

    return json_data


def validate_json_payload(json_data: dict) -> None:
    status_processamento = json_data.get("retorno", {}).get("status_processamento")

    if status_processamento == "3":
        return
    if status_processamento == "2":
        raise ValidationError("Invalid query parameter.")
    if status_processamento == "1":
        codigo_erro = json_data.get("retorno", {}).get("codigo_erro")
        erros = json_data.get("retorno", {}).get("erros", [])
        erro_message = erros[0].get("erro") if erros else "Unknown error"
        if codigo_erro == "1":
            raise InvalidTokenError(f"Token is not valid: {erro_message}")
        raise RetryableError(f"Error encountered, will attempt retry: {erro_message}")


def read_webhook_payload(bucket_name: str, file_name: str) -> dict:
    logger.debug(
        "Reading webhook payload from bucket: %s, file: %s", bucket_name, file_name
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    return json.loads(blob.download_as_string(client=None))


def extract_payload_details(event: dict) -> Optional[Tuple[str, str, str]]:
    file_name = event["name"]
    webhook_payload = read_webhook_payload(event["bucket"], file_name)
    dados_id = webhook_payload.get("dados", {}).get("id")

    if not dados_id:
        logger.warning("dados.id not found in webhook payload")
        return None

    timestamp, uuid_str = parse_filename_suffix(file_name)
    return dados_id, timestamp, uuid_str


def parse_filename_suffix(file_name: str) -> Tuple[str, str]:
    match = FILENAME_SUFFIX_PATTERN.search(file_name)
    if match:
        return match.group("timestamp"), match.group("uuid")

    logger.warning("Unable to parse timestamp/uuid from filename: %s", file_name)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    uuid_str = str(uuid4_hex())
    return timestamp, uuid_str


def uuid4_hex() -> str:
    return str(uuid.uuid4())


def parse_http_request_payload(request: Any) -> dict:
    payload = request.get_json(silent=True) or {}

    if "bucket" in payload and "name" in payload:
        return payload

    if "data" in payload and isinstance(payload["data"], dict):
        data_payload = payload["data"]
        if "bucket" in data_payload and "name" in data_payload:
            return data_payload

    pubsub_message = payload.get("message", {})
    encoded_data = pubsub_message.get("data")
    if encoded_data:
        decoded = base64.b64decode(encoded_data).decode("utf-8")
        decoded_payload = json.loads(decoded)
        if "bucket" in decoded_payload and "name" in decoded_payload:
            return decoded_payload

    raise ValueError("Unable to parse storage event from HTTP request payload")


def normalize_event_context(event: Any, context: Any) -> Tuple[dict, Any]:
    if context is not None:
        return event, context

    if isinstance(event, dict):
        return event, SimpleNamespace(event_id="unknown", is_http_invocation=False)

    parsed_event = parse_http_request_payload(event)
    event_id = event.headers.get("Ce-Id", "unknown")
    return parsed_event, SimpleNamespace(event_id=event_id, is_http_invocation=True)


def should_return_http_response(context: Any) -> bool:
    return bool(getattr(context, "is_http_invocation", False))


def http_success_response(context: Any) -> Optional[Tuple[str, int]]:
    if should_return_http_response(context):
        return ("", 204)
    return None


def process_webhook_payload(event: Any, context: Any = None) -> Any:
    event, context = normalize_event_context(event, context)
    success_response = http_success_response(context)
    logger.info("Function execution started - Context: %s", context.event_id)
    try:
        prefix = resolve_store_prefix(event)
        store_config = get_store_config(prefix)
        payload_details = extract_payload_details(event)
        if not payload_details:
            return success_response

        dados_id, timestamp, uuid_str = payload_details
        token = get_api_token(prefix, store_config.secret_path)

        pdv_pedido_data, pedido_numero, produto_data = process_pdv_pedido_data(
            store_config, dados_id, timestamp, uuid_str, token
        )
        pedidos_pesquisa_data = process_pedidos_pesquisa_data(
            store_config, dados_id, timestamp, uuid_str, token, pedido_numero
        )

        nota_fiscal_link_data = None
        try:
            nfce_id = process_nfce_generation(store_config, dados_id, token)
            if nfce_id:
                nota_fiscal_link_data = process_nota_fiscal_link_retrieval(
                    store_config,
                    nfce_id,
                    token,
                    dados_id,
                    timestamp,
                    uuid_str,
                    pedido_numero,
                )
        except Exception as exc:
            logger.exception(
                "An error occurred during NFC-e generation or link fetching: %s", exc
            )

        publish_notification(
            prefix,
            store_config,
            pdv_pedido_data,
            produto_data,
            pedidos_pesquisa_data,
            nota_fiscal_link_data,
            timestamp,
            uuid_str,
        )

    except InvalidTokenError as exc:
        logger.exception("Invalid token for prefix '%s': %s", prefix, exc)
    except Exception as exc:
        logger.exception(
            "Function failed: %s - Context: %s", exc, context.event_id
        )

    logger.info("Function execution completed - Context: %s", context.event_id)
    return success_response


def process_pdv_pedido_data(
    store_config: StoreConfig,
    dados_id: str,
    timestamp: str,
    uuid_str: str,
    token: str,
) -> Tuple[dict, str, List[dict]]:
    logger.debug(
        "Processing PDV pedido data - dados_id: %s, timestamp: %s, uuid_str: %s",
        dados_id,
        timestamp,
        uuid_str,
    )
    folder_path = store_config.folder_name.format(
        timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str
    )
    pdv_pedido_data = fetch_pdv_pedido_data(store_config.base_url, dados_id, token)
    pedido_numero = (
        pdv_pedido_data.get("retorno", {}).get("pedido", {}).get("numero")
    )
    store_payload(
        store_config,
        pdv_pedido_data,
        store_config.pdv_filename.format(
            dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str
        ),
        folder_path,
        {
            "uuid_str": uuid_str,
            "pedido_id": pedido_numero,
            "data_type": "pdv.pedido",
        },
    )

    produto_payloads = []
    item_ids = collect_unique_product_ids(
        pdv_pedido_data.get("retorno", {}).get("pedido", {}).get("itens", [])
    )
    for item_id in item_ids:
        produto_payload = fetch_produto_data(store_config.base_url, item_id, token)
        produto_payloads.append(produto_payload)
        store_payload(
            store_config,
            produto_payload,
            store_config.produto_filename.format(
                dados_id=dados_id,
                produto_id=item_id,
                timestamp=timestamp,
                uuid_str=uuid_str,
            ),
            folder_path,
            {
                "uuid_str": uuid_str,
                "pedido_id": pedido_numero,
                "produto_id": item_id,
                "data_type": "produto",
            },
        )

    return pdv_pedido_data, pedido_numero, produto_payloads


def collect_unique_product_ids(items: Iterable[dict]) -> List[str]:
    item_ids = set()
    for item in items:
        item_id = item.get("idProduto")
        if item_id:
            item_ids.add(str(item_id))
    return list(item_ids)


def process_pedidos_pesquisa_data(
    store_config: StoreConfig,
    dados_id: str,
    timestamp: str,
    uuid_str: str,
    token: str,
    pedido_numero: str,
) -> dict:
    logger.debug(
        "Processing pedidos pesquisa data - dados_id: %s, timestamp: %s, uuid_str: %s, "
        "pedido_numero: %s",
        dados_id,
        timestamp,
        uuid_str,
        pedido_numero,
    )
    folder_path = store_config.folder_name.format(
        timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str
    )
    pedidos_data = fetch_pedidos_pesquisa_data(
        store_config.base_url, pedido_numero, token
    )
    store_payload(
        store_config,
        pedidos_data,
        store_config.pesquisa_filename.format(
            dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str
        ),
        folder_path,
        {
            "uuid_str": uuid_str,
            "pedido_id": pedido_numero,
            "data_type": "pedidos.pesquisa",
        },
    )
    return pedidos_data


def process_nfce_generation(
    store_config: StoreConfig, dados_id: str, token: str
) -> Optional[str]:
    try:
        response = fetch_nfce_id(store_config.base_url, dados_id, token)
    except ValidationError as exc:
        logger.warning(
            "Skipping NFC-e generation for dados_id %s: %s",
            dados_id,
            exc,
        )
        return None

    registro = (
        response.get("retorno", {})
        .get("registros", {})
        .get("registro", {})
    )
    nfce_id = registro.get("idNotaFiscal")
    if nfce_id:
        logger.info("NFC-e generated successfully with idNotaFiscal: %s", nfce_id)
        return nfce_id
    logger.warning(
        "Skipping NFC-e link retrieval for dados_id %s: NFC-e generation response is missing expected fields.",
        dados_id,
    )
    return None


def process_nota_fiscal_link_retrieval(
    store_config: StoreConfig,
    id_notafiscal: str,
    token: str,
    dados_id: str,
    timestamp: str,
    uuid_str: str,
    pedido_numero: str,
) -> dict:
    response = fetch_nota_fiscal_link(store_config.base_url, id_notafiscal, token)
    logger.info(
        "Successfully fetched Nota Fiscal link payload for idNotafiscal: %s",
        id_notafiscal,
    )
    folder_path = store_config.folder_name.format(
        timestamp=timestamp, dados_id=dados_id, uuid_str=uuid_str
    )
    store_payload(
        store_config,
        response,
        store_config.nfce_filename.format(
            dados_id=dados_id, timestamp=timestamp, uuid_str=uuid_str
        ),
        folder_path,
        {
            "uuid_str": uuid_str,
            "nfce_id": id_notafiscal,
            "data_type": "nfce.link",
            "pedido_id": pedido_numero,
        },
    )
    return response


def fetch_pdv_pedido_data(base_url: str, dados_id: str, token: str) -> dict:
    logger.debug("Fetching PDV pedido data - dados_id: %s", dados_id)
    return make_api_call(f"{base_url}pdv.pedido.obter.php?token={token}&id={dados_id}")


def fetch_produto_data(base_url: str, item_id: str, token: str) -> dict:
    logger.debug("Fetching produto data - item_id: %s", item_id)
    return make_api_call(
        f"{base_url}produto.obter.php?token={token}&id={item_id}&formato=JSON"
    )


def fetch_pedidos_pesquisa_data(base_url: str, pedido_numero: str, token: str) -> dict:
    logger.debug("Fetching pedidos pesquisa data - pedido_numero: %s", pedido_numero)
    return make_api_call(
        f"{base_url}pedidos.pesquisa.php?token={token}&numero={pedido_numero}&formato=JSON"
    )


def fetch_nfce_id(base_url: str, dados_id: str, token: str) -> dict:
    logger.debug("Fetching NFC-e ID for dados_id: %s", dados_id)
    url = (
        f"{base_url}gerar.nota.fiscal.pedido.php?token={token}&formato=JSON"
        f"&id={dados_id}&modelo=NFCe"
    )
    return make_api_call(url)


def fetch_nota_fiscal_link(base_url: str, id_notafiscal: str, token: str) -> dict:
    logger.debug("Fetching Nota Fiscal link for idNotafiscal: %s", id_notafiscal)
    url = (
        f"{base_url}nota.fiscal.obter.link.php?token={token}&formato=JSON&id={id_notafiscal}"
    )
    return make_api_call(url)


def generate_checksum(data: dict) -> str:
    logger.debug("Generating checksum")
    return hashlib.md5(json.dumps(data, sort_keys=True).encode("utf-8")).hexdigest()


def build_metadata(
    store_config: StoreConfig,
    metadata: dict,
    checksum: str,
    processing_timestamp: str,
) -> Dict[str, str]:
    full_metadata = {
        "Processing-Timestamp": processing_timestamp,
        "Checksum": checksum,
        "Project-ID": store_config.project_id,
        "Source-Identifier": store_config.source_identifier,
        "Version-Control": store_config.version_control,
    }

    optional_fields = {
        "UUID": metadata.get("uuid_str"),
        "Pedido-ID": metadata.get("pedido_id"),
        "Produto-ID": metadata.get("produto_id"),
        "NFCe-ID": metadata.get("nfce_id"),
        "Data-Type": metadata.get("data_type"),
    }

    for key, value in optional_fields.items():
        if value is not None:
            full_metadata[key] = str(value)

    return full_metadata


def store_payload(
    store_config: StoreConfig, data: dict, filename_template: str, folder_path: str, metadata: dict
) -> None:
    file_path = f"{folder_path}/{store_config.file_prefix}{filename_template}.json"
    logger.debug("Storing payload in GCS at: %s", file_path)

    checksum = generate_checksum(data)
    processing_timestamp = datetime.utcnow().isoformat() + "Z"
    full_metadata = build_metadata(store_config, metadata, checksum, processing_timestamp)

    bucket = storage_client.bucket(store_config.target_bucket_name)
    blob = bucket.blob(file_path)
    blob.metadata = full_metadata
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    logger.debug("Payload stored with metadata: %s", full_metadata)


def publish_notification(
    prefix: str,
    store_config: StoreConfig,
    pdv_pedido_data: dict,
    produto_payloads: list,
    pedidos_pesquisa_data: dict,
    nota_fiscal_link_data: Optional[dict],
    timestamp: str,
    uuid_str: str,
) -> None:
    message = {
        "store_prefix": prefix,
        "pdv_pedido_data": pdv_pedido_data,
        "produto_data": produto_payloads,
        "pedidos_pesquisa_data": pedidos_pesquisa_data,
        "nota_fiscal_link_data": nota_fiscal_link_data,
        "timestamp": timestamp,
        "uuid": uuid_str,
    }
    serialized_message = json.dumps(message, ensure_ascii=False)
    payload = serialized_message.encode("utf-8")
    logger.info("Notification published to %s", PUBSUB_TOPIC)
    future = publisher.publish(PUBSUB_TOPIC, data=payload)
    future.result()
