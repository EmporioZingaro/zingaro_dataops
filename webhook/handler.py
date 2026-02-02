import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

import google.cloud.storage as storage
from flask import abort, jsonify, make_response
from tenacity import before_log, retry, stop_after_attempt, wait_exponential

BUCKET_NAME_TEMPLATE = os.getenv("BUCKET_NAME_TEMPLATE", "{prefix}-tiny-webhook")
FILENAME_FORMAT = os.getenv(
    "FILENAME_FORMAT",
    "vendas/{prefix}-tiny-webhook-vendas-{dados_id}-{timestamp}-{unique_id}.json",
)
ALLOWED_SITUATIONS = {
    item.strip().upper()
    for item in os.getenv("ALLOWED_SITUATIONS", "FATURADO").split(",")
    if item.strip()
}


def load_cnpj_prefixes():
    raw_mapping = os.getenv("CNPJ_PREFIXES", "{}")
    try:
        mapping = json.loads(raw_mapping)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid CNPJ_PREFIXES JSON: {exc}") from exc
    if not isinstance(mapping, dict):
        raise ValueError("CNPJ_PREFIXES must be a JSON object")
    return mapping


CNPJ_PREFIXES = load_cnpj_prefixes()

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

storage_client = storage.Client()


@retry(
    wait=wait_exponential(multiplier=1, max=10),
    stop=stop_after_attempt(3),
    before=before_log(logger, logging.INFO),
    reraise=True,
)
def upload_to_gcs(blob, data):
    blob.upload_from_string(data, content_type="application/json")
    logger.info("Successfully uploaded %s", blob.name)


def normalize_cnpj(cnpj):
    return "".join(ch for ch in str(cnpj) if ch.isdigit())


def validate_payload(request_data):
    if not isinstance(request_data, dict):
        raise ValueError("Payload is not a JSON object")

    required_fields = ["versao", "cnpj", "tipo", "dados"]
    if not all(field in request_data for field in required_fields):
        raise ValueError("Payload missing required fields")
    if request_data["tipo"] != "inclusao_pedido":
        raise ValueError("Payload 'tipo' is not 'inclusao_pedido'")
    if not isinstance(request_data["dados"], dict):
        raise ValueError("Payload 'dados' is not a JSON object")


def resolve_store_prefix(cnpj):
    normalized_cnpj = normalize_cnpj(cnpj)
    prefix = CNPJ_PREFIXES.get(normalized_cnpj)
    if not prefix:
        raise ValueError(f"Unknown CNPJ: {normalized_cnpj}")
    return prefix


def resolve_situacao(dados):
    situacao = dados.get("descricaoSituacao") or dados.get("codigoSituacao")
    if not situacao:
        raise ValueError("Payload 'dados' missing situacao")
    return str(situacao).strip().upper()


def resolve_bucket_name(prefix):
    return BUCKET_NAME_TEMPLATE.format(prefix=prefix)


def generate_filename(prefix, dados_id, timestamp, unique_id):
    return FILENAME_FORMAT.format(
        prefix=prefix,
        dados_id=dados_id,
        timestamp=timestamp,
        unique_id=unique_id,
    )


def erp_webhook_handler(request):
    if request.method != "POST":
        return make_response("Method not allowed", 405)

    request_data = request.get_json(silent=True)
    if not request_data:
        return make_response("No payload found", 400)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Received Tiny ERP payload: %s", json.dumps(request_data))

    try:
        validate_payload(request_data)
        prefix = resolve_store_prefix(request_data["cnpj"])
        situacao = resolve_situacao(request_data["dados"])
        if situacao not in ALLOWED_SITUATIONS:
            message = (
                f"Ignored payload: situacao '{situacao}' not allowed for "
                f"prefix '{prefix}'"
            )
            logger.info(message)
            return jsonify(message=message), 200
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Accepted payload: prefix=%s, situacao=%s, dados_id=%s",
                prefix,
                situacao,
                request_data.get("dados", {}).get("id"),
            )
    except ValueError as exc:
        if str(exc) == "Payload 'tipo' is not 'inclusao_pedido'":
            logger.info("Ignored payload: %s", exc)
            return jsonify(message=f"Ignored payload: {exc}"), 200
        logger.error("Invalid payload: %s", exc)
        abort(400, description=f"Invalid payload: {exc}")

    dados_id = request_data.get("dados", {}).get("id", "unknown")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    unique_id = uuid.uuid4()
    filename = generate_filename(prefix, dados_id, timestamp, unique_id)
    bucket_name = resolve_bucket_name(prefix)

    blob = storage_client.bucket(bucket_name).blob(filename)
    data = json.dumps(request_data)

    try:
        upload_to_gcs(blob, data)
    except Exception as exc:
        logger.exception("Failed to upload %s after retries", filename)
        abort(500, description=f"Failed to upload {filename} after retries")

    logger.info(
        "Stored payload for prefix '%s' in bucket '%s' as '%s'",
        prefix,
        bucket_name,
        filename,
    )
    return jsonify(message=f"Payload stored in {filename}", filename=filename), 200
