import base64
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any, Dict, List, Sequence, Tuple

from google.api_core import retry as g_retry
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound

getcontext().prec = 28

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
resolved_level = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(level=resolved_level)
logger = logging.getLogger(__name__)
logger.setLevel(resolved_level)

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_BASE_ID = os.getenv("DATASET_BASE_ID")
PEDIDOS_TABLE_NAME = os.getenv("PEDIDOS_TABLE_NAME", "pedidos")
ITENS_PEDIDO_TABLE_NAME = os.getenv("ITENS_PEDIDO_TABLE_NAME", "produtos")

# Backward-compatible full table IDs (legacy mode only)
PEDIDOS_TABLE_ID = os.getenv("PEDIDOS_TABLE_ID")
ITENS_PEDIDO_TABLE_ID = os.getenv("ITENS_PEDIDO_TABLE_ID")

SOURCE_ID = os.getenv("SOURCE_ID", "")

logger.debug(
    "Configuration loaded | PROJECT_ID=%s DATASET_BASE_ID=%s PEDIDOS_TABLE_NAME=%s "
    "ITENS_PEDIDO_TABLE_NAME=%s has_legacy_pedidos=%s has_legacy_itens=%s",
    PROJECT_ID,
    DATASET_BASE_ID,
    PEDIDOS_TABLE_NAME,
    ITENS_PEDIDO_TABLE_NAME,
    bool(PEDIDOS_TABLE_ID),
    bool(ITENS_PEDIDO_TABLE_ID),
)

client = bigquery.Client(project=PROJECT_ID)

pedidos_schema = [
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "STRING"),
    bigquery.SchemaField("pedido_dia", "DATE"),
    bigquery.SchemaField("pedido_id", "STRING"),
    bigquery.SchemaField("pedido_numero", "STRING"),
    bigquery.SchemaField("cliente_nome", "STRING"),
    bigquery.SchemaField("cliente_cpf", "STRING"),
    bigquery.SchemaField("cliente_email", "STRING"),
    bigquery.SchemaField("cliente_celular", "STRING"),
    bigquery.SchemaField("vendedor_nome", "STRING"),
    bigquery.SchemaField("vendedor_id", "STRING"),
    bigquery.SchemaField("valor_produtos_custo", "FLOAT"),
    bigquery.SchemaField("valor_produtos_sem_desconto", "FLOAT"),
    bigquery.SchemaField("desconto_produtos", "FLOAT"),
    bigquery.SchemaField("desconto_pedido", "FLOAT"),
    bigquery.SchemaField("desconto_total", "FLOAT"),
    bigquery.SchemaField("valor_faturado", "FLOAT"),
    bigquery.SchemaField("valor_lucro", "FLOAT"),
    bigquery.SchemaField("forma_pagamento", "STRING"),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("processed_timestamp", "TIMESTAMP"),
]

itens_pedido_schema = [
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "STRING"),
    bigquery.SchemaField("pedido_dia", "DATE"),
    bigquery.SchemaField("pedido_id", "STRING"),
    bigquery.SchemaField("pedido_numero", "STRING"),
    bigquery.SchemaField("cliente_nome", "STRING"),
    bigquery.SchemaField("cliente_cpf", "STRING"),
    bigquery.SchemaField("cliente_email", "STRING"),
    bigquery.SchemaField("cliente_celular", "STRING"),
    bigquery.SchemaField("vendedor_nome", "STRING"),
    bigquery.SchemaField("vendedor_id", "STRING"),
    bigquery.SchemaField("produto_id", "STRING"),
    bigquery.SchemaField("produto_nome", "STRING"),
    bigquery.SchemaField("produto_categoria_principal", "STRING"),
    bigquery.SchemaField("produto_categoria_secundaria", "STRING"),
    bigquery.SchemaField("produto_valor_custo_und", "FLOAT"),
    bigquery.SchemaField("produto_valor_sem_desconto_und", "FLOAT"),
    bigquery.SchemaField("produto_valor_com_desconto_und", "FLOAT"),
    bigquery.SchemaField("produto_valor_lucro_und", "FLOAT"),
    bigquery.SchemaField("desconto_produto_und", "FLOAT"),
    bigquery.SchemaField("desconto_pedido_und", "FLOAT"),
    bigquery.SchemaField("desconto_total_und", "FLOAT"),
    bigquery.SchemaField("produto_quantidade", "FLOAT"),
    bigquery.SchemaField("desconto_produto", "FLOAT"),
    bigquery.SchemaField("desconto_pedido", "FLOAT"),
    bigquery.SchemaField("desconto_total", "FLOAT"),
    bigquery.SchemaField("total_produto_valor_custo", "FLOAT"),
    bigquery.SchemaField("total_produto_valor_sem_desconto", "FLOAT"),
    bigquery.SchemaField("total_produto_valor_faturado", "FLOAT"),
    bigquery.SchemaField("total_produto_valor_lucro", "FLOAT"),
    bigquery.SchemaField("forma_pagamento", "STRING"),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("processed_timestamp", "TIMESTAMP"),
]


def decimal_value(raw_value: Any, default: str = "0") -> Decimal:
    if raw_value is None:
        return Decimal(default)
    if isinstance(raw_value, Decimal):
        return raw_value

    candidate = str(raw_value).strip().replace(",", ".")
    if not candidate:
        return Decimal(default)

    try:
        return Decimal(candidate)
    except (InvalidOperation, ValueError):
        logger.warning(
            "Numeric parsing failed. Using safe default | raw_value=%r default=%s", raw_value, default
        )
        return Decimal(default)


def normalize_store_prefix(store_prefix: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", store_prefix.strip().lower()).strip("_")
    if not normalized:
        raise ValueError("store_prefix is empty after normalization")
    return normalized


def parse_discount(discount_string: Any, base_amount: Decimal) -> Decimal:
    value = str(discount_string or "").strip()
    if not value:
        return Decimal("0")

    if "%" in value:
        percent = decimal_value(value.replace("%", ""))
        discount = max((percent / Decimal("100")) * base_amount, Decimal("0"))
        logger.debug(
            "Parsed percentage discount | raw=%s base_amount=%s percent=%s discount=%s",
            discount_string,
            base_amount,
            percent,
            discount,
        )
        return discount

    discount = max(decimal_value(value), Decimal("0"))
    logger.debug("Parsed absolute discount | raw=%s discount=%s", discount_string, discount)
    return discount


def transform_date_format(date_str: str) -> str:
    try:
        transformed = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        logger.debug("Date transformed | source=%s transformed=%s", date_str, transformed)
        return transformed
    except ValueError:
        logger.warning(
            "Date transformation failed. Keeping original date string | source=%s", date_str
        )
        return date_str


def resolve_table_ids(store_prefix: str) -> Tuple[str, str]:
    if DATASET_BASE_ID:
        if not PROJECT_ID:
            raise ValueError("PROJECT_ID is required when DATASET_BASE_ID is configured")

        normalized_prefix = normalize_store_prefix(store_prefix)
        dataset_id = f"{normalized_prefix}_{DATASET_BASE_ID}"
        pedidos_table = f"{PROJECT_ID}.{dataset_id}.{PEDIDOS_TABLE_NAME}"
        itens_table = f"{PROJECT_ID}.{dataset_id}.{ITENS_PEDIDO_TABLE_NAME}"

        logger.info(
            "Resolved destination (multi-store mode) | store_prefix=%s normalized_prefix=%s dataset_id=%s pedidos_table=%s itens_table=%s",
            store_prefix,
            normalized_prefix,
            dataset_id,
            pedidos_table,
            itens_table,
        )
        return pedidos_table, itens_table

    if PEDIDOS_TABLE_ID and ITENS_PEDIDO_TABLE_ID:
        logger.info(
            "Resolved destination (legacy mode) | pedidos_table=%s itens_table=%s",
            PEDIDOS_TABLE_ID,
            ITENS_PEDIDO_TABLE_ID,
        )
        return PEDIDOS_TABLE_ID, ITENS_PEDIDO_TABLE_ID

    raise ValueError(
        "Destination config missing. Set DATASET_BASE_ID (+ PROJECT_ID) or legacy table ids."
    )


def create_table_if_not_exists(
    table_id: str,
    schema: Sequence[bigquery.SchemaField],
    partition_field: str,
    clustering_fields: Sequence[str],
) -> None:
    try:
        client.get_table(table_id)
        logger.debug("Table already exists | table_id=%s", table_id)
        return
    except NotFound:
        logger.info(
            "Table does not exist. Creating table | table_id=%s partition_field=%s clustering_fields=%s",
            table_id,
            partition_field,
            list(clustering_fields),
        )

    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=partition_field,
    )
    table.clustering_fields = list(clustering_fields)

    try:
        client.create_table(table)
        logger.info("Table created successfully | table_id=%s", table_id)
    except Conflict:
        logger.info("Table was created concurrently by another execution | table_id=%s", table_id)


def insert_rows_json(table_id: str, rows: List[Dict[str, Any]], insert_ids: List[str]) -> None:
    table = client.get_table(table_id)

    retry_policy = g_retry.Retry(
        predicate=g_retry.if_transient_error,
        initial=1,
        maximum=30,
        multiplier=2,
        deadline=120,
    )

    logger.info("Streaming rows to BigQuery | table_id=%s rows=%s", table_id, len(rows))
    logger.debug(
        "Insert request details | table_id=%s insert_ids_preview=%s",
        table_id,
        insert_ids[:3],
    )

    errors = client.insert_rows_json(table, rows, row_ids=insert_ids, retry=retry_policy)
    if errors:
        logger.error("BigQuery returned row insertion errors | table_id=%s errors=%s", table_id, errors)
        raise RuntimeError(f"Error inserting rows into {table_id}: {errors}")

    logger.info("Rows inserted successfully | table_id=%s rows=%s", table_id, len(rows))


def split_category(categoria: str) -> Tuple[str, str]:
    text = categoria or ""
    idx = text.find(" >> ")
    if idx == -1:
        return text.strip(), ""
    return text[:idx].strip(), text[idx + 4 :].strip()


def build_product_lookup(produto_data: List[dict]) -> Dict[str, dict]:
    lookup: Dict[str, dict] = {}
    skipped_payloads = 0

    for payload in produto_data or []:
        try:
            produto = payload["retorno"]["produto"]
            lookup[str(produto["id"])] = produto
        except (KeyError, TypeError):
            skipped_payloads += 1

    logger.debug(
        "Built product lookup map | total_payloads=%s mapped_products=%s skipped_payloads=%s",
        len(produto_data or []),
        len(lookup),
        skipped_payloads,
    )
    return lookup


def calculate_item_discount(valor_com_desconto: Decimal, desconto_pct: Decimal) -> Decimal:
    denominator = Decimal("1") - desconto_pct / Decimal("100")
    if denominator <= 0:
        logger.debug(
            "Item discount denominator <= 0. Returning 0 discount to avoid division by zero | valor=%s desconto_pct=%s",
            valor_com_desconto,
            desconto_pct,
        )
        return Decimal("0")

    return valor_com_desconto / denominator - valor_com_desconto


def parse_message_payload(event: dict) -> dict:
    if "data" not in event:
        raise ValueError("Pub/Sub event is missing 'data'")

    raw_data = event["data"]
    if raw_data is None:
        raise ValueError("Pub/Sub event 'data' is None")

    if isinstance(raw_data, str):
        raw_data = raw_data.encode("utf-8")

    try:
        raw_message = base64.b64decode(raw_data).decode("utf-8")
    except Exception as exc:
        raise ValueError(f"Failed to decode Pub/Sub base64 data: {exc}") from exc

    payload = json.loads(raw_message)

    required_keys = [
        "uuid",
        "timestamp",
        "store_prefix",
        "pdv_pedido_data",
        "produto_data",
        "pedidos_pesquisa_data",
    ]

    for key in required_keys:
        if key not in payload:
            raise ValueError(f"Pub/Sub payload missing required key: {key}")

    logger.debug(
        "Validated Pub/Sub payload contract | keys_present=%s uuid=%s store_prefix=%s",
        sorted(payload.keys()),
        payload.get("uuid"),
        payload.get("store_prefix"),
    )
    return payload


def context_event_id(context: Any) -> str:
    if context is None:
        return "unknown"
    return str(getattr(context, "event_id", "unknown"))


def normalize_runtime_event(event_or_request: Any, context: Any) -> Tuple[dict, str, str]:
    """
    Normalize multiple runtime invocation shapes to a common background-event structure.

    Returns:
      - event dict with base64 data in event['data']
      - event_id for logs/correlation
      - runtime_mode string for diagnostics
    """
    if context is not None:
        return event_or_request, context_event_id(context), "background"

    # HTTP request invocation (common accidental deployment mode)
    if hasattr(event_or_request, "get_json"):
        body = event_or_request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            raise ValueError("HTTP request body must be a JSON object")

        message = body.get("message", {})
        if not isinstance(message, dict):
            raise ValueError("HTTP request body missing Pub/Sub 'message' object")

        if "data" not in message:
            raise ValueError("HTTP request Pub/Sub wrapper is missing message.data")

        event_id = str(
            message.get("messageId")
            or message.get("message_id")
            or body.get("subscription", "unknown")
        )
        logger.warning(
            "Function invoked in HTTP mode; adapting Pub/Sub push envelope automatically | event_id=%s",
            event_id,
        )
        return {"data": message.get("data")}, event_id, "http_pubsub_push"

    # CloudEvent-style invocation
    if hasattr(event_or_request, "data"):
        cloud_data = getattr(event_or_request, "data")
        if isinstance(cloud_data, dict):
            if "message" in cloud_data and isinstance(cloud_data["message"], dict):
                message = cloud_data["message"]
                event_id = str(
                    message.get("messageId")
                    or message.get("message_id")
                    or getattr(event_or_request, "id", "unknown")
                )
                logger.info("Function invoked in CloudEvent mode | event_id=%s", event_id)
                return {"data": message.get("data")}, event_id, "cloudevent_pubsub"

            if "data" in cloud_data:
                event_id = str(getattr(event_or_request, "id", "unknown"))
                logger.info("Function invoked in generic CloudEvent data mode | event_id=%s", event_id)
                return {"data": cloud_data.get("data")}, event_id, "cloudevent_generic"

    # Already a dict-like event without context
    if isinstance(event_or_request, dict):
        return event_or_request, "unknown", "dict_without_context"

    raise ValueError(
        "Unsupported function invocation shape. Expected background event/context, HTTP Pub/Sub push request, or CloudEvent."
    )


def process_pubsub_message(event: Any, context: Any = None) -> str:
    normalized_event, event_id, runtime_mode = normalize_runtime_event(event, context)
    logger.info(
        "Function start | handler=process_pubsub_message event_id=%s runtime_mode=%s",
        event_id,
        runtime_mode,
    )

    try:
        message_data = parse_message_payload(normalized_event)

        uuid_value = str(message_data["uuid"])
        timestamp = str(message_data["timestamp"])
        store_prefix = str(message_data["store_prefix"])

        logger.info(
            "Payload metadata extracted | event_id=%s uuid=%s timestamp=%s store_prefix=%s",
            event_id,
            uuid_value,
            timestamp,
            store_prefix,
        )

        pdv_pedido = message_data["pdv_pedido_data"]["retorno"]["pedido"]
        pedidos_pesquisa = message_data["pedidos_pesquisa_data"]["retorno"]["pedidos"][0]["pedido"]
        produtos_lookup = build_product_lookup(message_data["produto_data"])

        pedidos_table_id, itens_table_id = resolve_table_ids(store_prefix)

        pedido_dia = transform_date_format(pdv_pedido.get("data", ""))
        pedido_numero = str(pdv_pedido.get("numero", ""))
        pedido_id = str(pdv_pedido.get("id", ""))

        contato = pdv_pedido.get("contato", {})
        cliente_nome = contato.get("nome", "")
        cliente_cpf = contato.get("cpfCnpj", "")
        cliente_email = contato.get("email", "")
        cliente_celular = contato.get("celular", "")

        vendedor_nome = pedidos_pesquisa.get("nome_vendedor", "")
        vendedor_id = str(pedidos_pesquisa.get("id_vendedor", ""))

        forma_pagamento = pdv_pedido.get("formaPagamento", "")
        valor_faturado = decimal_value(pdv_pedido.get("totalVenda"))
        total_produtos_base = decimal_value(pdv_pedido.get("totalProdutos"))

        itens = pdv_pedido.get("itens", []) or []
        logger.info(
            "Order core fields extracted | event_id=%s pedido_id=%s pedido_numero=%s itens_count=%s forma_pagamento=%s",
            event_id,
            pedido_id,
            pedido_numero,
            len(itens),
            forma_pagamento,
        )

        total_produtos_custo = Decimal("0")
        total_produtos_sem_desconto = Decimal("0")
        total_desconto_produtos = Decimal("0")
        total_pre_discount_value = Decimal("0")

        for item in itens:
            produto = produtos_lookup.get(str(item.get("idProduto")))
            if produto:
                total_produtos_custo += decimal_value(produto.get("preco_custo")) * decimal_value(item.get("quantidade"))

            item_valor = decimal_value(item.get("valor"))
            desconto_pct = decimal_value(item.get("desconto"))
            denominator = Decimal("1") - desconto_pct / Decimal("100")
            item_sem_desconto = item_valor / denominator if denominator > 0 else item_valor
            total_produtos_sem_desconto += item_sem_desconto
            total_pre_discount_value += item_valor * decimal_value(item.get("quantidade"))

        desconto_pedido = parse_discount(pdv_pedido.get("desconto", "0"), total_produtos_base)
        total_discount = desconto_pedido
        processed_timestamp = datetime.now(timezone.utc).isoformat()

        logger.debug(
            "Order totals prepared before item allocation | event_id=%s total_produtos_custo=%s total_sem_desconto=%s total_pre_discount_value=%s desconto_pedido=%s",
            event_id,
            total_produtos_custo,
            total_produtos_sem_desconto,
            total_pre_discount_value,
            desconto_pedido,
        )

        itens_rows: List[Dict[str, Any]] = []
        itens_insert_ids: List[str] = []

        for index, item in enumerate(itens):
            produto_id = str(item.get("idProduto", ""))
            produto = produtos_lookup.get(produto_id)
            if not produto:
                logger.warning(
                    "Item skipped: product details not found | event_id=%s pedido_id=%s produto_id=%s item_index=%s",
                    event_id,
                    pedido_id,
                    produto_id,
                    index,
                )
                continue

            qtd = decimal_value(item.get("quantidade"))
            valor_item = decimal_value(item.get("valor"))
            desconto_pct = decimal_value(item.get("desconto"))

            denominator = Decimal("1") - desconto_pct / Decimal("100")
            valor_sem_desconto_und = valor_item / denominator if denominator > 0 else valor_item
            desconto_produto_und = calculate_item_discount(valor_item, desconto_pct)
            desconto_produto = desconto_produto_und * qtd

            item_pre_discount_value = valor_item * qtd
            desconto_pedido_item = Decimal("0")
            if total_pre_discount_value > 0:
                desconto_pedido_item = total_discount * (item_pre_discount_value / total_pre_discount_value)

            desconto_pedido_und = desconto_pedido_item / qtd if qtd > 0 else Decimal("0")
            desconto_total_und = desconto_produto_und + desconto_pedido_und
            desconto_total = desconto_produto + desconto_pedido_item

            valor_com_desconto_und = valor_sem_desconto_und - desconto_produto_und - desconto_pedido_und
            custo_und = decimal_value(produto.get("preco_custo"))
            valor_lucro_und = valor_com_desconto_und - custo_und

            total_valor_custo = custo_und * qtd
            total_valor_sem_desc = valor_sem_desconto_und * qtd
            total_valor_faturado = valor_com_desconto_und * qtd
            total_valor_lucro = total_valor_faturado - total_valor_custo

            cat1, cat2 = split_category(produto.get("categoria", ""))

            itens_rows.append(
                {
                    "uuid": uuid_value,
                    "timestamp": timestamp,
                    "pedido_dia": pedido_dia,
                    "pedido_id": pedido_id,
                    "pedido_numero": pedido_numero,
                    "cliente_nome": cliente_nome,
                    "cliente_cpf": cliente_cpf,
                    "cliente_email": cliente_email,
                    "cliente_celular": cliente_celular,
                    "vendedor_nome": vendedor_nome,
                    "vendedor_id": vendedor_id,
                    "produto_id": produto_id,
                    "produto_nome": item.get("descricao", ""),
                    "produto_categoria_principal": cat1,
                    "produto_categoria_secundaria": cat2,
                    "produto_valor_custo_und": float(custo_und),
                    "produto_valor_sem_desconto_und": float(valor_sem_desconto_und),
                    "produto_valor_com_desconto_und": float(valor_com_desconto_und),
                    "produto_valor_lucro_und": float(valor_lucro_und),
                    "desconto_produto_und": float(desconto_produto_und),
                    "desconto_pedido_und": float(desconto_pedido_und),
                    "desconto_total_und": float(desconto_total_und),
                    "produto_quantidade": float(qtd),
                    "desconto_produto": float(desconto_produto),
                    "desconto_pedido": float(desconto_pedido_item),
                    "desconto_total": float(desconto_total),
                    "total_produto_valor_custo": float(total_valor_custo),
                    "total_produto_valor_sem_desconto": float(total_valor_sem_desc),
                    "total_produto_valor_faturado": float(total_valor_faturado),
                    "total_produto_valor_lucro": float(total_valor_lucro),
                    "forma_pagamento": forma_pagamento,
                    "source_id": SOURCE_ID,
                    "processed_timestamp": processed_timestamp,
                }
            )

            total_desconto_produtos += desconto_produto
            itens_insert_ids.append(
                hashlib.md5(f"{uuid_value}|{pedido_id}|{produto_id}|{index}".encode("utf-8")).hexdigest()
            )

            logger.debug(
                "Item transformed | event_id=%s pedido_id=%s produto_id=%s item_index=%s qtd=%s total_valor_faturado=%s total_valor_lucro=%s",
                event_id,
                pedido_id,
                produto_id,
                index,
                qtd,
                total_valor_faturado,
                total_valor_lucro,
            )

        desconto_total = total_desconto_produtos + desconto_pedido
        valor_lucro = valor_faturado - total_produtos_custo

        pedidos_row = {
            "uuid": uuid_value,
            "timestamp": timestamp,
            "pedido_dia": pedido_dia,
            "pedido_id": pedido_id,
            "pedido_numero": pedido_numero,
            "cliente_nome": cliente_nome,
            "cliente_cpf": cliente_cpf,
            "cliente_email": cliente_email,
            "cliente_celular": cliente_celular,
            "vendedor_nome": vendedor_nome,
            "vendedor_id": vendedor_id,
            "valor_produtos_custo": float(total_produtos_custo),
            "valor_produtos_sem_desconto": float(total_produtos_sem_desconto),
            "desconto_produtos": float(total_desconto_produtos),
            "desconto_pedido": float(desconto_pedido),
            "desconto_total": float(desconto_total),
            "valor_faturado": float(valor_faturado),
            "valor_lucro": float(valor_lucro),
            "forma_pagamento": forma_pagamento,
            "source_id": SOURCE_ID,
            "processed_timestamp": processed_timestamp,
        }

        logger.info(
            "Rows ready for persistence | event_id=%s pedido_id=%s pedidos_rows=1 itens_rows=%s",
            event_id,
            pedido_id,
            len(itens_rows),
        )

        create_table_if_not_exists(
            pedidos_table_id,
            pedidos_schema,
            "pedido_dia",
            ["pedido_id", "cliente_cpf", "vendedor_id", "forma_pagamento"],
        )
        create_table_if_not_exists(
            itens_table_id,
            itens_pedido_schema,
            "pedido_dia",
            ["pedido_id", "produto_id", "cliente_cpf", "vendedor_id"],
        )

        pedidos_insert_id = hashlib.md5(f"{uuid_value}|{pedido_id}".encode("utf-8")).hexdigest()
        insert_rows_json(pedidos_table_id, [pedidos_row], [pedidos_insert_id])

        if itens_rows:
            insert_rows_json(itens_table_id, itens_rows, itens_insert_ids)

        logger.info(
            "Function success | event_id=%s pedido_id=%s store_prefix=%s dataset_mode=%s items_written=%s",
            event_id,
            pedido_id,
            store_prefix,
            "multi-store" if DATASET_BASE_ID else "legacy",
            len(itens_rows),
        )
        return "OK"

    except Exception:
        logger.exception("Function failed | event_id=%s runtime_mode=%s", event_id, runtime_mode)
        raise
