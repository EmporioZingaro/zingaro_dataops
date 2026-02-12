import base64
import hashlib
import json
import logging
import math
import os
import re
from pprint import pformat
from datetime import datetime
from typing import Any, List, Optional, Tuple
from zoneinfo import ZoneInfo

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, pubsub_v1

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
DEBUG_MODE = os.environ.get("DEBUG_MODE", "false").lower() == "true"

resolved_log_level = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(level=resolved_log_level)
logging.getLogger().setLevel(resolved_log_level)
logger = logging.getLogger(__name__)
logger.setLevel(resolved_log_level)

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_ID_SALES = os.environ.get("TABLE_ID_SALES")
TABLE_ID_SALES_ITEMS = os.environ.get("TABLE_ID_SALES_ITEMS")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")
SOURCE_IDENTIFIER = os.environ.get("SOURCE_IDENTIFIER")
VERSION_CONTROL = os.environ.get("VERSION_CONTROL")
FIDELIDADE_MULTIPLIER = float(os.environ.get("FIDELIDADE_MULTIPLIER", 0.0))
ENABLE_DETERMINISTIC_ROW_IDS = os.environ.get("ENABLE_DETERMINISTIC_ROW_IDS", "false").lower() == "true"

bigquery_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()


sales_schema = [
    bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("store_prefix", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pedido_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pedido_dia", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("pedido_numero", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_cpf", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_email", "STRING"),
    bigquery.SchemaField("vendedor_nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("vendedor_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pedido_valor", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("pedido_pontos", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_identifier", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("version_control", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("processed_timestamp", "TIMESTAMP", mode="REQUIRED"),
]

sales_items_schema = [
    bigquery.SchemaField("uuid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("store_prefix", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("pedido_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_idProduto", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pedido_dia", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("pedido_numero", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_cpf", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cliente_email", "STRING"),
    bigquery.SchemaField("vendedor_nome", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("vendedor_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_descricao", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_category_first", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_category_second", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("produto_quantidade", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("produto_valor", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("produto_valor_total", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("produto_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("fidelidade_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("special_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("final_multiplier", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("produto_pontos_total", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_identifier", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("version_control", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("processed_timestamp", "TIMESTAMP", mode="REQUIRED"),
]


def convert_to_sao_paulo_time(utc_timestamp: datetime) -> datetime:
    logger.debug("Converting UTC timestamp to São Paulo time.")
    sao_paulo_timezone = ZoneInfo("America/Sao_Paulo")
    return utc_timestamp.astimezone(sao_paulo_timezone)

def log_debug_payload(label: str, payload: Any) -> None:
    if not (DEBUG_MODE or logger.isEnabledFor(logging.DEBUG)):
        return

    if isinstance(payload, (dict, list)):
        rendered_payload = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    else:
        rendered_payload = pformat(payload, width=120)

    if DEBUG_MODE:
        logger.info("[DEBUG_PAYLOAD] %s\n%s", label, rendered_payload)
    else:
        logger.debug("%s\n%s", label, rendered_payload)


def evaluate_pdv_eligibility(
    desconto: str, forma_pagamento: str, cliente_nome: str, cliente_cpf: str
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []

    if cliente_nome == "Consumidor Final":
        reasons.append("cliente_nome is 'Consumidor Final'")

    if desconto not in ["0", "0,00"]:
        reasons.append(f"desconto='{desconto}' is not in ['0', '0,00']")

    if forma_pagamento not in ["credito", "debito", "pix", "dinheiro"]:
        reasons.append(
            f"forma_pagamento='{forma_pagamento}' is not one of ['credito', 'debito', 'pix', 'dinheiro']"
        )

    if not re.match(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$", str(cliente_cpf)):
        reasons.append(
            "cliente_cpf is not in expected format '000.000.000-00'"
        )

    return len(reasons) == 0, reasons


def extract_pesquisa_data(pedidos_pesquisa_data: dict) -> Tuple[Optional[str], Optional[str]]:
    try:
        pedido = pedidos_pesquisa_data["retorno"]["pedidos"][0]["pedido"]
        nome_vendedor = pedido["nome_vendedor"]
        id_vendedor = pedido["id_vendedor"]
        return nome_vendedor, id_vendedor
    except (KeyError, IndexError) as exception:
        logger.error("Error extracting pedidos pesquisa data: %s", exception)
        logger.error("Pedidos pesquisa data: %s", pedidos_pesquisa_data)
        return None, None


def extract_pdv_data(
    pdv_pedido_data: dict,
) -> Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[float],
    Optional[float],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    logger.info("Extracting PDV pedido data")
    try:
        pedido_pdv = pdv_pedido_data["retorno"]["pedido"]
        pedido_id = str(pedido_pdv["id"])
        pedido_numero = str(pedido_pdv["numero"])
        pedido_dia = datetime.strptime(pedido_pdv["data"], "%d/%m/%Y").strftime(
            "%Y-%m-%d"
        )
        total_produtos = float(pedido_pdv["totalProdutos"])
        total_venda = float(pedido_pdv["totalVenda"])
        observacoes = pedido_pdv.get("observacoes", "")
        forma_pagamento = pedido_pdv["formaPagamento"]
        cliente_nome = pedido_pdv["contato"]["nome"]
        cliente_cpf = pedido_pdv["contato"]["cpfCnpj"]
        cliente_email = pedido_pdv["contato"].get("email")
        desconto = pedido_pdv.get("desconto", "0")
        logger.info(
            "Extracted PDV pedido data - Pedido ID: %s, Pedido Numero: %s, Pedido Dia: %s",
            pedido_id,
            pedido_numero,
            pedido_dia,
        )
        return (
            pedido_id,
            pedido_numero,
            pedido_dia,
            total_produtos,
            total_venda,
            observacoes,
            forma_pagamento,
            cliente_nome,
            cliente_cpf,
            desconto,
            cliente_email,
        )
    except (KeyError, ValueError) as exception:
        logger.error("Error extracting PDV pedido data: %s", exception)
        logger.error("PDV pedido data: %s", pdv_pedido_data)
        return (None, None, None, None, None, None, None, None, None, None, None)


def validate_pdv_data(
    desconto: str, forma_pagamento: str, cliente_nome: str, cliente_cpf: str
) -> bool:
    logger.info("Checking sale eligibility for Loyalty Program")
    valid, reasons = evaluate_pdv_eligibility(
        desconto, forma_pagamento, cliente_nome, cliente_cpf
    )

    if valid:
        logger.info(
            "Loyalty Program eligibility check result: eligible | forma_pagamento=%s desconto=%s cliente_nome=%s cliente_cpf=%s",
            forma_pagamento,
            desconto,
            cliente_nome,
            cliente_cpf,
        )
        return True

    logger.warning("Loyalty Program eligibility check result: NOT eligible")
    for reason in reasons:
        logger.warning("Eligibility rejection reason: %s", reason)

    return False


def process_pedido_item(
    item: dict,
    produto_data: dict,
    sao_paulo_timestamp: datetime,
    pedido_dia: str,
    uuid: str,
    nome_vendedor: str,
    id_vendedor: str,
    cliente_nome: str,
    cliente_cpf: str,
    cliente_email: Optional[str],
    store_prefix: str,
) -> Tuple[Optional[dict], float, int]:
    logger.info("Processing pedido item: %s", item)
    produto = next(
        (
            prod["retorno"]["produto"]
            for prod in produto_data
            if str(prod["retorno"]["produto"]["id"]) == str(item["idProduto"])
        ),
        None,
    )

    if not produto:
        logger.warning("Produto not found for Produto ID: %s", item["idProduto"])
        return None, 0, 0

    produto_quantidade = float(item.get("quantidade", 0))
    produto_valor = float(item.get("valor", 0))
    produto_valor_total = produto_quantidade * produto_valor
    produto_multiplier = 0.0
    special_multiplier = 0.0
    final_multiplier = FIDELIDADE_MULTIPLIER
    produto_pontos_total = math.ceil(produto_valor_total * final_multiplier)

    produto_id = str(produto["id"])
    produto_descricao = produto.get("nome", "")
    produto_categoria = produto.get("categoria", "")
    categoria_split = produto_categoria.split(" >> ")
    produto_category_first = categoria_split[0] if len(categoria_split) > 0 else ""
    produto_category_second = categoria_split[1] if len(categoria_split) > 1 else ""

    sales_items_row = {
        "uuid": uuid,
        "store_prefix": store_prefix,
        "timestamp": sao_paulo_timestamp.isoformat(),
        "pedido_dia": pedido_dia,
        "pedido_id": item["pedido_id"],
        "pedido_numero": item["pedido_numero"],
        "cliente_nome": cliente_nome,
        "cliente_cpf": cliente_cpf,
        "cliente_email": cliente_email,
        "vendedor_nome": nome_vendedor,
        "vendedor_id": id_vendedor,
        "produto_idProduto": produto_id,
        "produto_descricao": produto_descricao,
        "produto_category_first": produto_category_first,
        "produto_category_second": produto_category_second,
        "produto_quantidade": produto_quantidade,
        "produto_valor": produto_valor,
        "produto_valor_total": produto_valor_total,
        "produto_multiplier": produto_multiplier,
        "fidelidade_multiplier": FIDELIDADE_MULTIPLIER,
        "special_multiplier": special_multiplier,
        "final_multiplier": final_multiplier,
        "produto_pontos_total": produto_pontos_total,
        "project_id": PROJECT_ID,
        "source_identifier": SOURCE_IDENTIFIER,
        "version_control": VERSION_CONTROL,
        "processed_timestamp": datetime.now(tz=ZoneInfo("America/Sao_Paulo")).isoformat(),
    }

    return sales_items_row, produto_valor_total, produto_pontos_total


def process_pedido_items(
    pdv_pedido_data: dict,
    produto_data: dict,
    sao_paulo_timestamp: datetime,
    pedido_dia: str,
    uuid: str,
    nome_vendedor: str,
    id_vendedor: str,
    cliente_nome: str,
    cliente_cpf: str,
    cliente_email: Optional[str],
    store_prefix: str,
) -> Tuple[float, int, List[dict]]:
    logger.info(
        "Processing pedido items for pedido ID: %s",
        pdv_pedido_data["retorno"]["pedido"]["id"],
    )
    sales_items_rows: List[dict] = []
    pedido_valor = 0
    pedido_pontos = 0

    for item in pdv_pedido_data["retorno"]["pedido"]["itens"]:
        item["pedido_id"] = str(pdv_pedido_data["retorno"]["pedido"]["id"])
        item["pedido_numero"] = str(pdv_pedido_data["retorno"]["pedido"]["numero"])

        sales_items_row, produto_valor_total, produto_pontos_total = process_pedido_item(
            item,
            produto_data,
            sao_paulo_timestamp,
            pedido_dia,
            uuid,
            nome_vendedor,
            id_vendedor,
            cliente_nome,
            cliente_cpf,
            cliente_email,
            store_prefix,
        )

        if sales_items_row:
            sales_items_rows.append(sales_items_row)
            pedido_valor += produto_valor_total
            pedido_pontos += produto_pontos_total

    logger.info(
        "Processed pedido items - Pedido Valor: %s, Pedido Pontos: %s",
        pedido_valor,
        pedido_pontos,
    )
    return pedido_valor, pedido_pontos, sales_items_rows


def prepare_sales_row(
    sao_paulo_timestamp: datetime,
    pedido_dia: str,
    pdv_pedido_data: dict,
    nome_vendedor: str,
    id_vendedor: str,
    cliente_nome: str,
    cliente_cpf: str,
    cliente_email: Optional[str],
    pedido_valor: float,
    pedido_pontos: float,
    uuid: str,
    store_prefix: str,
) -> dict:
    logger.debug("Preparing sales row with the given data.")
    sales_row = {
        "uuid": uuid,
        "store_prefix": store_prefix,
        "timestamp": sao_paulo_timestamp.isoformat(),
        "pedido_dia": pedido_dia,
        "pedido_id": str(pdv_pedido_data["retorno"]["pedido"]["id"]),
        "pedido_numero": str(pdv_pedido_data["retorno"]["pedido"]["numero"]),
        "cliente_nome": cliente_nome,
        "cliente_cpf": cliente_cpf,
        "cliente_email": cliente_email,
        "vendedor_nome": nome_vendedor,
        "vendedor_id": id_vendedor,
        "pedido_valor": pedido_valor,
        "pedido_pontos": pedido_pontos,
        "project_id": PROJECT_ID,
        "source_identifier": SOURCE_IDENTIFIER,
        "version_control": VERSION_CONTROL,
        "processed_timestamp": datetime.now(tz=ZoneInfo("America/Sao_Paulo")).isoformat(),
    }
    return sales_row


def create_dataset_if_not_exists(dataset_id: str) -> None:
    logger.debug("Checking if dataset exists: %s", dataset_id)
    try:
        bigquery_client.get_dataset(dataset_id)
        logger.info("Dataset '%s' already exists.", dataset_id)
    except NotFound:
        logger.info("Dataset '%s' not found, creating it.", dataset_id)
        dataset = bigquery.Dataset(dataset_id)
        bigquery_client.create_dataset(dataset)
        logger.info("Created dataset '%s'.", dataset_id)


def create_table_if_not_exists(
    table_id: str, schema: List[bigquery.SchemaField], cluster_fields: List[str]
) -> None:
    logger.debug("Checking if table exists: %s", table_id)
    try:
        bigquery_client.get_table(table_id)
        logger.info("Table '%s' already exists.", table_id)
    except NotFound:
        logger.info("Table '%s' not found, creating it.", table_id)
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="pedido_dia"
        )
        table.clustering_fields = cluster_fields

        bigquery_client.create_table(table)
        logger.info("Created table '%s'.", table_id)




def _build_row_id(*parts: Any) -> str:
    normalized = "|".join(str(part) for part in parts)
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def load_data_to_bigquery(
    sales_row: dict, sales_items_rows: List[dict], uuid: str
) -> None:
    logger.info("Loading data to BigQuery for UUID: %s", uuid)
    try:
        dataset_id = f"{PROJECT_ID}.{DATASET_ID}"
        sales_table_id = f"{dataset_id}.{TABLE_ID_SALES}"
        sales_items_table_id = f"{dataset_id}.{TABLE_ID_SALES_ITEMS}"
        create_dataset_if_not_exists(dataset_id)
        create_table_if_not_exists(
            sales_table_id,
            sales_schema,
            ["store_prefix", "cliente_cpf", "vendedor_id"],
        )
        create_table_if_not_exists(
            sales_items_table_id,
            sales_items_schema,
            ["store_prefix", "cliente_cpf", "vendedor_id", "produto_idProduto"],
        )
        if sales_row and sales_items_rows:
            sales_row_ids = None
            if ENABLE_DETERMINISTIC_ROW_IDS:
                sales_row_ids = [
                    _build_row_id(
                        sales_row.get("uuid", ""),
                        sales_row.get("pedido_id", ""),
                        "sales",
                    )
                ]
            errors = bigquery_client.insert_rows_json(
                sales_table_id,
                [sales_row],
                row_ids=sales_row_ids,
            )
            if errors:
                logger.error("Errors while inserting sales data: %s", errors)
            else:
                logger.info("Sales data inserted successfully for UUID: %s", uuid)
            sales_items_row_ids = None
            if ENABLE_DETERMINISTIC_ROW_IDS:
                sales_items_row_ids = [
                    _build_row_id(
                        row.get("uuid", ""),
                        row.get("pedido_id", ""),
                        row.get("produto_idProduto", ""),
                        index,
                        "sales_items",
                    )
                    for index, row in enumerate(sales_items_rows)
                ]
            errors = bigquery_client.insert_rows_json(
                sales_items_table_id,
                sales_items_rows,
                row_ids=sales_items_row_ids,
            )
            if errors:
                logger.error("Errors while inserting sales items data: %s", errors)
            else:
                logger.info("Sales items data inserted successfully for UUID: %s", uuid)
                logger.info("Metrics - UUID: %s, Rows Inserted: %s", uuid, len(sales_items_rows))
        else:
            logger.warning("No data to insert for UUID: %s", uuid)
    except Exception as exception:
        logger.exception("Error loading data to BigQuery: %s", exception)
        raise


def generate_message_payload(
    sales_row: dict,
    sales_items_rows: List[dict],
    nota_fiscal_link: str,
    store_prefix: str,
) -> dict:
    sales_data = {**sales_row}
    sales_data["timestamp"] = (
        sales_row["timestamp"].isoformat()
        if isinstance(sales_row["timestamp"], datetime)
        else sales_row["timestamp"]
    )
    sales_data["processed_timestamp"] = (
        sales_row["processed_timestamp"].isoformat()
        if isinstance(sales_row["processed_timestamp"], datetime)
        else sales_row["processed_timestamp"]
    )

    items_data = []
    for item in sales_items_rows:
        item_data = {**item}
        item_data["timestamp"] = (
            item["timestamp"].isoformat()
            if isinstance(item["timestamp"], datetime)
            else item["timestamp"]
        )
        item_data["processed_timestamp"] = (
            item["processed_timestamp"].isoformat()
            if isinstance(item["processed_timestamp"], datetime)
            else item["processed_timestamp"]
        )
        items_data.append(item_data)

    return {
        "store_prefix": store_prefix,
        "sales_data": sales_data,
        "items_data": items_data,
        "nota_fiscal_link": nota_fiscal_link,
    }


def publish_to_pubsub(message: dict) -> None:
    try:
        topic_path = publisher.topic_path(PROJECT_ID, PUBSUB_TOPIC)

        logger.info("Publishing message to %s", PUBSUB_TOPIC)

        serialized_message = json.dumps(message, ensure_ascii=False)
        payload = serialized_message.encode("utf-8")

        future = publisher.publish(topic_path, data=payload)
        future.result()

        logger.debug(
            "Message successfully published to %s, serialized payload: %s",
            PUBSUB_TOPIC,
            serialized_message,
        )

    except Exception as exception:
        logger.exception("Failed to publish message to Pub/Sub: %s", exception)


def process_message(
    pdv_pedido_data: dict,
    produto_data: dict,
    pedidos_pesquisa_data: dict,
    nota_fiscal_link: str,
    sao_paulo_timestamp: datetime,
    uuid: str,
    store_prefix: str,
) -> None:
    try:
        log_debug_payload("Raw pdv_pedido_data", pdv_pedido_data)
        log_debug_payload("Raw produto_data", produto_data)
        log_debug_payload("Raw pedidos_pesquisa_data", pedidos_pesquisa_data)

        nome_vendedor, id_vendedor = extract_pesquisa_data(pedidos_pesquisa_data)
        (
            pedido_id,
            pedido_numero,
            pedido_dia,
            _total_produtos,
            _total_venda,
            _observacoes,
            forma_pagamento,
            cliente_nome,
            cliente_cpf,
            desconto,
            cliente_email,
        ) = extract_pdv_data(pdv_pedido_data)

        if nome_vendedor and id_vendedor and pedido_id and pedido_numero and pedido_dia:
            if validate_pdv_data(desconto, forma_pagamento, cliente_nome, cliente_cpf):
                pedido_valor, pedido_pontos, sales_items_rows = process_pedido_items(
                    pdv_pedido_data,
                    produto_data,
                    sao_paulo_timestamp,
                    pedido_dia,
                    uuid,
                    nome_vendedor,
                    id_vendedor,
                    cliente_nome,
                    cliente_cpf,
                    cliente_email,
                    store_prefix,
                )
                sales_row = prepare_sales_row(
                    sao_paulo_timestamp,
                    pedido_dia,
                    pdv_pedido_data,
                    nome_vendedor,
                    id_vendedor,
                    cliente_nome,
                    cliente_cpf,
                    cliente_email,
                    pedido_valor,
                    pedido_pontos,
                    uuid,
                    store_prefix,
                )
                load_data_to_bigquery(sales_row, sales_items_rows, uuid)
                json_message = generate_message_payload(
                    sales_row, sales_items_rows, nota_fiscal_link, store_prefix
                )
                publish_to_pubsub(json_message)
                logger.info(
                    "Pedido processed successfully | pedido_id=%s pedido_numero=%s store_prefix=%s pedido_valor=%.2f pedido_pontos=%s items=%s",
                    pedido_id,
                    pedido_numero,
                    store_prefix,
                    pedido_valor,
                    pedido_pontos,
                    len(sales_items_rows),
                )
            else:
                logger.warning("Invalid PDV data for pedido ID: %s", pedido_id)
                logger.info(
                    "Rejected pedido snapshot | pedido_id=%s pedido_numero=%s forma_pagamento=%s desconto=%s cliente_nome=%s cliente_cpf=%s",
                    pedido_id,
                    pedido_numero,
                    forma_pagamento,
                    desconto,
                    cliente_nome,
                    cliente_cpf,
                )
        else:
            logger.warning("Incomplete data for pedido ID: %s", pedido_id)
            logger.info(
                "Missing required fields | nome_vendedor=%s id_vendedor=%s pedido_id=%s pedido_numero=%s pedido_dia=%s",
                nome_vendedor,
                id_vendedor,
                pedido_id,
                pedido_numero,
                pedido_dia,
            )
    except Exception as exception:
        logger.exception("Error processing message: %s", exception)
        logger.error("PDV pedido data: %s", pdv_pedido_data)
        logger.error("Produto data: %s", produto_data)
        logger.error("Pedidos pesquisa data: %s", pedidos_pesquisa_data)




def decode_pubsub_message_payload(message: Any) -> dict:
    if isinstance(message, dict):
        log_debug_payload("Raw Pub/Sub event envelope (dict)", message)
        encoded_data = message.get("data")
        if not encoded_data:
            raise ValueError("Missing Pub/Sub data field")
        logger.info("Pub/Sub payload source=dict encoded_data_length=%s", len(encoded_data))
        return json.loads(base64.b64decode(encoded_data).decode("utf-8"))

    if hasattr(message, "get_json"):
        envelope = message.get_json(silent=True) or {}
        log_debug_payload("Raw HTTP request envelope", envelope)
        pubsub_message = envelope.get("message", envelope)
        encoded_data = pubsub_message.get("data") if isinstance(pubsub_message, dict) else None
        if not encoded_data:
            raise ValueError("Missing Pub/Sub message.data field in HTTP request")
        logger.info("Pub/Sub payload source=http_request encoded_data_length=%s", len(encoded_data))
        return json.loads(base64.b64decode(encoded_data).decode("utf-8"))

    raise TypeError(f"Unsupported message type: {type(message)!r}")

def pubsub_callback(message: Any, context=None):
    event_id = getattr(context, "event_id", "unknown")
    logger.info("Received message | event_id=%s message_type=%s", event_id, type(message).__name__)
    is_http_request = hasattr(message, "get_json")
    logger.info("Runtime logging config | LOG_LEVEL=%s DEBUG_MODE=%s is_http_request=%s", LOG_LEVEL, DEBUG_MODE, is_http_request)
    try:
        message_data = decode_pubsub_message_payload(message)
        logger.info("Decoded message data.")
        log_debug_payload("Raw decoded Pub/Sub message payload", message_data)

        store_prefix = message_data.get("store_prefix")
        pdv_pedido_data = message_data.get("pdv_pedido_data")
        produto_data = message_data.get("produto_data")
        pedidos_pesquisa_data = message_data.get("pedidos_pesquisa_data")
        nota_fiscal_link_data = message_data.get("nota_fiscal_link_data")
        nota_fiscal_link = (
            nota_fiscal_link_data.get("retorno", {}).get("link_nfe", "")
            if nota_fiscal_link_data
            else ""
        )
        timestamp = message_data.get("timestamp")
        uuid = message_data.get("uuid")

        logger.info("Store Prefix: %s", store_prefix)
        logger.info("Timestamp: %s", timestamp)
        logger.info("UUID: %s", uuid)

        if not (store_prefix and timestamp and uuid):
            logger.warning("Missing required fields in the message payload.")
            return ("IGNORED", 200) if is_http_request else "IGNORED"


        utc_timestamp = datetime.strptime(timestamp, "%Y%m%dT%H%M%S")
        sao_paulo_timestamp = convert_to_sao_paulo_time(utc_timestamp)
        logger.debug("Timestamp converted to São Paulo time.")

        process_message(
            pdv_pedido_data,
            produto_data,
            pedidos_pesquisa_data,
            nota_fiscal_link,
            sao_paulo_timestamp,
            uuid,
            store_prefix,
        )
        return ("OK", 200) if is_http_request else "OK"
    except Exception as exception:
        logger.exception("Error processing Pub/Sub message: %s", exception)
        if is_http_request:
            return ("ERROR", 500)
        raise
