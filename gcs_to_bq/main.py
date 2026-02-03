import base64
import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Dict, List

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud.exceptions import NotFound
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

DATASET_ID = os.getenv("DATASET_ID")
SOURCE = os.getenv("SOURCE")
VERSION = os.getenv("VERSION")
PROJECT_ID = os.getenv("PROJECT_ID")
TOPIC_ID = os.getenv("TOPIC_ID")
NOTIFY = os.getenv("NOTIFY", "False").lower() == "true"

logging.basicConfig(level=logging.DEBUG)

PDV_SCHEMA = [
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("numero", "INTEGER"),
    bigquery.SchemaField("data", "DATE"),
    bigquery.SchemaField("frete", "FLOAT"),
    bigquery.SchemaField("desconto", "STRING"),
    bigquery.SchemaField("valorICMSSubst", "FLOAT"),
    bigquery.SchemaField("valorIPI", "FLOAT"),
    bigquery.SchemaField("totalProdutos", "FLOAT"),
    bigquery.SchemaField("totalVenda", "FLOAT"),
    bigquery.SchemaField("fretePorConta", "STRING"),
    bigquery.SchemaField("pesoLiquido", "FLOAT"),
    bigquery.SchemaField("pesoBruto", "FLOAT"),
    bigquery.SchemaField("observacoes", "STRING"),
    bigquery.SchemaField("formaPagamento", "STRING"),
    bigquery.SchemaField("situacao", "STRING"),
    bigquery.SchemaField(
        "contato",
        "RECORD",
        fields=[
            bigquery.SchemaField("nome", "STRING"),
            bigquery.SchemaField("fantasia", "STRING"),
            bigquery.SchemaField("codigo", "STRING"),
            bigquery.SchemaField("tipo", "STRING"),
            bigquery.SchemaField("cpfCnpj", "STRING"),
            bigquery.SchemaField("endereco", "STRING"),
            bigquery.SchemaField("enderecoNro", "STRING"),
            bigquery.SchemaField("complemento", "STRING"),
            bigquery.SchemaField("bairro", "STRING"),
            bigquery.SchemaField("cidade", "STRING"),
            bigquery.SchemaField("uf", "STRING"),
            bigquery.SchemaField("cep", "STRING"),
            bigquery.SchemaField("fone", "STRING"),
            bigquery.SchemaField("celular", "STRING"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("inscricaoEstadual", "STRING"),
            bigquery.SchemaField("indIEDest", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "enderecoEntrega",
        "RECORD",
        fields=[
            bigquery.SchemaField("nome", "STRING"),
            bigquery.SchemaField("tipo", "STRING"),
            bigquery.SchemaField("cpfCnpj", "STRING"),
            bigquery.SchemaField("endereco", "STRING"),
            bigquery.SchemaField("enderecoNro", "STRING"),
            bigquery.SchemaField("complemento", "STRING"),
            bigquery.SchemaField("bairro", "STRING"),
            bigquery.SchemaField("cidade", "STRING"),
            bigquery.SchemaField("uf", "STRING"),
            bigquery.SchemaField("cep", "STRING"),
            bigquery.SchemaField("fone", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "itens",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("idProduto", "INTEGER"),
            bigquery.SchemaField("descricao", "STRING"),
            bigquery.SchemaField("codigo", "STRING"),
            bigquery.SchemaField("valor", "FLOAT"),
            bigquery.SchemaField("quantidade", "FLOAT"),
            bigquery.SchemaField("desconto", "STRING"),
            bigquery.SchemaField("pesoLiquido", "FLOAT"),
            bigquery.SchemaField("pesoBruto", "FLOAT"),
            bigquery.SchemaField("unidade", "STRING"),
            bigquery.SchemaField("tipo", "STRING"),
            bigquery.SchemaField("ncm", "STRING"),
            bigquery.SchemaField("origem", "STRING"),
            bigquery.SchemaField("cest", "STRING"),
            bigquery.SchemaField("gtin", "STRING"),
            bigquery.SchemaField("gtinTributavel", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "parcelas",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("formaPagamento", "STRING"),
            bigquery.SchemaField("dataVencimento", "DATE"),
            bigquery.SchemaField("valor", "FLOAT"),
            bigquery.SchemaField("tPag", "STRING"),
        ],
    ),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("update_timestamp", "TIMESTAMP"),
]

PESQUISA_SCHEMA = [
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("numero", "STRING"),
    bigquery.SchemaField("numero_ecommerce", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("data_pedido", "DATE"),
    bigquery.SchemaField("data_prevista", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("nome", "STRING"),
    bigquery.SchemaField("valor", "FLOAT"),
    bigquery.SchemaField("id_vendedor", "STRING"),
    bigquery.SchemaField("nome_vendedor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("situacao", "STRING"),
    bigquery.SchemaField("codigo_rastreamento", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("url_rastreamento", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("update_timestamp", "TIMESTAMP"),
]

PRODUTO_SCHEMA = [
    bigquery.SchemaField("uuid", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
    bigquery.SchemaField("id", "INTEGER"),
    bigquery.SchemaField("nome", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("codigo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unidade", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("preco", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("preco_promocional", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ncm", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("origem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gtin", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gtin_embalagem", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("localizacao", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("peso_liquido", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("peso_bruto", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("estoque_minimo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("estoque_maximo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("id_fornecedor", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("nome_fornecedor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("codigo_fornecedor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("codigo_pelo_fornecedor", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("unidade_por_caixa", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("preco_custo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("preco_custo_medio", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("situacao", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipo", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("classe_ipi", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("valor_ipi_fixo", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("cod_lista_servicos", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("descricao_complementar", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("garantia", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("cest", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("obs", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipoVariacao", "STRING"),
    bigquery.SchemaField("variacoes", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("idProdutoPai", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("sob_encomenda", "STRING"),
    bigquery.SchemaField("dias_preparacao", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("marca", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("tipoEmbalagem", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("alturaEmbalagem", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("larguraEmbalagem", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("comprimentoEmbalagem", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("diametroEmbalagem", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("qtd_volumes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("categoria", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "anexos",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("anexo", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "imagens_externas",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("url", "STRING"),
        ],
    ),
    bigquery.SchemaField("classe_produto", "STRING"),
    bigquery.SchemaField("seo_title", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("seo_keywords", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("link_video", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("seo_description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("slug", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("source_id", "STRING"),
    bigquery.SchemaField("update_timestamp", "TIMESTAMP"),
]

class MissingConfigError(RuntimeError):
    pass


def ensure_dataset_exists(client: bigquery.Client, dataset_id: str) -> None:
    logging.debug("Checking if dataset %s exists", dataset_id)
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logging.debug("Dataset %s already exists.", dataset_id)
    except NotFound:
        logging.info("Dataset %s does not exist. Creating dataset.", dataset_id)
        client.create_dataset(bigquery.Dataset(dataset_ref))
        logging.info("Dataset %s created successfully.", dataset_id)


def ensure_table_exists(
    client: bigquery.Client, dataset_id: str, table_id: str, schema: List[bigquery.SchemaField]
) -> None:
    logging.debug("Checking if table %s exists", table_id)
    dataset_ref = client.dataset(dataset_id, project=PROJECT_ID)
    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
        logging.debug("Table %s already exists.", table_id)
    except NotFound:
        logging.info(
            "Table %s does not exist. Creating table with day-partitioning on 'timestamp'.",
            table_id,
        )
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="timestamp")
        client.create_table(table)
        logging.info("Table %s created successfully.", table_id)


def log_bigquery_reference(client: bigquery.Client, dataset_id: str, table_id: str) -> None:
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"
    logging.info("BigQuery table reference: %s", full_table_id)


def transform_date_format(date_str: str) -> str:
    logging.debug("Transforming date format for: %s", date_str)
    try:
        transformed_date = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        logging.debug("Transformed date: %s", transformed_date)
        return transformed_date
    except ValueError as exc:
        logging.warning("Error transforming date format: %s", exc)
        return date_str


def normalize_store_prefix(store_prefix: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", store_prefix.strip().lower()).strip("_")


def resolve_dataset_id(store_prefix: str) -> str:
    if not DATASET_ID:
        raise MissingConfigError("DATASET_ID is not set")
    normalized_prefix = normalize_store_prefix(store_prefix)
    if not normalized_prefix:
        raise MissingConfigError("store_prefix is empty after normalization")
    return f"{normalized_prefix}_{DATASET_ID}"


def resolve_table_id(store_prefix: str, table_base: str) -> str:
    normalized_prefix = normalize_store_prefix(store_prefix)
    return f"{normalized_prefix}__{table_base}"


@retry(
    retry=retry_if_exception_type(exceptions.DeadlineExceeded),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    stop=stop_after_attempt(3),
)
def publish_to_pubsub(uuid: str) -> None:
    if not NOTIFY:
        logging.info(
            "Notification disabled. Skipping publishing message to %s with UUID: %s",
            TOPIC_ID,
            uuid,
        )
        return
    if not TOPIC_ID:
        raise MissingConfigError("TOPIC_ID is not set")
    try:
        logging.info("Publishing message to %s with UUID: %s", TOPIC_ID, uuid)
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
        message_data = json.dumps({"uuid": uuid}).encode("utf-8")
        future = publisher.publish(topic_path, message_data)
        future.result(timeout=30)
        logging.info("Published message to %s with UUID: %s", TOPIC_ID, uuid)
    except exceptions.DeadlineExceeded:
        logging.error("Timeout occurred while publishing message to %s with UUID: %s", TOPIC_ID, uuid)
        raise
    except Exception as exc:
        logging.error(
            "An unexpected error occurred while publishing message to %s with UUID: %s: %s",
            TOPIC_ID,
            uuid,
            exc,
        )


@retry(
    retry=retry_if_exception_type(exceptions.ServerError),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    stop=stop_after_attempt(3),
)
def insert_rows_with_retry(
    client: bigquery.Client, table_ref: bigquery.TableReference, rows: List[Dict[str, Any]]
) -> None:
    logging.info("Inserting %s rows into %s", len(rows), table_ref.table_id)
    try:
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logging.error("Errors streaming data to BigQuery: %s", errors)
        else:
            logging.info("Data streamed successfully to %s.", table_ref.table_id)
    except exceptions.ServerError as exc:
        logging.error("Server error occurred while inserting rows to %s: %s", table_ref.table_id, exc)
        raise


def transform_and_load_pdv_data(
    client: bigquery.Client, dataset_id: str, pdv_data: dict, uuid: str, timestamp: str, store_prefix: str
) -> None:
    logging.info("Transforming and loading PDV data.")
    table_id = resolve_table_id(store_prefix, "pdv")
    ensure_table_exists(client, dataset_id, table_id, PDV_SCHEMA)

    pedido_data = pdv_data["retorno"]["pedido"]

    if "data" in pedido_data:
        pedido_data["data"] = transform_date_format(pedido_data["data"])

    if "parcelas" in pedido_data:
        for parcela in pedido_data["parcelas"]:
            if "dataVencimento" in parcela:
                parcela["dataVencimento"] = transform_date_format(parcela["dataVencimento"])

    pedido_data.update(
        {
            "uuid": uuid,
            "timestamp": datetime.strptime(timestamp, "%Y%m%dT%H%M%S").isoformat(),
            "source_id": f"{SOURCE}-pdv_{VERSION}",
            "update_timestamp": datetime.utcnow().isoformat(),
        }
    )

    log_bigquery_reference(client, dataset_id, table_id)

    table_ref = client.dataset(dataset_id).table(table_id)
    insert_rows_with_retry(client, table_ref, [pedido_data])

    if NOTIFY:
        publish_to_pubsub(uuid)

    logging.info("PDV data transformation and loading completed.")


def transform_and_load_pesquisa_data(
    client: bigquery.Client,
    dataset_id: str,
    pesquisa_data: dict,
    uuid: str,
    timestamp: str,
    store_prefix: str,
) -> None:
    logging.info("Transforming and loading Pesquisa data.")
    table_id = resolve_table_id(store_prefix, "pesquisa")
    ensure_table_exists(client, dataset_id, table_id, PESQUISA_SCHEMA)

    for pedido in pesquisa_data["retorno"]["pedidos"]:
        pedido_data = pedido["pedido"]

        pedido_data["data_pedido"] = transform_date_format(pedido_data.get("data_pedido", ""))

        data_prevista = pedido_data.get("data_prevista", "")
        if data_prevista:
            pedido_data["data_prevista"] = transform_date_format(data_prevista)
        else:
            pedido_data.pop("data_prevista", None)

        pedido_data.update(
            {
                "uuid": uuid,
                "timestamp": datetime.strptime(timestamp, "%Y%m%dT%H%M%S").isoformat(),
                "source_id": f"{SOURCE}-pesquisa_{VERSION}",
                "update_timestamp": datetime.utcnow().isoformat(),
            }
        )

        log_bigquery_reference(client, dataset_id, table_id)

        table_ref = client.dataset(dataset_id).table(table_id)

        insert_rows_with_retry(client, table_ref, [pedido_data])

        if NOTIFY:
            publish_to_pubsub(uuid)

    logging.info("Pesquisa data transformation and loading completed.")


def transform_and_load_produto_data(
    client: bigquery.Client,
    dataset_id: str,
    produto_data: dict,
    uuid: str,
    timestamp: str,
    store_prefix: str,
) -> None:
    logging.info("Transforming and loading Produto data.")
    table_id = resolve_table_id(store_prefix, "produto")
    ensure_table_exists(client, dataset_id, table_id, PRODUTO_SCHEMA)

    if not produto_data:
        logging.debug("Received empty produto data.")
        return

    produto_data.update(
        {
            "uuid": uuid,
            "timestamp": datetime.strptime(timestamp, "%Y%m%dT%H%M%S").isoformat(),
            "source_id": f"{SOURCE}-produto_{VERSION}",
            "update_timestamp": datetime.utcnow().isoformat(),
        }
    )

    log_bigquery_reference(client, dataset_id, table_id)

    table_ref = client.dataset(dataset_id).table(table_id)

    insert_rows_with_retry(client, table_ref, [produto_data])

    if NOTIFY:
        publish_to_pubsub(uuid)

    logging.info("Produto data transformation and loading completed.")


def cloud_function_entry_point(event: dict, context: Any) -> None:
    logging.info("Cloud Function triggered by Pub/Sub message: %s", event)
    if not PROJECT_ID:
        raise MissingConfigError("PROJECT_ID is not set")

    client = bigquery.Client()
    message_data = base64.b64decode(event["data"]).decode("utf-8")
    message_json = json.loads(message_data)
    uuid = message_json.get("uuid")
    timestamp = message_json.get("timestamp")
    store_prefix = message_json.get("store_prefix")

    if not uuid or not timestamp:
        logging.error("UUID or Timestamp missing in Pub/Sub message.")
        return

    if not store_prefix:
        logging.error("store_prefix missing in Pub/Sub message.")
        return

    dataset_id = resolve_dataset_id(store_prefix)
    ensure_dataset_exists(client, dataset_id)

    if "pdv_pedido_data" in message_json:
        pdv_pedido_data = message_json["pdv_pedido_data"]
        transform_and_load_pdv_data(client, dataset_id, pdv_pedido_data, uuid, timestamp, store_prefix)

    if "produto_data" in message_json:
        produto_data_list = message_json["produto_data"]
        for produto_data in produto_data_list:
            if "retorno" in produto_data and "produto" in produto_data["retorno"]:
                transform_and_load_produto_data(
                    client,
                    dataset_id,
                    produto_data["retorno"]["produto"],
                    uuid,
                    timestamp,
                    store_prefix,
                )

    if "pedidos_pesquisa_data" in message_json:
        pedidos_pesquisa_data = message_json["pedidos_pesquisa_data"]
        transform_and_load_pesquisa_data(
            client, dataset_id, pedidos_pesquisa_data, uuid, timestamp, store_prefix
        )

    logging.info("Processing completed for Pub/Sub message.")
