import base64
import json
import logging
import os
import time
from datetime import datetime, timedelta

from google.cloud import bigquery
from google.cloud import secretmanager
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Asm, Email, Mail

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
resolved_log_level = getattr(logging, LOG_LEVEL, logging.INFO)
logging.basicConfig(level=resolved_log_level)
logging.getLogger().setLevel(resolved_log_level)
logger = logging.getLogger(__name__)
logger.setLevel(resolved_log_level)

TIERS = ["Bronze", "Prata", "Ouro", "Platina", "Top10", "Top5", "Top3", "Top1"]
CASHBACK_PERCENTAGES = {
    "Top1": 20,
    "Top3": 15,
    "Top5": 10,
    "Top10": 7,
    "Platina": 5,
    "Ouro": 4,
    "Prata": 3,
    "Bronze": 2,
}

PROJECT_ID = os.environ.get("PROJECT_ID")
DATASET_ID = os.environ.get("DATASET_ID")
TABLE_PEDIDOS = os.environ.get("TABLE_PEDIDOS", "pedidos")
TABLE_CURRENT = os.environ.get("TABLE_CURRENT", "current")
TABLE_CASHBACK = os.environ.get("TABLE_CASHBACK", "cashback")

EMAIL_SENDER_NAME = os.environ.get("EMAIL_SENDER_NAME")
FROM_EMAIL = os.environ.get("FROM_EMAIL")
TEMPLATE_ID = os.environ.get("SENDGRID_TEMPLATE_ID")
SENDGRID_SECRET_PATH = os.environ.get("SENDGRID_SECRET_PATH")
TEST_MODE = os.environ.get("TEST_MODE", "True")
TEST_EMAIL = os.environ.get("TEST_EMAIL")
ASM_GROUP_ID = int(os.environ.get("ASM_GROUP_ID", "0"))
ASM_GROUPS_TO_DISPLAY = [
    int(g.strip())
    for g in os.environ.get("ASM_GROUPS_TO_DISPLAY", "").split(",")
    if g.strip()
]

STORE_DISPLAY_CONFIGS = {}
_raw_store_configs = os.environ.get("STORE_DISPLAY_CONFIGS", "")
if _raw_store_configs:
    try:
        STORE_DISPLAY_CONFIGS = json.loads(_raw_store_configs)
    except json.JSONDecodeError:
        logger.error("Failed to parse STORE_DISPLAY_CONFIGS environment variable")


def _fq_table(table_name):
    return f"`{PROJECT_ID}.{DATASET_ID}.{table_name}`"


bq_client = bigquery.Client()
secret_manager_client = secretmanager.SecretManagerServiceClient()

_sendgrid_api_key = None


def _get_sendgrid_api_key():
    global _sendgrid_api_key
    if _sendgrid_api_key is None:
        logger.debug("Retrieving SendGrid API key from Secret Manager")
        try:
            response = secret_manager_client.access_secret_version(
                name=SENDGRID_SECRET_PATH
            )
            _sendgrid_api_key = response.payload.data.decode("UTF-8")
            logger.debug("SendGrid API key retrieved successfully")
        except Exception as e:
            logger.error("Failed to retrieve SendGrid API key: %s", e)
            raise
    return _sendgrid_api_key


def get_current_trimester_dates():
    logger.debug("Calculating current trimester dates")
    current_date = datetime.now()
    current_trimester = (current_date.month - 1) // 3
    start_month = (current_trimester * 3) + 1
    end_month = start_month + 2

    beginning_of_trimester = datetime(current_date.year, start_month, 1)

    if end_month == 12:
        end_of_trimester = datetime(current_date.year, 12, 31)
    else:
        end_of_trimester = datetime(current_date.year, end_month + 1, 1) - timedelta(
            days=1
        )

    trimester_dates = (
        beginning_of_trimester.strftime("%Y-%m-%d"),
        end_of_trimester.strftime("%Y-%m-%d"),
    )

    logger.debug("Current trimester dates: %s", trimester_dates)
    return trimester_dates


def calculate_remaining_days(pedido_dia):
    logger.debug("Calculating remaining days from: %s", pedido_dia)
    pedido_date = datetime.strptime(pedido_dia, "%Y-%m-%d")
    _, end_of_trimester_str = get_current_trimester_dates()
    end_of_trimester = datetime.strptime(end_of_trimester_str, "%Y-%m-%d")
    remaining_days = max((end_of_trimester - pedido_date).days, 0)
    logger.debug("Remaining days: %s", remaining_days)
    return remaining_days


def update_current_tier_data():
    beginning_of_trimester, end_of_trimester = get_current_trimester_dates()

    pedidos_table = _fq_table(TABLE_PEDIDOS)
    current_table = _fq_table(TABLE_CURRENT)

    query = f"""
    CREATE OR REPLACE TABLE {current_table} AS
    WITH RankedPurchases AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY timestamp DESC) AS rn
      FROM {pedidos_table}
      WHERE
        pedido_dia >= @beginning_of_trimester AND
        pedido_dia <= @end_of_trimester
    ),
    MergedClients AS (
      SELECT
        cliente_nome,
        cliente_cpf,
        cliente_email,
        ROW_NUMBER() OVER (PARTITION BY cliente_cpf ORDER BY timestamp DESC) AS rn
      FROM {pedidos_table}
    ),
    FilteredPurchases AS (
      SELECT
        mc.cliente_nome,
        mc.cliente_cpf,
        mc.cliente_email,
        rp.pedido_pontos,
        rp.pedido_valor
      FROM RankedPurchases rp
      JOIN MergedClients mc ON rp.cliente_cpf = mc.cliente_cpf
      WHERE rp.rn = 1 AND mc.rn = 1
    ),
    ClientPointsAndSpend AS (
      SELECT
        cliente_nome,
        cliente_cpf,
        cliente_email,
        SUM(pedido_pontos) AS points,
        SUM(pedido_valor) AS spend
      FROM FilteredPurchases
      GROUP BY
        cliente_nome,
        cliente_cpf,
        cliente_email
    ),
    TotalPoints AS (
      SELECT
        SUM(points) AS total_points
      FROM ClientPointsAndSpend
    ),
    CumulativePoints AS (
      SELECT
        cliente_nome,
        cliente_cpf,
        cliente_email,
        points,
        spend,
        ROW_NUMBER() OVER (ORDER BY points DESC, spend DESC) AS rn,
        (SUM(points) OVER (ORDER BY points DESC, spend DESC)
          / (SELECT total_points FROM TotalPoints)) * 100 AS cumulative_percent
      FROM ClientPointsAndSpend
    )
    SELECT
      rn,
      cliente_nome,
      cliente_cpf,
      cliente_email,
      points,
      spend,
      CASE
        WHEN rn = 1 THEN 'Top1'
        WHEN rn BETWEEN 2 AND 3 THEN 'Top3'
        WHEN rn BETWEEN 4 AND 5 THEN 'Top5'
        WHEN rn BETWEEN 6 AND 10 THEN 'Top10'
        WHEN cumulative_percent <= 40 THEN 'Platina'
        WHEN cumulative_percent > 40 AND cumulative_percent <= 70 THEN 'Ouro'
        WHEN cumulative_percent > 70 AND cumulative_percent <= 90 THEN 'Prata'
        ELSE 'Bronze'
      END AS tier
    FROM CumulativePoints
    ORDER BY rn;
    """

    query_params = [
        bigquery.ScalarQueryParameter(
            "beginning_of_trimester", "STRING", beginning_of_trimester
        ),
        bigquery.ScalarQueryParameter(
            "end_of_trimester", "STRING", end_of_trimester
        ),
    ]

    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = bq_client.query(query, job_config=job_config)
    query_job.result()
    logger.info("Current tier data updated successfully")


def get_min_max_points():
    logger.debug("Getting min and max points for each tier")
    current_table = _fq_table(TABLE_CURRENT)
    query = f"""
    SELECT
      tier,
      MAX(points) AS maximum,
      MIN(points) AS minimum
    FROM
      {current_table}
    GROUP BY
      tier
    ORDER BY
      maximum;
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    min_max_points = {
        row["tier"]: {"minimum": row["minimum"], "maximum": row["maximum"]}
        for row in results
    }
    logger.debug("Min and max points for tiers: %s", min_max_points)
    return min_max_points


def get_current_tier(cliente_cpf):
    logger.debug("Getting current tier for cliente_cpf: %s", cliente_cpf)
    current_table = _fq_table(TABLE_CURRENT)
    query = f"""
    SELECT
      points,
      tier
    FROM
      {current_table}
    WHERE
      cliente_cpf = @cliente_cpf;
    """
    query_params = [
        bigquery.ScalarQueryParameter("cliente_cpf", "STRING", cliente_cpf)
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = bq_client.query(query, job_config=job_config)
    results = query_job.result()
    row = next(results, None)
    if row:
        logger.debug(
            "Current tier for %s: %s, Points: %s",
            cliente_cpf,
            row["tier"],
            row["points"],
        )
        return row["tier"], row["points"]
    logger.debug("No current tier found for %s", cliente_cpf)
    return None, None


def get_past_tier(cliente_cpf):
    logger.debug("Getting past tier for cliente_cpf: %s", cliente_cpf)
    cashback_table = _fq_table(TABLE_CASHBACK)
    query = f"""
    SELECT
      tier
    FROM
      {cashback_table}
    WHERE
      cliente_cpf = @cliente_cpf;
    """
    query_params = [
        bigquery.ScalarQueryParameter("cliente_cpf", "STRING", cliente_cpf)
    ]
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = bq_client.query(query, job_config=job_config)
    results = query_job.result()
    row = next(results, None)
    if row:
        logger.debug("Past tier for %s: %s", cliente_cpf, row["tier"])
        return row["tier"]
    logger.debug("No past tier found for %s", cliente_cpf)
    return None


def get_cashback_percentage(tier):
    return CASHBACK_PERCENTAGES.get(tier, 0)


def get_next_tier(current_tier):
    if current_tier is None:
        return None
    try:
        current_index = TIERS.index(current_tier)
    except ValueError:
        logger.error("Unknown tier '%s' not in tier list", current_tier)
        raise
    if current_index < len(TIERS) - 1:
        return TIERS[current_index + 1]
    return None


def calculate_points_needed(current_points, next_tier_min_points):
    if current_points is None or next_tier_min_points is None:
        return None
    return max(next_tier_min_points - current_points, 0)


def _has_next_tier(points_data):
    return points_data["next_tier"] is not None


def select_tier_message(current_tier, past_tier, points_data):
    # --- New customer (no past tier) ---
    if past_tier is None:
        if current_tier == "Top1":
            return f"""
            <p>Você entrou com tudo em nosso programa de fidelidade e foi direto para o topo!</p>

            <p>Com os <span class=highlight_blue>{points_data['current_points']}</span> pontos acumulados neste trimestre, incluindo os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você conquistou o nível mais exclusivo, o <span class=highlight_purple>Top1</span>, que oferece incríveis <span class=highlight_pink>20%</span> de cashback.</p>

            <p>Estamos felizes em tê-lo conosco e ansiosos para continuar recompensando sua fidelidade!</p>
            """
        if current_tier == "Bronze":
            return f"""
            <p>Você está começando sua jornada em nosso programa de fidelidade no nível <span class=highlight_purple>Bronze</span>, que oferece <span class=highlight_pink>2%</span> de cashback.</p>

            <p>Nesta compra, você ganhou <span class=highlight_blue>{points_data['purchase_points']}</span> pontos, acumulando um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre.</p>

            <p>Junte mais <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos e suba para o nível <span class=highlight_purple>Prata</span>, onde você receberá <span class=highlight_pink>3%</span> de cashback em suas compras. Continue nos visitando e aproveite os benefícios!</p>
            """
        # New customer, non-Bronze, non-Top1 — has a next tier
        return f"""
        <p>Você entrou com tudo em nosso programa de fidelidade e já está brilhando!</p>

        <p>Com os <span class=highlight_blue>{points_data['current_points']}</span> pontos acumulados neste trimestre, incluindo os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você alcançou o incrível nível <span class=highlight_purple>{current_tier}</span>, que oferece <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

        <p>Quer mais? Acumule <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos nos próximos <span class=highlight_brown>{points_data['remaining_days']}</span> dias e suba para o nível <span class=highlight_purple>{points_data['next_tier']}</span>, onde você receberá <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Vamos lá!</p>
        """

    # --- Top1 stays Top1 ---
    if current_tier == "Top1" and past_tier == "Top1":
        return f"""
        <p>Você é simplesmente incrível! Continua sendo nosso cliente Top1, o nível mais exclusivo.</p>

        <p>Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, mantendo sua posição no topo com <span class=highlight_pink>20%</span> de cashback.</p>

        <p>Agradecemos sua fidelidade contínua e estamos sempre buscando maneiras de tornar sua experiência ainda melhor. Você é especial para nós!</p>
        """

    # --- Reached Top1 (from a lower tier) ---
    if current_tier == "Top1":
        return f"""
        <p>Parabéns por alcançar o topo!</p>

        <p>Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, garantindo seu lugar no nível mais desejado, o <span class=highlight_purple>Top1</span>, que oferece incríveis <span class=highlight_pink>20%</span> de cashback.</p>

        <p>Você é um cliente valioso para nós e estamos felizes em recompensá-lo com o máximo de benefícios!</p>
        """

    # --- Same tier (non-Top1) ---
    if current_tier == past_tier:
        return f"""
        <p>Nesta compra, você ganhou <span class=highlight_blue>{points_data['purchase_points']}</span> pontos, totalizando <span class=highlight_blue>{points_data['current_points']}</span> pontos acumulados neste trimestre.</p>

        <p>Você continua no nível <span class=highlight_purple>{current_tier}</span>, que oferece <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

        <p>Faltam apenas <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos para subir para o nível <span class=highlight_purple>{points_data['next_tier']}</span> nos próximos <span class=highlight_brown>{points_data['remaining_days']}</span> dias e receber <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Não perca essa oportunidade!</p>
        """

    # --- Dropped tier ---
    if TIERS.index(current_tier) < TIERS.index(past_tier):
        return f"""
        <p>Nesta compra, você ganhou <span class=highlight_blue>{points_data['purchase_points']}</span> pontos, acumulando um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre.</p>

        <p>Para recuperar seu nível anterior <span class=highlight_purple>{past_tier}</span> e voltar a receber <span class=highlight_pink>{points_data['past_cashback']}%</span> de cashback, você precisa juntar mais <span class=highlight_blue>{points_data['points_to_current_tier']}</span> pontos nos próximos <span class=highlight_brown>{points_data['remaining_days']}</span> dias.</p>

        <p>Não desanime, você está no caminho certo para reconquistar seus benefícios.</p>
        """

    # --- Went up (non-Top1 destination, guaranteed to have next_tier) ---
    tier_diff = TIERS.index(current_tier) - TIERS.index(past_tier)
    if tier_diff == 1:
        return f"""
        <p>Parabéns! Você subiu de nível!</p>

        <p>Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, o que te coloca no nível <span class=highlight_purple>{current_tier}</span>, com <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

        <p>Quer subir ainda mais? Ganhe <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos nos <span class=highlight_brown>{points_data['remaining_days']}</span> dias restantes para alcançar o nível <span class=highlight_purple>{points_data['next_tier']}</span> e receber <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Vamos nessa!</p>
        """
    if tier_diff == 2:
        return f"""
        <p>Seu desempenho é impressionante!</p>

        <p>Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, pulando para o fantástico nível <span class=highlight_purple>{current_tier}</span>, com <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

        <p>Não pare agora! Acumule mais <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos nos próximos <span class=highlight_brown>{points_data['remaining_days']}</span> dias e alcance o nível <span class=highlight_purple>{points_data['next_tier']}</span>, onde você aproveitará <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Você consegue!</p>
        """
    return f"""
    <p>Alerta de progresso excepcional!</p>

    <p>Você não só subiu de nível, como também pulou vários de uma só vez. Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, garantindo seu lugar no nível <span class=highlight_purple>{current_tier}</span>, com <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

    <p>Agora, prepare-se para o próximo desafio: você tem <span class=highlight_brown>{points_data['remaining_days']}</span> dias para acumular <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos e desbloquear o nível <span class=highlight_purple>{points_data['next_tier']}</span>, onde você aproveitará <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Estamos ansiosos para comemorar suas conquistas. Vamos nessa!</p>
    """


def _resolve_store_display(store_prefix):
    store_config = STORE_DISPLAY_CONFIGS.get(store_prefix, {})
    return store_config.get("display_name", store_prefix)


def _build_stores_list():
    stores = []
    for prefix in sorted(STORE_DISPLAY_CONFIGS):
        cfg = STORE_DISPLAY_CONFIGS[prefix]
        stores.append(
            {
                "display_name": cfg.get("display_name", prefix),
                "address": cfg.get("address", ""),
                "phone": cfg.get("phone", ""),
                "maps_link": cfg.get("maps_link", ""),
            }
        )
    return stores


def validate_data_structures(sales_data, items_data):
    if not isinstance(sales_data, dict):
        raise ValueError(
            f"Expected sales_data to be a dictionary, got {type(sales_data)} instead."
        )
    if not isinstance(items_data, list) or not all(
        isinstance(item, dict) for item in items_data
    ):
        raise ValueError("Expected items_data to be a list of dictionaries.")


def prepare_email_data(pubsub_message):
    logger.debug("Preparing email data")
    if not isinstance(pubsub_message, dict):
        raise ValueError("pubsub_message must be a dictionary.")

    sales_data = pubsub_message.get("sales_data", {})
    items_data = pubsub_message.get("items_data", [])
    store_prefix = pubsub_message.get("store_prefix", "")

    validate_data_structures(sales_data, items_data)

    cliente_cpf = sales_data.get("cliente_cpf")
    cliente_nome = sales_data.get("cliente_nome")
    cliente_email = sales_data.get("cliente_email")
    vendedor_nome = sales_data.get("vendedor_nome")
    pedido_id = sales_data.get("pedido_id")
    pedido_dia = sales_data.get("pedido_dia")

    if not cliente_email:
        raise ValueError("Missing 'cliente_email' in sales_data.")

    if not pedido_dia:
        raise ValueError("Missing 'pedido_dia' in sales_data.")

    try:
        datetime.strptime(pedido_dia, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"Invalid 'pedido_dia' format: {pedido_dia}. Expected 'YYYY-MM-DD'."
        )

    purchase_points = sum(
        item.get("produto_pontos_total", 0)
        for item in items_data
        if isinstance(item, dict)
    )

    current_tier, current_points = get_current_tier(cliente_cpf)
    past_tier = get_past_tier(cliente_cpf)
    logger.info(
        "Tier lookup | cliente_cpf=%s current_tier=%s past_tier=%s current_points=%s",
        cliente_cpf,
        current_tier,
        past_tier,
        current_points,
    )

    min_max_points = get_min_max_points()

    next_tier = get_next_tier(current_tier)
    next_tier_min_points = (
        min_max_points.get(next_tier, {}).get("minimum") if next_tier else None
    )
    points_to_next_tier = calculate_points_needed(current_points, next_tier_min_points)

    remaining_days = calculate_remaining_days(pedido_dia)

    points_to_current_tier = None
    if past_tier and current_tier != past_tier:
        past_tier_min_points = min_max_points.get(past_tier, {}).get("minimum")
        if past_tier_min_points is not None:
            points_to_current_tier = calculate_points_needed(
                current_points, past_tier_min_points
            )

    points_data = {
        "purchase_points": purchase_points,
        "current_points": current_points,
        "current_cashback": get_cashback_percentage(current_tier),
        "next_tier": next_tier,
        "next_cashback": get_cashback_percentage(next_tier) if next_tier else None,
        "points_to_next_tier": points_to_next_tier,
        "remaining_days": remaining_days,
        "past_cashback": get_cashback_percentage(past_tier) if past_tier else None,
        "points_to_current_tier": points_to_current_tier,
    }

    tier_message = select_tier_message(current_tier, past_tier, points_data)
    tier_message = " ".join(tier_message.split())

    email_data = {
        "pedido_id": pedido_id,
        "cliente_nome": cliente_nome,
        "cliente_email": cliente_email,
        "pedido_pontos": purchase_points,
        "vendedor_nome": vendedor_nome,
        "purchase_store_name": _resolve_store_display(store_prefix),
        "stores": _build_stores_list(),
        "items_data": [
            {
                "produto_descricao": item.get("produto_descricao", ""),
                "produto_quantidade": str(item.get("produto_quantidade", "")),
                "final_multiplier": str(item.get("final_multiplier", "")),
                "produto_pontos_total": str(item.get("produto_pontos_total", "")),
            }
            for item in items_data
        ],
        "tier_message": tier_message,
    }

    if pubsub_message.get("nota_fiscal_link"):
        email_data["nota_fiscal_link"] = pubsub_message["nota_fiscal_link"]

    logger.debug("Email data preparation completed")
    return email_data


def send_email(email_data, retry_count=0):
    max_retries = 3
    recipient_email = None

    if not email_data or not email_data.get("cliente_email"):
        logger.warning("Invalid email data. Skipping email send.")
        return

    try:
        test_mode = os.environ.get("TEST_MODE", "True").lower() in ["true", "1", "t"]
        recipient_email = TEST_EMAIL if test_mode else email_data["cliente_email"]
        logger.info(
            "Sending email | recipient=%s test_mode=%s attempt=%s",
            recipient_email,
            test_mode,
            retry_count + 1,
        )

        message = Mail(
            from_email=Email(FROM_EMAIL, EMAIL_SENDER_NAME),
            to_emails=recipient_email,
        )
        message.template_id = TEMPLATE_ID
        message.dynamic_template_data = email_data

        if ASM_GROUP_ID and ASM_GROUPS_TO_DISPLAY:
            message.asm = Asm(
                group_id=ASM_GROUP_ID, groups_to_display=ASM_GROUPS_TO_DISPLAY
            )

        sendgrid_api_key = _get_sendgrid_api_key()
        sendgrid_client = SendGridAPIClient(sendgrid_api_key)
        response = sendgrid_client.send(message)

        if response.status_code not in range(200, 300):
            raise Exception(
                f"SendGrid returned non-success status: {response.status_code}"
            )

        logger.info(
            "Email sent successfully | recipient=%s status=%s",
            recipient_email,
            response.status_code,
        )

    except Exception as e:
        logger.error(
            "Error sending email | recipient=%s attempt=%s error=%s",
            recipient_email,
            retry_count + 1,
            e,
        )
        if retry_count < max_retries:
            backoff = 2 ** retry_count
            logger.info("Retrying in %ss", backoff)
            time.sleep(backoff)
            send_email(email_data, retry_count + 1)
        else:
            logger.error("Max retries reached. Raising exception.")
            raise


def main(event, context):
    if "data" not in event:
        raise KeyError("Missing 'data' field in the received event.")

    try:
        message_str = base64.b64decode(event["data"]).decode("utf-8")
        pubsub_data = json.loads(message_str)

        if isinstance(pubsub_data, str):
            pubsub_data = json.loads(pubsub_data)

        if not isinstance(pubsub_data, dict):
            raise TypeError(
                f"Pub/Sub data should be a dictionary, but got {type(pubsub_data)}"
            )

        logger.info(
            "Processing email | store_prefix=%s",
            pubsub_data.get("store_prefix", "unknown"),
        )

        update_current_tier_data()

        email_data = prepare_email_data(pubsub_data)

        if email_data and email_data.get("cliente_email"):
            send_email(email_data)
            logger.info("Email pipeline completed successfully")
        else:
            logger.warning("Email data invalid, skipping email send.")

    except json.JSONDecodeError as je:
        logger.error("JSON decoding error: %s", je)
        raise
    except Exception as e:
        logger.error("Unexpected error: %s", e, exc_info=True)
        raise
