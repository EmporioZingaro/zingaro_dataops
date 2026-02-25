import logging
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.cloud import secretmanager
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Asm, Email, Mail

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

bq_client = bigquery.Client()

# -----------------------------
# REQUIRED MANUAL UPDATES
# -----------------------------
# 1) Keep CASHBACK_TABLE pointed at the canonical `cashback` table generated
#    by the quarter orchestrator.
# 2) Keep PEDIDOS_TABLE pointed at the shared loyalty `pedidos` table.
# 3) Ensure PEDIDOS_STORE_COLUMN exists and carries the store prefix that will
#    be sent to SendGrid as `store_location`.
# -----------------------------

SENDGRID_SECRET_PATH = "projects/559935551835/secrets/SendGrid/versions/1"
SENDGRID_TEMPLATE_ID = "d-7dacbc8b57f44d88bb1252d474d870fc"
FROM_EMAIL = "sac@emporiozingaro.com"
FROM_NAME = "Empório Zingaro"

# Shared fidelidade tables (not store-split by table name).
CASHBACK_TABLE = "emporio-zingaro.z316_fidelidade.cashback"
PEDIDOS_TABLE = "emporio-zingaro.z316_fidelidade.pedidos"
PEDIDOS_STORE_COLUMN = "store_prefix"

secret_manager = secretmanager.SecretManagerServiceClient()
sendgrid_secret = secret_manager.access_secret_version(
    request={"name": SENDGRID_SECRET_PATH}
).payload.data.decode("UTF-8")
sg_client = SendGridAPIClient(sendgrid_secret)

TEST_MODE = False
TEST_EMAIL = "rodrigo@brunale.com"
EMAIL_SEND_LIMIT = 3

# Nominal percentages must stay aligned with sql/fidelidade_quarter_orchestrator.sql.
TIER_PERCENTAGES = {
    'Top1': 20,
    'Top3': 15,
    'Top5': 10,
    'Top10': 7,
    'Platina': 5,
    'Ouro': 4,
    'Prata': 3,
    'Bronze': 2,
}


def get_porcentagem_cashback(tier: str, cashback_value: float, spend_value: float) -> Optional[float]:
    nominal_percentage = TIER_PERCENTAGES.get(tier)
    if nominal_percentage is not None:
        return nominal_percentage

    if spend_value and spend_value > 0:
        return round((cashback_value / spend_value) * 100, 2)
    return None


def fetch_pedidos(client_email: str, quarter_start: Any, quarter_end: Any) -> List[Dict[str, Any]]:
    pedidos_query = f"""
    SELECT
      pedido_dia,
      pedido_id,
      vendedor_nome,
      pedido_pontos,
      {PEDIDOS_STORE_COLUMN} AS store_location
    FROM `{PEDIDOS_TABLE}`
    WHERE cliente_email = @cliente_email
      AND pedido_dia >= @quarter_start
      AND pedido_dia <= @quarter_end
    ORDER BY pedido_dia DESC
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cliente_email", "STRING", client_email),
            bigquery.ScalarQueryParameter("quarter_start", "DATE", quarter_start),
            bigquery.ScalarQueryParameter("quarter_end", "DATE", quarter_end),
        ]
    )

    try:
        pedidos_results = bq_client.query(pedidos_query, job_config=job_config).result()
        pedidos = []
        for pedido_row in pedidos_results:
            pedidos.append(
                {
                    'pedido_dia': pedido_row['pedido_dia'].strftime('%Y-%m-%d'),
                    'pedido_id': pedido_row['pedido_id'],
                    'vendedor_nome': pedido_row['vendedor_nome'],
                    'pedido_pontos': pedido_row['pedido_pontos'],
                    'store_location': pedido_row['store_location'],
                }
            )
        return pedidos
    except Exception as exc:
        logging.error(
            "Error fetching pedidos data for email '%s': %s",
            client_email,
            exc,
        )
        return []


def fetch_cashback_data() -> List[Dict[str, Any]]:
    cashback_query = f"""
    SELECT
      quarter_id,
      quarter_start,
      quarter_end,
      cliente_nome,
      cliente_cpf,
      cliente_email,
      points,
      spend,
      tier,
      cashback
    FROM `{CASHBACK_TABLE}`
    """
    try:
        cashback_results = bq_client.query(cashback_query).result()
        return list(cashback_results)
    except Exception as exc:
        logging.error("Error fetching cashback data: %s", exc)
        return []


def process_client_data(cashback_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    client_email = cashback_row.get('cliente_email')
    if not client_email:
        logging.warning(
            "Skipping client %s due to missing email in cashback table.",
            cashback_row.get('cliente_nome'),
        )
        return None

    quarter_start = cashback_row.get('quarter_start')
    quarter_end = cashback_row.get('quarter_end')
    quarter_id = cashback_row.get('quarter_id')
    if not quarter_start or not quarter_end or not quarter_id:
        logging.warning(
            "Skipping client %s due to missing quarter metadata in cashback table.",
            cashback_row.get('cliente_nome'),
        )
        return None

    spend_value = float(cashback_row.get('spend', 0) or 0)
    cashback_value = float(cashback_row.get('cashback', 0) or 0)
    porcentagem_cashback = get_porcentagem_cashback(
        cashback_row.get('tier'),
        cashback_value,
        spend_value,
    )

    client_data = {
        'quarter_id': quarter_id,
        'quarter_start': quarter_start.strftime('%Y-%m-%d'),
        'quarter_end': quarter_end.strftime('%Y-%m-%d'),
        'cliente_nome': cashback_row['cliente_nome'],
        'cliente_email': client_email,
        'cashback': "{:.2f}".format(cashback_value),
        'tier': cashback_row['tier'],
        'porcentagem_cashback': porcentagem_cashback,
        'points': cashback_row['points'],
        'pedidos': [],
    }

    if client_data['porcentagem_cashback'] is None:
        logging.warning(
            "Skipping client %s due to unexpected tier and no effective cashback fallback.",
            client_data['cliente_nome'],
        )
        return None

    pedidos = fetch_pedidos(client_email, quarter_start, quarter_end)
    if not pedidos:
        logging.warning(
            "No pedidos found for client %s in quarter %s.",
            client_data['cliente_nome'],
            quarter_id,
        )
        return None

    client_data['pedidos'] = pedidos
    return client_data


def send_email(client_data: Dict[str, Any], retry_count: int = 0) -> None:
    if not client_data or 'cliente_email' not in client_data:
        logging.warning("Invalid client data. Skipping email send.")
        return

    recipient_email = TEST_EMAIL if TEST_MODE else client_data['cliente_email']

    try:
        message = Mail(from_email=Email(FROM_EMAIL, FROM_NAME), to_emails=recipient_email)
        message.template_id = SENDGRID_TEMPLATE_ID
        message.dynamic_template_data = client_data
        message.asm = Asm(group_id=23817, groups_to_display=[23817, 23816, 23831, 24352])

        response = sg_client.send(message)
        if response.status_code not in range(200, 300):
            raise RuntimeError(f"Failed to send email: {response.status_code}")

        logging.info(
            "Email successfully sent to %s for quarter %s",
            recipient_email,
            client_data.get('quarter_id'),
        )

    except Exception as exc:
        logging.error("Error sending email to %s: %s", recipient_email, exc)
        if retry_count < 3:
            send_email(client_data, retry_count + 1)


def main(request) -> str:
    logging.info("Starting cashback email sender...")
    email_count = 0

    cashback_data = fetch_cashback_data()
    for cashback_row in cashback_data:
        client_data = process_client_data(cashback_row)
        if not client_data:
            continue

        if TEST_MODE and email_count >= EMAIL_SEND_LIMIT:
            logging.info("Email send limit reached in test mode.")
            break

        send_email(client_data)
        email_count += 1

    logging.info("Email sending process completed.")
    return "OK"


if __name__ == "__main__":
    main(None)
