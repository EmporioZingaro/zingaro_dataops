import base64
import json
import logging
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import secretmanager
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Asm, Email, Mail, Personalization

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s: %(levelname)s: %(message)s')

TIERS = ['Bronze', 'Prata', 'Ouro', 'Platina', 'Top10', 'Top5', 'Top3', 'Top1']
CASHBACK_PERCENTAGES = {
    'Top1': 20,
    'Top3': 15,
    'Top5': 10,
    'Top10': 7,
    'Platina': 5,
    'Ouro': 4,
    'Prata': 3,
    'Bronze': 2
}
EMAIL_SENDER_NAME = os.environ.get('EMAIL_SENDER_NAME')
FROM_EMAIL = os.environ.get('FROM_EMAIL')
TEMPLATE_ID = os.environ.get('SENDGRID_TEMPLATE_ID')
SENDGRID_SECRET_PATH = os.environ.get('SENDGRID_SECRET_PATH')
TEST_MODE = os.environ.get('TEST_MODE', 'True')
TEST_EMAIL = os.environ.get('TEST_EMAIL')


bq_client = bigquery.Client()
secret_manager_client = secretmanager.SecretManagerServiceClient()


def get_secret(secret_path):
    logging.debug(f"Attempting to retrieve secret from path: {secret_path}")
    try:
        response = secret_manager_client.access_secret_version(name=secret_path)
        secret = response.payload.data.decode("UTF-8")
        logging.debug(f"Successfully retrieved secret from path: {secret_path}")
        return secret
    except Exception as e:
        logging.error(f"Failed to retrieve secret from path {secret_path}: {e}")
        raise


def calculate_remaining_days(pedido_dia):
    logging.debug(f"Calculating remaining days from: {pedido_dia}")
    try:
        pedido_date = datetime.strptime(pedido_dia, '%Y-%m-%d')
        logging.debug(f"Parsed pedido_date: {pedido_date}")

        _, end_of_trimester_str = get_current_trimester_dates()
        logging.debug(f"End of trimester (string): {end_of_trimester_str}")

        end_of_trimester = datetime.strptime(end_of_trimester_str, '%Y-%m-%d')
        logging.debug(f"End of trimester (datetime): {end_of_trimester}")

        remaining_days = (end_of_trimester - pedido_date).days
        logging.debug(f"Remaining days calculated: {remaining_days}")

        return remaining_days
    except ValueError as ve:
        logging.error(f"ValueError in calculate_remaining_days: {ve}")
        raise
    except Exception as e:
        logging.error(f"Error calculating remaining days: {e}")
        raise



def get_current_trimester_dates():
    logging.debug("Calculating current trimester dates")
    try:
        current_date = datetime.now()

        current_trimester = (current_date.month - 1) // 3
        start_month = (current_trimester * 3) + 1
        end_month = start_month + 2

        if not (1 <= start_month <= 12 and 1 <= end_month <= 12):
            raise ValueError(f"Invalid month calculation: start_month={start_month}, end_month={end_month}")

        beginning_of_trimester = datetime(current_date.year, start_month, 1)

        if end_month == 12:
            end_of_trimester = datetime(current_date.year, 12, 31)
        else:
            end_of_trimester = datetime(current_date.year, end_month + 1, 1) - timedelta(days=1)

        trimester_dates = (
            beginning_of_trimester.strftime('%Y-%m-%d'),
            end_of_trimester.strftime('%Y-%m-%d')
        )

        logging.debug(f"Current trimester dates calculated: {trimester_dates}")
        return trimester_dates

    except Exception as e:
        logging.error(f"Error getting current trimester dates: {e}")
        raise


def update_current_tier_data():
    try:
        beginning_of_trimester, end_of_trimester = get_current_trimester_dates()

        query = """
        DECLARE table_exists BOOL DEFAULT (
          SELECT
            COUNT(1) > 0
          FROM
            `emporio-zingaro.z316_fidelidade.INFORMATION_SCHEMA.TABLES`
          WHERE
            table_name = @table_name
        );

        IF table_exists THEN
          EXECUTE IMMEDIATE 'DROP TABLE `emporio-zingaro.z316_fidelidade.current`';
        END IF;
        CREATE TABLE `emporio-zingaro.z316_fidelidade.current` AS
        WITH RankedPurchases AS (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY timestamp DESC) AS rn
          FROM `emporio-zingaro.z316_fidelidade.pedidos`
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
          FROM `emporio-zingaro.z316_fidelidade.pedidos`
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
            (SUM(points) OVER (ORDER BY points DESC, spend DESC) / (SELECT total_points FROM TotalPoints)) * 100 AS cumulative_percent
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
            bigquery.ScalarQueryParameter("table_name", "STRING", "current"),
            bigquery.ScalarQueryParameter("beginning_of_trimester", "STRING", beginning_of_trimester),
            bigquery.ScalarQueryParameter("end_of_trimester", "STRING", end_of_trimester)
        ]

        job_config = bigquery.QueryJobConfig(query_parameters=query_params)
        query_job = bq_client.query(query, job_config=job_config)
        query_job.result()
        logging.info("Current tier data updated successfully")
    except Exception as e:
        logging.error(f"Error updating current tier data: {e}", exc_info=True)
        raise


def get_min_max_points():
    logging.debug("Getting min and max points for each tier")
    try:
        query = """
        SELECT
          tier,
          MAX(points) AS maximum,
          MIN(points) AS minimum
        FROM
          `emporio-zingaro.z316_fidelidade.current`
        GROUP BY
          tier
        ORDER BY
          maximum;
        """
        query_job = bq_client.query(query)
        results = query_job.result()
        min_max_points = {row['tier']: {'minimum': row['minimum'], 'maximum': row['maximum']} for row in results}
        logging.debug(f"Min and max points for tiers: {min_max_points}")
        return min_max_points
    except Exception as e:
        logging.error(f"Error getting min/max points: {e}", exc_info=True)
        raise


def get_current_tier(cliente_cpf):
    logging.debug(f"Getting current tier for cliente_cpf: {cliente_cpf}")
    try:
        query = """
        SELECT
          points,
          tier
        FROM
          `emporio-zingaro.z316_fidelidade.current`
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
            logging.debug(f"Current tier for {cliente_cpf}: {row['tier']}, Points: {row['points']}")
            return row['tier'], row['points']
        else:
            logging.debug(f"No current tier found for {cliente_cpf}")
            return None, None
    except Exception as e:
        logging.error(f"Error getting current tier for cliente_cpf {cliente_cpf}: {e}", exc_info=True)
        raise


def get_past_tier(cliente_cpf):
    logging.debug(f"Getting past tier for cliente_cpf: {cliente_cpf}")
    try:
        query = """
        SELECT
          tier
        FROM
          `emporio-zingaro.z316_fidelidade.cashback`
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
            logging.debug(f"Past tier for {cliente_cpf}: {row['tier']}")
            return row['tier']
        else:
            logging.debug(f"No past tier found for {cliente_cpf}")
            return None
    except Exception as e:
        logging.error(f"Error getting past tier for cliente_cpf {cliente_cpf}: {e}", exc_info=True)
        raise


def get_cashback_percentage(tier):
    cashback_percentage = CASHBACK_PERCENTAGES.get(tier, 0)
    logging.debug(f"Cashback percentage for tier '{tier}': {cashback_percentage}%")
    return cashback_percentage


def get_next_tier(current_tier):
    logging.debug(f"Getting next tier for current tier: {current_tier}")
    try:
        current_index = TIERS.index(current_tier)
        if current_index < len(TIERS) - 1:
            next_tier = TIERS[current_index + 1]
            logging.debug(f"Next tier for {current_tier}: {next_tier}")
            return next_tier
        else:
            logging.debug(f"{current_tier} is the highest tier. No next tier available.")
            return None
    except ValueError as e:
        logging.error(f"Error finding current tier '{current_tier}' in tier list: {e}")
        raise


def calculate_points_needed(current_points, next_tier_min_points):
    points_needed = max(next_tier_min_points - current_points, 0)
    logging.debug(f"Calculating points needed: Current points = {current_points}, Next tier minimum points = {next_tier_min_points}, Points needed = {points_needed}")
    return points_needed


def select_tier_message(current_tier, past_tier, points_data):
    try:
        if past_tier is None:
            if current_tier == 'Bronze':
                return f"""
                <p>Você está começando sua jornada em nosso programa de fidelidade no nível <span class=highlight_purple>Bronze</span>, que oferece <span class=highlight_pink>2%</span> de cashback.</p>

                <p>Nesta compra, você ganhou <span class=highlight_blue>{points_data['purchase_points']}</span> pontos, acumulando um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre.</p>

                <p>Junte mais <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos e suba para o nível <span class=highlight_purple>Prata</span>, onde você receberá <span class=highlight_pink>3%</span> de cashback em suas compras. Continue nos visitando e aproveite os benefícios!</p>
                """
            else:
                return f"""
                <p>Você entrou com tudo em nosso programa de fidelidade e já está brilhando!</p>

                <p>Com os <span class=highlight_blue>{points_data['current_points']}</span> pontos acumulados neste trimestre, incluindo os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você alcançou o incrível nível <span class=highlight_purple>{current_tier}</span>, que oferece <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

                <p>Quer mais? Acumule <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos nos próximos <span class=highlight_brown>{points_data['remaining_days']}</span> dias e suba para o nível <span class=highlight_purple>{points_data['next_tier']}</span>, onde você receberá <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Vamos lá!</p>
                """
        elif current_tier == 'Top1' and past_tier == 'Top1':
            return f"""
            <p>Você é simplesmente incrível! Continua sendo nosso cliente Top1, o nível mais exclusivo.</p>

            <p>Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, mantendo sua posição no topo com <span class=highlight_pink>20%</span> de cashback.</p>

            <p>Agradecemos sua fidelidade contínua e estamos sempre buscando maneiras de tornar sua experiência ainda melhor. Você é especial para nós!</p>
            """
        elif current_tier == past_tier:
            return f"""
            <p>Nesta compra, você ganhou <span class=highlight_blue>{points_data['purchase_points']}</span> pontos, totalizando <span class=highlight_blue>{points_data['current_points']}</span> pontos acumulados neste trimestre.</p>

            <p>Você continua no nível <span class=highlight_purple>{current_tier}</span>, que oferece <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

            <p>Faltam apenas <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos para subir para o nível <span class=highlight_purple>{points_data['next_tier']}</span> nos próximos <span class=highlight_brown>{points_data['remaining_days']}</span> dias e receber <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Não perca essa oportunidade!</p>
            """
        elif current_tier == 'Top1':
            return f"""
            <p>Parabéns por alcançar o topo!</p>

            <p>Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, garantindo seu lugar no nível mais desejado, o <span class=highlight_purple>Top1</span>, que oferece incríveis <span class=highlight_pink>20%</span> de cashback.</p>

            <p>Você é um cliente valioso para nós e estamos felizes em recompensá-lo com o máximo de benefícios!</p>
            """
        elif TIERS.index(current_tier) < TIERS.index(past_tier):
            return f"""
            <p>Nesta compra, você ganhou <span class=highlight_blue>{points_data['purchase_points']}</span> pontos, acumulando um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre.</p>

            <p>Para recuperar seu nível anterior <span class=highlight_purple>{past_tier}</span> e voltar a receber <span class=highlight_pink>{points_data['past_cashback']}%</span> de cashback, você precisa juntar mais <span class=highlight_blue>{points_data['points_to_current_tier']}</span> pontos nos próximos <span class=highlight_brown>{points_data['remaining_days']}</span> dias.</p>

            <p>Não desanime, você está no caminho certo para reconquistar seus benefícios.</p>
            """
        else:
            tier_diff = TIERS.index(current_tier) - TIERS.index(past_tier)
            if tier_diff == 1:
                return f"""
                <p>Parabéns! Você subiu de nível!</p>

                <p>Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, o que te coloca no nível <span class=highlight_purple>{current_tier}</span>, com <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

                <p>Quer subir ainda mais? Ganhe <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos nos <span class=highlight_brown>{points_data['remaining_days']}</span> dias restantes para alcançar o nível <span class=highlight_purple>{points_data['next_tier']}</span> e receber <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Vamos nessa!</p>
                """
            elif tier_diff == 2:
                return f"""
                <p>Seu desempenho é impressionante!</p>

                <p>Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, pulando para o fantástico nível <span class=highlight_purple>{current_tier}</span>, com <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

                <p>Não pare agora! Acumule mais <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos nos próximos <span class=highlight_brown>{points_data['remaining_days']}</span> dias e alcance o nível <span class=highlight_purple>{points_data['next_tier']}</span>, onde você aproveitará <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Você consegue!</p>
                """
            else:
                return f"""
                <p>Alerta de progresso excepcional!</p>

                <p>Você não só subiu de nível, como também pulou vários de uma só vez. Com os <span class=highlight_blue>{points_data['purchase_points']}</span> pontos desta compra, você acumulou um total de <span class=highlight_blue>{points_data['current_points']}</span> pontos neste trimestre, garantindo seu lugar no nível <span class=highlight_purple>{current_tier}</span>, com <span class=highlight_pink>{points_data['current_cashback']}%</span> de cashback.</p>

                <p>Agora, prepare-se para o próximo desafio: você tem <span class=highlight_brown>{points_data['remaining_days']}</span> dias para acumular <span class=highlight_blue>{points_data['points_to_next_tier']}</span> pontos e desbloquear o nível <span class=highlight_purple>{points_data['next_tier']}</span>, onde você aproveitará <span class=highlight_pink>{points_data['next_cashback']}%</span> de cashback. Estamos ansiosos para comemorar suas conquistas. Vamos nessa!</p>
                """
    except Exception as e:
        logging.error(f"Error selecting tier message: {e}")
        raise


def validate_data_structures(sales_data, items_data):
    if not isinstance(sales_data, dict):
        raise ValueError(f"Expected sales_data to be a dictionary, got {type(sales_data)} instead.")

    if not isinstance(items_data, list) or not all(isinstance(item, dict) for item in items_data):
        raise ValueError(f"Expected items_data to be a list of dictionaries.")

    logging.debug("Data structures for sales_data and items_data are valid.")


def prepare_email_data(pubsub_message):
    logging.debug("Starting to prepare email data.")
    logging.debug(f"Type of pubsub_message upon entering prepare_email_data: {type(pubsub_message)}")
    try:
        if not isinstance(pubsub_message, dict):
            raise ValueError("pubsub_message must be a dictionary.")

        sales_data = pubsub_message.get('sales_data', {})
        items_data = pubsub_message.get('items_data', [])

        logging.debug(f"Sales data extracted: {sales_data}, type: {type(sales_data)}")
        logging.debug(f"Items data extracted: {items_data}, type: {type(items_data)}")

        validate_data_structures(sales_data, items_data)

        cliente_cpf = sales_data.get('cliente_cpf')
        cliente_nome = sales_data.get('cliente_nome')
        cliente_email = sales_data.get('cliente_email')
        vendedor_nome = sales_data.get('vendedor_nome')
        pedido_id = sales_data.get('pedido_id')
        pedido_dia = sales_data.get('pedido_dia')

        if not cliente_email:
            raise ValueError("Missing 'cliente_email' in sales_data.")

        # Validate 'pedido_dia' presence
        if not pedido_dia:
            raise ValueError("Missing 'pedido_dia' in sales_data.")

        # Ensure 'pedido_dia' is in 'YYYY-MM-DD' format
        try:
            datetime.strptime(pedido_dia, '%Y-%m-%d')
        except ValueError:
            raise ValueError(f"Invalid 'pedido_dia' format: {pedido_dia}. Expected 'YYYY-MM-DD'.")

        purchase_points = sum(item.get('produto_pontos_total', 0) for item in items_data if isinstance(item, dict))
        logging.debug(f"Total purchase points calculated: {purchase_points}")

        current_tier, current_points = get_current_tier(cliente_cpf)
        past_tier = get_past_tier(cliente_cpf)
        logging.debug(f"Current tier: {current_tier}, Past tier: {past_tier}")

        min_max_points = get_min_max_points()
        logging.debug(f"Min and max points for tiers: {min_max_points}")

        next_tier = get_next_tier(current_tier)
        next_tier_min_points = min_max_points.get(next_tier, {}).get('minimum') if next_tier else None
        points_to_next_tier = calculate_points_needed(current_points, next_tier_min_points) if next_tier else None

        remaining_days = calculate_remaining_days(pedido_dia)

        # Calculate points to current tier if past_tier exists and tiers have changed
        points_to_current_tier = None
        if past_tier and current_tier != past_tier:
            past_tier_min_points = min_max_points.get(past_tier, {}).get('minimum')
            if past_tier_min_points is not None:
                points_to_current_tier = calculate_points_needed(current_points, past_tier_min_points)

        points_data = {
            'purchase_points': purchase_points,
            'current_points': current_points,
            'current_cashback': get_cashback_percentage(current_tier),
            'next_tier': next_tier,
            'next_cashback': get_cashback_percentage(next_tier) if next_tier else None,
            'points_to_next_tier': points_to_next_tier,
            'remaining_days': remaining_days,
            'past_cashback': get_cashback_percentage(past_tier) if past_tier else None,
            'points_to_current_tier': points_to_current_tier
        }
        logging.debug(f"Points data prepared: {points_data}")

        tier_message = select_tier_message(current_tier, past_tier, points_data)
        tier_message = ' '.join(tier_message.split())
        logging.debug(f"Cleaned tier message: {tier_message}")

        email_data = {
            'pedido_id': pedido_id,
            'cliente_nome': cliente_nome,
            'cliente_email': cliente_email,
            'pedido_pontos': purchase_points,
            'vendedor_nome': vendedor_nome,
            'items_data': [
                {
                    'produto_descricao': item.get('produto_descricao', ''),
                    'produto_quantidade': str(item.get('produto_quantidade', '')),
                    'final_multiplier': str(item.get('final_multiplier', '')),
                    'produto_pontos_total': str(item.get('produto_pontos_total', ''))
                }
                for item in items_data
            ],
            'tier_message': tier_message
        }

        if 'nota_fiscal_link' in pubsub_message:
            email_data['nota_fiscal_link'] = pubsub_message['nota_fiscal_link']
            logging.debug(f"Nota fiscal link added: {pubsub_message['nota_fiscal_link']}")

        logging.debug("Email data preparation completed successfully.")
        return email_data
    except ValueError as ve:
        logging.error(f"Data validation error: {ve}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error preparing email data: {e}")
        raise


def send_email(email_data, retry_count=0):
    logging.debug(f"Entering send_email function with email data: {email_data}, retry count: {retry_count}")

    if not email_data or 'cliente_email' not in email_data or not email_data['cliente_email']:
        logging.warning("Invalid email data. Skipping email send.")
        return

    try:
        test_mode = os.environ.get('TEST_MODE', 'True').lower() in ['true', '1', 't']
        logging.debug(f"Test mode is {'active' if test_mode else 'inactive'}")

        recipient_email = TEST_EMAIL if test_mode else email_data['cliente_email']
        logging.debug(f"Recipient email determined as: {recipient_email}")

        message = Mail(
            from_email=Email(FROM_EMAIL, EMAIL_SENDER_NAME),
            to_emails=recipient_email
        )
        message.template_id = TEMPLATE_ID
        message.dynamic_template_data = email_data
        logging.debug("Mail object created with dynamic template data.")

        asm = Asm(group_id=23816, groups_to_display=[23817, 23816, 23831, 24352])
        message.asm = asm
        logging.debug("Advanced Suppression Management (ASM) settings applied to the message.")

        sendgrid_api_key = get_secret(SENDGRID_SECRET_PATH)
        logging.debug("SendGrid API key retrieved.")

        sendgrid_client = SendGridAPIClient(sendgrid_api_key)
        response = sendgrid_client.send(message)
        logging.debug(f"Message sent via SendGrid. Response status code: {response.status_code}")

        if response.status_code not in range(200, 300):
            raise Exception(f"Failed to send email: {response.status_code}")

        logging.info(f"Email successfully sent to {recipient_email}")

    except Exception as e:
        logging.error(f"Error sending email to {recipient_email}: {e}")
        if retry_count < 3:
            logging.debug(f"Retrying email send. Retry count: {retry_count + 1}")
            send_email(email_data, retry_count + 1)
        else:
            logging.debug("Maximum retry attempts reached. Email send aborted.")


def main(event, context):
    logging.debug(f"Received event: {event}")
    if 'data' not in event:
        logging.error("Missing 'data' field in the received event.")
        raise KeyError("Missing 'data' field in the received event.")

    logging.debug(f"Received base64-encoded Pub/Sub message: {event['data']}")
    try:
        message_str = base64.b64decode(event['data']).decode('utf-8')
        logging.debug(f"Decoded message string: {message_str}")
        logging.debug(f"Type of decoded message string: {type(message_str)}")

        pubsub_data = json.loads(message_str)
        logging.debug(f"Parsed Pub/Sub data: {pubsub_data}")
        logging.debug(f"Type of parsed Pub/Sub data: {type(pubsub_data)}")

        if isinstance(pubsub_data, str):
            logging.debug("Pub/Sub data is still a string, attempting to parse again.")
            pubsub_data = json.loads(pubsub_data)
            logging.debug(f"Re-parsed Pub/Sub data: {pubsub_data}")
            logging.debug(f"Type of re-parsed Pub/Sub data: {type(pubsub_data)}")

        if not isinstance(pubsub_data, dict):
            logging.error(f"Pub/Sub data is not a dictionary. Type: {type(pubsub_data)}")
            raise TypeError(f"Pub/Sub data should be a dictionary, but got {type(pubsub_data)}")

        logging.debug("Updating current tier data.")
        update_current_tier_data()

        logging.debug(f"Type of pubsub_data before calling prepare_email_data: {type(pubsub_data)}")
        logging.debug("Preparing email data.")
        email_data = prepare_email_data(pubsub_data)

        if email_data and 'cliente_email' in email_data and email_data['cliente_email']:
            logging.debug("Sending email.")
            send_email(email_data)
            logging.info("Email sent successfully.")
        else:
            logging.warning("Email data invalid, skipping email send.")

    except json.JSONDecodeError as je:
        logging.error(f"JSON decoding error: {je}")
        logging.error(f"Message string: {message_str}")
        raise

    except TypeError as te:
        logging.error(f"Type error: {te}")
        logging.error(f"Pub/Sub data type: {type(pubsub_data)}")
        raise

    except KeyError as ke:
        logging.error(f"Key error: {ke}")
        raise

    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        raise
