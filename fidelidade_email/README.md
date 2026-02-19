# Fidelidade Email Cloud Function

This Cloud Function consumes Pub/Sub messages from `fidelidade_points_to_bq` and sends a
transactional purchase confirmation email to the customer via SendGrid. The email includes a
summary of the items purchased, the points earned, and a personalised message about the
customer's current position in the Clube de Fidelidade loyalty programme.

## Entry point

Set the function entry point to `main`.

## Expected Pub/Sub payload

Messages must include:

- `sales_data`: dict with order-level fields (see `fidelidade_points_to_bq` for the full schema)
- `items_data`: list of item-level dicts from the same sales row
- `nota_fiscal_link` (optional): URL to the customer's NFC-e invoice

## Required environment variables

- `FROM_EMAIL`: Sender email address used in the SendGrid `from` field.
- `EMAIL_SENDER_NAME`: Display name shown alongside the sender address.
- `SENDGRID_TEMPLATE_ID`: SendGrid dynamic template ID for the purchase email.
- `SENDGRID_SECRET_PATH`: Full Secret Manager resource path for the SendGrid API key,
  e.g. `projects/PROJECT/secrets/NAME/versions/latest`.

## Optional environment variables

- `TEST_MODE`: `True` or `False` (default `True`). When `True`, all emails are redirected to
  `TEST_EMAIL` instead of the real customer address.
- `TEST_EMAIL`: Destination address used when `TEST_MODE` is active.

## BigQuery dependencies

The function reads from three tables in the `emporio-zingaro.z316_fidelidade` dataset:

- `pedidos`: source of purchase history used to build the current trimester ranking.
- `current`: ephemeral table rebuilt on every invocation to rank customers by points for the
  current trimester.
- `cashback`: stores each customer's tier from the previous trimester, used to detect tier
  changes and personalise the loyalty message.

## Loyalty tier logic

Tiers are assigned based on a customer's cumulative points share within the current trimester:

| Tier    | Cashback | Criteria                           |
|---------|----------|------------------------------------|
| Top1    | 20 %     | Rank 1                             |
| Top3    | 15 %     | Rank 2–3                           |
| Top5    | 10 %     | Rank 4–5                           |
| Top10   | 7 %      | Rank 6–10                          |
| Platina | 5 %      | Cumulative points share ≤ 40 %     |
| Ouro    | 4 %      | Cumulative points share 40–70 %    |
| Prata   | 3 %      | Cumulative points share 70–90 %    |
| Bronze  | 2 %      | All remaining customers            |
