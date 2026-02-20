# Fidelidade Email Cloud Function

This Cloud Function consumes Pub/Sub messages from `fidelidade_points_to_bq` and sends a
transactional purchase confirmation email to customers via SendGrid. The email includes
the purchased items, points earned, and a personalised message about the customer's
current position in the Clube de Fidelidade loyalty programme.

The loyalty programme is **unified across all stores** — customers from every store are
ranked together in a single pool. The email shows which store the purchase was made at
and lists all stores in the footer.

## Entry point

Set the function entry point to `main`.

## Expected Pub/Sub payload

Messages published by `fidelidade_points_to_bq` must include:

- `store_prefix`: identifies the store where the purchase was made
- `sales_data`: dict with order-level fields (`cliente_cpf`, `cliente_nome`,
  `cliente_email`, `vendedor_nome`, `pedido_id`, `pedido_dia`, etc.)
- `items_data`: list of item-level dicts (`produto_descricao`, `produto_quantidade`,
  `final_multiplier`, `produto_pontos_total`)
- `nota_fiscal_link` (optional): URL to the customer's NFC-e invoice

## Required environment variables

| Variable | Description |
|----------|-------------|
| `PROJECT_ID` | GCP project ID (e.g. `emporio-zingaro`) |
| `DATASET_ID` | BigQuery dataset for fidelidade tables (e.g. `fidelidade`) |
| `FROM_EMAIL` | Sender email address |
| `EMAIL_SENDER_NAME` | Display name for the sender (e.g. `Empório Zingaro`) |
| `SENDGRID_TEMPLATE_ID` | SendGrid dynamic template ID |
| `SENDGRID_SECRET_PATH` | Full Secret Manager path for the SendGrid API key (e.g. `projects/emporio-zingaro/secrets/sendgrid-api-key/versions/latest`) |
| `STORE_DISPLAY_CONFIGS` | JSON mapping `store_prefix` to display metadata (see below) |
| `ASM_GROUP_ID` | SendGrid ASM group ID for unsubscribe management |
| `ASM_GROUPS_TO_DISPLAY` | Comma-separated list of ASM group IDs to show in the preference centre |

## Optional environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TABLE_PEDIDOS` | `pedidos` | Table name for purchase history |
| `TABLE_CURRENT` | `current` | Table name for the ephemeral current-trimester ranking |
| `TABLE_CASHBACK` | `cashback` | Table name for previous-trimester tier history |
| `TEST_MODE` | `True` | When `True`, redirects all emails to `TEST_EMAIL` |
| `TEST_EMAIL` | _(none)_ | Destination address when `TEST_MODE` is active |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

## STORE_DISPLAY_CONFIGS format

```json
{
  "z316": {
    "display_name": "Empório Zingaro CLN 316",
    "address": "CLN 316, Bloco E, Loja 2 - Asa Norte - Brasília",
    "phone": "+55 (61) 99641-0175",
    "maps_link": "https://maps.app.goo.gl/irqng1cV7ZcCBmcRA"
  },
  "zscs": {
    "display_name": "Empório Zingaro SCS",
    "address": "SCS Qd. 7, Bloco A - Asa Sul - Brasília",
    "phone": "+55 (61) 00000-0000",
    "maps_link": "https://maps.app.goo.gl/placeholder"
  }
}
```

The `purchase_store_name` template variable is resolved from the `store_prefix` in the
Pub/Sub message. The full `stores` list is passed to the template for the footer.

## BigQuery dependencies

All tables live in a single shared dataset (`PROJECT_ID.DATASET_ID`):

| Table | Purpose |
|-------|---------|
| `TABLE_PEDIDOS` | Source of all purchase history (all stores, unified) |
| `TABLE_CURRENT` | Ephemeral table rebuilt on each invocation via `CREATE OR REPLACE TABLE`, ranking all customers by points for the current trimester |
| `TABLE_CASHBACK` | Previous-trimester tier assignments, used to detect tier changes |

## Loyalty tier logic

Tiers are assigned based on a customer's cumulative points share within the current
trimester across all stores:

| Tier | Cashback | Criteria |
|------|----------|----------|
| Top1 | 20 % | Rank 1 |
| Top3 | 15 % | Rank 2–3 |
| Top5 | 10 % | Rank 4–5 |
| Top10 | 7 % | Rank 6–10 |
| Platina | 5 % | Cumulative points share ≤ 40 % |
| Ouro | 4 % | Cumulative points share 40–70 % |
| Prata | 3 % | Cumulative points share 70–90 % |
| Bronze | 2 % | All remaining customers |

## Email template

The SendGrid dynamic template (`template.html`) expects these variables:

- `cliente_nome`, `pedido_pontos`, `purchase_store_name`
- `tier_message` (raw HTML, rendered with triple-braces `{{{tier_message}}}`)
- `pedido_id`, `vendedor_nome`, `nota_fiscal_link` (optional)
- `items_data` (array of objects with `produto_descricao`, `produto_quantidade`,
  `final_multiplier`, `produto_pontos_total`)
- `stores` (array of objects with `display_name`, `address`, `phone`, `maps_link`)
