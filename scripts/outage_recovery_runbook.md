# Tiny outage recovery runbook (cleanup + targeted backfill)

This runbook is for incidents where:
- webhook events may exist,
- API artifacts and/or downstream BigQuery tables are missing or partial for a bounded date window,
- replay must be constrained to specific business days.

> Use this as a generic operational playbook. Replace placeholders with the impacted store/date window.

## Inputs to define before execution
- `PROJECT_ID` (e.g. `emporio-zingaro`)
- `STORE_PREFIX` (e.g. `z316`)
- `START_DATE` and `END_DATE` (inclusive, `YYYY-MM-DD`)
- impacted datasets/tables (raw, transformed, loyalty, merged/reporting)
- webhook and API bucket names

## Phase 1 — Freeze and snapshot
1. Pause scheduled retry/backfill jobs for the impacted store.
2. Export snapshots (or clone tables) before destructive deletes.
3. Capture pre-check daily counts for pre/post comparison.

Example pre-check:

```sql
SELECT data_pedido, COUNT(*) AS rows, COUNT(DISTINCT pedido_number) AS pedidos, SUM(valor) AS valor_total
FROM `<PROJECT_ID>.<MERGED_DATASET>.<MERGED_ITEMS_TABLE>`
WHERE data_pedido BETWEEN DATE('<START_DATE>') AND DATE('<END_DATE>')
GROUP BY data_pedido
ORDER BY data_pedido;
```

## Phase 2 — Cleanup strategy across pipeline outputs
Prefer **business date** columns where available. Use ingestion/timestamp date only when business date is absent.

### 2.1 Generate review-first SQL

```bash
python scripts/generate_targeted_recovery_sql.py \
  --project <PROJECT_ID> \
  --store-prefix <STORE_PREFIX> \
  --raw-dataset <RAW_DATASET> \
  --raw-pdv-table <RAW_PDV_TABLE> \
  --raw-pdv-date-column <RAW_PDV_DATE_COL> \
  --raw-pesquisa-table <RAW_PESQUISA_TABLE> \
  --raw-pesquisa-date-column <RAW_PESQUISA_DATE_COL> \
  --raw-produto-table <RAW_PRODUTO_TABLE> \
  --raw-produto-timestamp-columns timestamp,update_timestamp \
  --transformed-dataset <TRANSFORMED_DATASET> \
  --transformed-pedidos-table <TRANSFORMED_PEDIDOS_TABLE> \
  --transformed-produtos-table <TRANSFORMED_PRODUTOS_TABLE> \
  --transformed-date-column pedido_dia \
  --loyalty-dataset <LOYALTY_DATASET> \
  --loyalty-pedidos-table <LOYALTY_PEDIDOS_TABLE> \
  --loyalty-produtos-table <LOYALTY_PRODUTOS_TABLE> \
  --loyalty-date-column pedido_dia \
  --merged-dataset <MERGED_DATASET> \
  --merged-items-table <MERGED_ITEMS_TABLE> \
  --merged-date-column data_pedido \
  --start-date <START_DATE> \
  --end-date <END_DATE>
```

Review generated SQL first, then run in BigQuery.

### 2.2 Discover all candidate tables before deleting

```sql
SELECT table_schema, table_name
FROM `<PROJECT_ID>`.region-us.INFORMATION_SCHEMA.TABLES
WHERE table_schema IN ('<RAW_DATASET>', '<TRANSFORMED_DATASET>', '<LOYALTY_DATASET>', '<MERGED_DATASET>')
ORDER BY table_schema, table_name;
```

## Phase 3 — API bucket cleanup for affected window
1. Always list first.
2. Delete exact matching prefixes for your real object layout.

Layout A (root prefix like `20260203T...`):

```bash
gsutil ls "gs://<API_BUCKET>/2026020[3-8]*/*"
gsutil -m rm -r "gs://<API_BUCKET>/20260203*" "gs://<API_BUCKET>/20260204*" "gs://<API_BUCKET>/20260205*" "gs://<API_BUCKET>/20260206*" "gs://<API_BUCKET>/20260207*" "gs://<API_BUCKET>/20260208*"
```

Layout B (`vendas/20260203...`):

```bash
gsutil ls "gs://<API_BUCKET>/vendas/2026020[3-8]*"
gsutil -m rm -r "gs://<API_BUCKET>/vendas/20260203*" "gs://<API_BUCKET>/vendas/20260204*" "gs://<API_BUCKET>/vendas/20260205*" "gs://<API_BUCKET>/vendas/20260206*" "gs://<API_BUCKET>/vendas/20260207*" "gs://<API_BUCKET>/vendas/20260208*"
```

If `rg` is unavailable in your shell, use `grep -E`.

## Phase 4 — Targeted backfill (missing days only)
Replay only the affected window through the webhook bucket so downstream contracts remain unchanged.

```bash
python scripts/backfill_to_webhook_bucket.py \
  --store-prefix <STORE_PREFIX> \
  --start-date <START_DATE> \
  --end-date <END_DATE> \
  --dry-run \
  --log-level INFO
```

Then run without `--dry-run`.

## Phase 5 — Validation and closure
1. **Completeness**: non-zero rows for each day in impacted tables.
2. **Reasonableness**: compare totals against a rolling baseline.
3. **Ingestion heartbeat**: check whether `timestamp` semantics changed.

## Important note on merged tables
If merged tables are rebuilt with `CREATE OR REPLACE TABLE ... AS SELECT ...`, then once upstream tables are corrected and merge is rerun, explicit merged-table DELETE is usually unnecessary.

## Guardrails
- Keep cleanup/backfill idempotent and date-window bounded.
- Always run preview `SELECT` before `DELETE`.
- Save generated SQL + execution logs for incident auditability.
