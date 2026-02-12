# Zingaro DataOps Pipeline

This repository hosts a multi-store ingestion and transformation pipeline for Tiny ERP sales events.
The canonical flow is:

`webhook -> GCS webhook bucket -> tiny_pipeline -> Pub/Sub -> (gcs_to_bq | tiny-transformation-sales-to-bq | fidelidade_points_to_bq)`

Use this README as the entrypoint; then open each function README for implementation-level details.

## Pipeline flow (diagram style)

```text
Tiny ERP
  └─(HTTP POST webhook payload)
     └─ webhook (Cloud Function: erp_webhook_handler)
         └─ Writes JSON to GCS webhook bucket ({prefix}-tiny-webhook)
            └─ tiny_pipeline (Cloud Function: process_webhook_payload, GCS trigger)
                ├─ Reads webhook payload
                ├─ Calls Tiny API (pedido, pesquisa, produtos, nota fiscal)
                ├─ Stores enriched payload artifacts in configured target bucket
                └─ Publishes a single Pub/Sub message with store_prefix + payload blocks
                    ├─ gcs_to_bq (raw payload landing in per-store datasets/tables)
                    ├─ tiny-transformation-sales-to-bq (sales reporting tables)
                    └─ fidelidade_points_to_bq (loyalty points tables)
```

## Cloud Functions inventory

| Function folder | Entry point | Trigger type | Required environment variables |
| --- | --- | --- | --- |
| `webhook/` | `erp_webhook_handler` | HTTP | `CNPJ_PREFIXES` (JSON map `cnpj -> prefix`). Recommended operational vars: `BUCKET_NAME_TEMPLATE`, `FILENAME_FORMAT`, `ALLOWED_SITUATIONS`. |
| `tiny_pipeline/` | `process_webhook_payload` | GCS object finalize (webhook bucket) | `STORE_CONFIGS` (JSON map by prefix, each requiring: `base_url`, `secret_path`, `target_bucket_name`, `folder_name`, `file_prefix`, `pdv_filename`, `pesquisa_filename`, `produto_filename`, `nfce_filename`, `project_id`, `source_identifier`, `version_control`), `PUBSUB_TOPIC`. |
| `gcs_to_bq/` | `cloud_function_entry_point` | Pub/Sub | `DATASET_ID`, `PROJECT_ID`, `SOURCE`, `VERSION`. Optional notification vars: `TOPIC_ID`, `NOTIFY`. |
| `tiny-transformation-sales-to-bq/` | `process_pubsub_message` | Pub/Sub | Preferred multi-store mode: `PROJECT_ID`, `DATASET_BASE_ID`, `SOURCE_ID`. Optional: `PEDIDOS_TABLE_NAME`, `ITENS_PEDIDO_TABLE_NAME`, `LOG_LEVEL`. Legacy fallback: `PEDIDOS_TABLE_ID`, `ITENS_PEDIDO_TABLE_ID` (only when `DATASET_BASE_ID` is absent). |
| `fidelidade_points_to_bq/` | `pubsub_callback` | Pub/Sub | `PROJECT_ID`, `DATASET_ID`, `TABLE_ID_SALES`, `TABLE_ID_SALES_ITEMS`, `PUBSUB_TOPIC`, `SOURCE_IDENTIFIER`, `VERSION_CONTROL`, `FIDELIDADE_MULTIPLIER`. Optional: `DEBUG_MODE`, `LOG_LEVEL`. |

## Multi-store conventions

### 1) Store prefix resolution

- `webhook` resolves `store_prefix` from `CNPJ_PREFIXES` by normalizing inbound CNPJ and mapping it to a configured prefix.
- `tiny_pipeline` resolves `store_prefix` from the webhook bucket name suffix (`-tiny-webhook`) or from webhook filename pattern as fallback.
- Downstream consumers receive `store_prefix` in Pub/Sub payload and apply their own normalization for routing.

### 2) Naming formulas by consumer

#### `webhook` producer naming

- Webhook bucket: `{prefix}-tiny-webhook` (default template).
- Webhook object path: `vendas/{prefix}-tiny-webhook-vendas-{dados_id}-{timestamp}-{unique_id}.json` (default format).

#### `tiny_pipeline` publisher contract

- Publishes a unified Pub/Sub message containing:
  - `store_prefix`
  - `uuid`
  - `timestamp`
  - `pdv_pedido_data`
  - `produto_data`
  - `pedidos_pesquisa_data`
  - `nota_fiscal_link_data` (when available)

#### `gcs_to_bq` (raw landing)

- Dataset formula: `<DATASET_ID>_<normalized_store_prefix>`
- Table formulas:
  - `<normalized_store_prefix>__pdv`
  - `<normalized_store_prefix>__pesquisa`
  - `<normalized_store_prefix>__produto`

#### `tiny-transformation-sales-to-bq` (reporting)

- Preferred dataset formula: `<normalized_store_prefix>_<DATASET_BASE_ID>`
- Table names inside that dataset:
  - `<PEDIDOS_TABLE_NAME>` (default `pedidos`)
  - `<ITENS_PEDIDO_TABLE_NAME>` (default `produtos`)

#### `fidelidade_points_to_bq` (shared loyalty tables)

- Shared dataset (no per-store dataset split): `<DATASET_ID>`
- Shared tables:
  - `<TABLE_ID_SALES>`
  - `<TABLE_ID_SALES_ITEMS>`
- Store separation is done at row level using `store_prefix` column.

## Backfill strategy

Backfills should mimic the online contract so downstream functions remain unchanged.

### Source of truth

- Tiny API is the source of truth for historical reconstruction.
- Use Tiny order identifiers/date windows to re-fetch `pdv_pedido_data`, `pedidos_pesquisa_data`, and `produto_data` (and `nota_fiscal_link_data` when needed).

### Message contract to publish

Publish to the same Pub/Sub topic consumed in production, using the same envelope fields:

```json
{
  "store_prefix": "z316",
  "uuid": "<deterministic-or-generated-uuid>",
  "timestamp": "20260202T153000",
  "pdv_pedido_data": {"...": "..."},
  "produto_data": [{"...": "..."}],
  "pedidos_pesquisa_data": {"...": "..."},
  "nota_fiscal_link_data": {"...": "..."}
}
```

### Chunking plan (date/store)

- Chunk by **store_prefix x day** (or smaller windows for high-volume days).
- Process oldest to newest to preserve business chronology in derived tables.
- Keep chunk size bounded (for example, N orders per batch or one day per run) to simplify retries and validation.

### Retry/idempotency notes

- Prefer deterministic `uuid` per source order for replay safety where possible.
- `tiny-transformation-sales-to-bq` already uses deterministic BigQuery `insertId` behavior for idempotent streaming inserts.
- For at-least-once Pub/Sub delivery, treat consumers as replayable and validate duplicates with `uuid` + order identifiers.
- Re-run failed chunks without mutating message shape.

### Validation queries in BigQuery

Use these examples after each chunk:

```sql
-- 1) Raw landing completeness by store/day (gcs_to_bq PDV table)
SELECT
  DATE(timestamp) AS dia,
  COUNT(*) AS qtd
FROM `<PROJECT>.<DATASET_ID>_<store>.${store}__pdv`
GROUP BY dia
ORDER BY dia;
```

```sql
-- 2) Duplicate check by UUID in transformed pedidos
SELECT uuid, COUNT(*) AS c
FROM `<PROJECT>.<store>_<DATASET_BASE_ID>.pedidos`
GROUP BY uuid
HAVING c > 1
ORDER BY c DESC;
```

```sql
-- 3) Loyalty points sanity by store/day
SELECT
  pedido_dia,
  store_prefix,
  COUNT(*) AS pedidos,
  SUM(pedido_pontos_total) AS pontos
FROM `<PROJECT>.<DATASET_ID>.<TABLE_ID_SALES>`
GROUP BY pedido_dia, store_prefix
ORDER BY pedido_dia DESC;
```

## Run from local machine or Google Cloud Shell

### Authentication prerequisites

- Install and initialize Google Cloud SDK (`gcloud auth login`).
- Set active project (`gcloud config set project <PROJECT_ID>`).
- Configure Application Default Credentials for local execution:
  - `gcloud auth application-default login`
- If running from Cloud Shell, ADC is usually available automatically for the current identity.

### Required permissions (minimum practical set)

- Cloud Functions deploy/invoke permissions.
- Pub/Sub publisher/subscriber permissions for pipeline topics.
- BigQuery dataset/table create + dataEditor permissions.
- Storage object admin (or scoped create/read) on webhook and target buckets.
- Secret Manager secret accessor for Tiny API token paths used by `tiny_pipeline`.

### Safe dry-run mode behavior

This repo does not expose one global `DRY_RUN=true` switch across all functions. Safe dry-runs should therefore use **isolation**:

1. Deploy functions to non-prod resources (separate project or resource suffixes).
2. Use sandbox bucket/topic/dataset names in env vars.
3. For webhook-only checks, send payloads with disallowed situations to confirm ignore path (`ALLOWED_SITUATIONS`) without downstream writes.
4. For Pub/Sub consumers, publish a limited sample payload and verify row counts in sandbox tables.


### Incident cleanup + targeted replay (generic outage pattern)

For outage windows where business dates are missing/partial, use the dedicated runbook and SQL generator:

- Runbook: [`scripts/outage_recovery_runbook.md`](scripts/outage_recovery_runbook.md)
- SQL generator: [`scripts/generate_targeted_recovery_sql.py`](scripts/generate_targeted_recovery_sql.py)

This workflow is review-first and keeps replay bounded to affected dates only.

## Detailed module READMEs

- [`webhook/README.md`](webhook/README.md)
- [`tiny_pipeline/README.md`](tiny_pipeline/README.md)
- [`gcs_to_bq/README.md`](gcs_to_bq/README.md)
- [`tiny-transformation-sales-to-bq/README.md`](tiny-transformation-sales-to-bq/README.md)
- [`fidelidade_points_to_bq/README.md`](fidelidade_points_to_bq/README.md)


## Validation workflow CLI (Chunk 7)

A local CLI is available at `scripts/validation_cli.py` to run read-only validation, remediation planning, and dedupe planning workflows using JSON inputs.

### Commands

- `validate`: builds UUID propagation + pedido completeness reports.
- `remediate`: runs bounded remediation loop (`--max-retries`) with optional action dispatch backend and writes attempt summary.
- `dedupe-report`: generates duplicate winner/loser report for one stage.
- `dedupe-apply`: generates delete-plan manifest for duplicate losers (safe planning mode).

For `validate` and `remediate`, there are two source modes:
- `--mode file` (default): consume `--expected-json` and `--stages-json`.
- `--mode live`: pull expected/observed data directly from GCS + BigQuery.

### Common flags

- `--store-prefix`
- `--start-date`
- `--end-date`
- `--output-dir`
- `--verbose`
- `--verbose-every`

### Live mode flags (validate/remediate)

- `--webhook-bucket`
- `--webhook-prefix` (default `vendas/`)
- `--tiny-api-bucket`
- `--tiny-api-prefix`
- `--stage-query stage_name::SELECT ...` (repeat per stage)
- `--require-pedido-stage <stage_name>` (repeat as needed)
- `--gcp-project` (optional)

- `--retry-wait-seconds`
- `--remediation-backend` (`noop` or `http_webhook`)
- `--webhook-url` (required for `http_webhook`)
- `--webhook-timeout-s`
- `--remediate-stage` (repeatable allowlist for stage targets)
- `--max-actions-per-attempt`
- `--stage-webhook-url stage::url` (stage-specific webhook route override)

### Example

```bash
python -m scripts.validation_cli validate   --expected-json ./tmp/expected.json   --stages-json ./tmp/stages.json   --store-prefix z316   --output-dir ./tmp/out
```

```bash
python -m scripts.validation_cli validate \
  --mode live \
  --webhook-bucket z316-tiny-webhook \
  --tiny-api-bucket z316-tiny-api \
  --stage-query "raw_pdv::SELECT store_prefix, uuid, dados_id, pedido_id, timestamp FROM `project.dataset.table`" \
  --stage-query "points_sales::SELECT store_prefix, uuid, dados_id, pedido_id, event_timestamp FROM `project.dataset.table`" \
  --require-pedido-stage points_sales \
  --store-prefix z316 \
  --verbose --verbose-every 200 --output-dir ./tmp/out-live
```


```bash
python -m scripts.validation_cli remediate \
  --mode live \
  --webhook-bucket z316-tiny-webhook \
  --tiny-api-bucket z316-tiny-api \
  --stage-query "raw_pdv::SELECT store_prefix, uuid, dados_id, pedido_id, timestamp FROM `project.dataset.table`" \
  --remediation-backend http_webhook \
  --webhook-url https://example.com/retrigger \
  --max-retries 3 \
  --retry-wait-seconds 30 \
  --verbose --verbose-every 200 --output-dir ./tmp/remediate-out
```


### Remediation outputs

`remediate` now writes:
- `remediation_result.json`
- `remediation_runtime_diagnostics.json`
- `manual_intervention_queue.json` (unresolved targets + policy recommendations)
- `remediation_run_verdict.json` (pass/fail closure verdict)
