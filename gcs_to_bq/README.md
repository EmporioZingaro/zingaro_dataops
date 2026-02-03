# GCS-to-BigQuery Cloud Function

This Cloud Function consumes Pub/Sub messages from the Tiny pipeline and loads raw payloads
into BigQuery. It routes data by `store_prefix`, creating per-store datasets and per-store tables
for each API payload type (PDV, pesquisa, produto).

## Entry point

Set the function entry point to `cloud_function_entry_point`.

## Expected Pub/Sub payload

Messages must include:

- `store_prefix`
- `uuid`
- `timestamp`
- `pdv_pedido_data` (optional)
- `produto_data` (optional)
- `pedidos_pesquisa_data` (optional)

## Required environment variables

- `DATASET_ID`: Base dataset name used to create per-store datasets.
- `PROJECT_ID`: GCP project ID.
- `SOURCE`: Source identifier stored in `source_id` fields.
- `VERSION`: Version identifier stored in `source_id` fields.
- `TOPIC_ID`: Pub/Sub topic ID for completion notifications.
- `NOTIFY`: `true` or `false` to enable completion notifications.

## Data layout

For a `store_prefix` of `z316`, the function writes to:

- Dataset: `<DATASET_ID>_z316`
- Tables: `z316__pdv`, `z316__pesquisa`, `z316__produto`
