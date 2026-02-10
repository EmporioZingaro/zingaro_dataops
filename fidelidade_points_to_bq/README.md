# Fidelidade Points to BigQuery Cloud Function

This Cloud Function consumes Pub/Sub messages from the Tiny pipeline and writes loyalty program
points into BigQuery. It keeps **one shared dataset/table pair** for all stores and uses a
`store_prefix` column to identify the store in every row.

## Entry point

Set the function entry point to `pubsub_callback`.

## Expected Pub/Sub payload

Messages must include:

- `store_prefix`
- `uuid`
- `timestamp`
- `pdv_pedido_data`
- `produto_data`
- `pedidos_pesquisa_data`
- `nota_fiscal_link_data` (optional)

## Required environment variables

- `PROJECT_ID`: GCP project ID used for BigQuery and Pub/Sub.
- `DATASET_ID`: Dataset name for the shared loyalty tables.
- `TABLE_ID_SALES`: Table name for order-level loyalty points.
- `TABLE_ID_SALES_ITEMS`: Table name for item-level loyalty points.
- `PUBSUB_TOPIC`: Pub/Sub topic name for downstream notifications.
- `SOURCE_IDENTIFIER`: Source identifier stored in output rows.
- `VERSION_CONTROL`: Version identifier stored in output rows.
- `FIDELIDADE_MULTIPLIER`: Base multiplier for loyalty points.

Compatibility note:

- The item payload keeps legacy fields `produto_multiplier`, `special_multiplier`, and
  `final_multiplier` for downstream compatibility with existing BigQuery tables.
- In the simplified model, `produto_multiplier` and `special_multiplier` are always `0.0` and
  `final_multiplier` is always equal to `FIDELIDADE_MULTIPLIER`.

## Optional environment variables

- `DEBUG_MODE`: `true` or `false` (default `false`). When `true`, logs raw inbound payloads and processing objects with the `[DEBUG_PAYLOAD]` marker at INFO level for easier visibility in Cloud Logging.
- `LOG_LEVEL`: Logging level (default `INFO`), e.g. `DEBUG`, `INFO`, `WARNING`, `ERROR`.

## BigQuery layout

Tables are partitioned by `pedido_dia` and clustered by `store_prefix` plus customer/vendor
identifiers for faster filtering.

Example shared layout:

- Dataset: `<DATASET_ID>`
- Tables: `<TABLE_ID_SALES>`, `<TABLE_ID_SALES_ITEMS>`
