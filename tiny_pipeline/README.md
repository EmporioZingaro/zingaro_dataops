# Tiny ERP Pipeline Cloud Function

This Cloud Function consumes Tiny ERP webhook payloads stored in Google Cloud Storage, retrieves
additional data from the Tiny API, and stores enriched payloads in a target bucket. It also
publishes a Pub/Sub notification with the collected payloads to a **single topic** that includes
the store prefix in the message payload. The function is **multi-store only** and requires
`STORE_CONFIGS` and `PUBSUB_TOPIC` environment variables.

## Deployment

1. Create a new Cloud Function (Python 3.11 recommended).
2. Set the entry point to `process_webhook_payload`.
3. Configure the trigger for the webhook storage bucket(s).
4. Set the required environment variables (see below).
5. Deploy with the dependencies listed in `requirements.txt`.

## Required environment variables

### `STORE_CONFIGS`
JSON object mapping store prefixes to configuration values. Every store entry must include the
fields below.

**Required fields per store**
- `base_url`: Base Tiny API URL (e.g. `https://api.tiny.com.br/api2/`)
- `secret_path`: Secret Manager path for the store API token
- `target_bucket_name`: Destination bucket for enriched payloads
- `folder_name`: Folder template for output payloads (supports `{timestamp}`, `{dados_id}`, `{uuid_str}`)
- `file_prefix`: Prefix applied to filenames when writing to the target bucket
- `pdv_filename`: Filename template for `pdv.pedido` payloads
- `pesquisa_filename`: Filename template for `pedidos.pesquisa` payloads
- `produto_filename`: Filename template for `produto` payloads
- `nfce_filename`: Filename template for `nfce.link` payloads
- `project_id`: Project identifier stored in metadata
- `source_identifier`: Source identifier stored in metadata
- `version_control`: Version identifier stored in metadata
 
### `PUBSUB_TOPIC`
Single Pub/Sub topic path (e.g. `projects/<project>/topics/tiny-pipeline-events`) used for all
stores. Messages include a `store_prefix` field so downstream consumers can route data as needed.

## Configuration model

This function relies on a per-store configuration map (`STORE_CONFIGS`). Each store entry declares:

- Tiny API access (`base_url`, `secret_path`)
- Output storage settings (`target_bucket_name`, `folder_name`, `file_prefix`, and filename templates)
- Metadata fields (`project_id`, `source_identifier`, `version_control`)

The function resolves the store prefix from the triggering bucket or filename, selects the
corresponding store entry, fetches the Tiny API token from Secret Manager, and stores the resulting
payloads in the configured target bucket for that store.

## Data flow

1. A Tiny webhook payload is written to a per-store webhook bucket.
2. This function reads the webhook payload to extract `dados.id`.
3. Tiny API endpoints are called to fetch order, product, and NFC-e data.
4. Enriched JSON payloads are stored in the store-specific target bucket.
5. A Pub/Sub message is published to the shared topic with `store_prefix` included.

## Notes
- The store prefix is resolved from the triggering bucket name (e.g. `store-a-tiny-webhook`) or
  the webhook filename (e.g. `vendas/store-a-tiny-webhook-vendas-...`).
- Use **one Pub/Sub topic** for all stores and rely on the `store_prefix` field in the published
  message to route data downstream.
- Use **one bucket per store** for both the webhook ingestion bucket and the enriched data output
  bucket. This keeps store data isolated while allowing a shared downstream BigQuery pipeline.
- Each store can point to its own Tiny API token in Secret Manager.
- The function deduplicates product IDs before fetching product details.
