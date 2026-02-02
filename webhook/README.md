# Tiny ERP Webhook Handler

This document describes how the Tiny ERP webhook handler works, how to configure
it for multiple stores, and how to test it locally. The goal is to keep the
handler simple, robust, and production-ready because it is the first stage of
our pipeline.

## What the webhook does

1. **Receives POST requests** from Tiny ERP and parses the JSON payload.
2. **Validates** required fields (`versao`, `cnpj`, `tipo`, `dados`).
3. **Filters events** to only accept:
   - `tipo = "inclusao_pedido"`
   - `situacao = "FATURADO"` (configurable)
4. **Resolves the store prefix** by matching the payload `cnpj` against a
   mapping of `CNPJ -> prefix`.
5. **Builds a bucket name** using a template (e.g., `{prefix}-tiny-webhook`).
6. **Stores the payload** in Google Cloud Storage using a deterministic filename
   format that includes the store prefix and a timestamp.
7. **Logs** concise INFO messages in production and full payloads at DEBUG level.

If the webhook receives a valid payload but `tipo` or `situacao` are not allowed,
it returns HTTP 200 with an "ignored" message. Invalid payloads return HTTP 400.

## Environment variables

These values let you scale to many stores without changing code.

| Variable | Purpose | Default |
| --- | --- | --- |
| `CNPJ_PREFIXES` | JSON object mapping `cnpj` to store prefix. | `{}` |
| `BUCKET_NAME_TEMPLATE` | Template for GCS bucket names. | `{prefix}-tiny-webhook` |
| `FILENAME_FORMAT` | Template for object names in GCS. | `vendas/{prefix}-tiny-webhook-vendas-{dados_id}-{timestamp}-{unique_id}.json` |
| `ALLOWED_SITUATIONS` | Comma-separated list of accepted situations. | `FATURADO` |

### Example env configuration

```
CNPJ_PREFIXES={"22945440000154":"Z316","12345678000199":"ZSCS"}
BUCKET_NAME_TEMPLATE={prefix}-tiny-webhook
FILENAME_FORMAT=vendas/{prefix}-tiny-webhook-vendas-{dados_id}-{timestamp}-{unique_id}.json
ALLOWED_SITUATIONS=FATURADO
```

### Mapping many stores

To support dozens or hundreds of stores, add each `CNPJ` to the JSON mapping in
`CNPJ_PREFIXES`. Example with more entries:

```
CNPJ_PREFIXES={"22945440000154":"Z316","12345678000199":"ZSCS","98765432000155":"Z999"}
```

## Logging behavior

- **INFO**: concise operational messages (ignored/accepted/stored events).
- **DEBUG**: full payload plus acceptance details (prefix, situacao, dados id).

Set the runtime log level to DEBUG only when diagnosing issues to avoid storing
sensitive payloads in logs.

## Payload examples

These are the payloads seen in production. They are useful for testing and
understanding the data shape.

### Example: `atualizacao_pedido` (ignored)

```json
{
  "versao": "1.0.1",
  "cnpj": "22945440000154",
  "tipo": "atualizacao_pedido",
  "dados": {
    "id": "885188291",
    "numero": "176097",
    "data": "02/02/2026",
    "idPedidoEcommerce": "",
    "codigoSituacao": "faturado",
    "descricaoSituacao": "Faturado",
    "idContato": "708490510",
    "idNotaFiscal": "885188295",
    "nomeEcommerce": "",
    "cliente": {
      "nome": "Consumidor Final",
      "cpfCnpj": ""
    }
  }
}
```

### Example: `inclusao_pedido` (accepted when `situacao = FATURADO`)

```json
{
  "versao": "1.0.1",
  "cnpj": "22945440000154",
  "tipo": "inclusao_pedido",
  "dados": {
    "id": "885188291",
    "numero": "176097",
    "data": "02/02/2026",
    "idPedidoEcommerce": "",
    "codigoSituacao": "faturado",
    "descricaoSituacao": "Faturado",
    "idContato": "708490510",
    "idNotaFiscal": "885188295",
    "nomeEcommerce": "",
    "cliente": {
      "nome": "Consumidor Final",
      "cpfCnpj": ""
    }
  }
}
```

## Testing

### Unit tests

Unit tests live in `tests/test_main.py` and exercise `webhook/handler.py`. The
test suite:

- Stubs Flask helpers (`jsonify`, `abort`, `make_response`).
- Stubs the GCS client so uploads do not hit the network.
- Stubs `tenacity` retry decorators so logic can be tested quickly.

The tests cover:

- Accepted `FATURADO` payloads.
- Ignored non-`FATURADO` situations.
- Rejection of unknown CNPJ values.
- Ignored `tipo` values other than `inclusao_pedido`.

Run the tests from the repository root:

```
pytest
```

## Additional notes & suggestions

- Keep `CNPJ_PREFIXES` and bucket templates in your deployment environment and
  treat them as configuration, not code.
- Consider restricting log level to INFO in production to avoid logging full
  payload data.
- Keep `ALLOWED_SITUATIONS` narrow to reduce noise in the pipeline entry point.
