#!/usr/bin/env python3
"""Production backfill utility for webhook-bucket reconciliation.

This script reconciles Tiny orders against existing webhook files, then uploads
missing webhook-like payloads so the existing production pipeline can process
historical data using the normal flow:

  webhook bucket -> tiny_pipeline -> Pub/Sub -> BigQuery consumers
"""

# ---------------------------------------------------------------------------
# Backfill runbook (operator instructions)
# ---------------------------------------------------------------------------
# Install dependencies (local/Cloud Shell):
#   python3 -m pip install --upgrade requests google-cloud-storage google-cloud-secret-manager tenacity
#
# Required environment variables:
#   export STORE_CONFIGS='{
#     "z316": {
#       "base_url": "https://api.tiny.com.br/api2/",
#       "secret_path": "projects/559935551835/secrets/z316-tiny-token-api"
#     },
#     "z500": {
#       "base_url": "https://api.tiny.com.br/api2/",
#       "secret_path": "projects/559935551835/secrets/z500-tiny-token-api"
#     }
#   }'
#
#   # secret_path supports both forms:
#   #   projects/<id>/secrets/<name>
#   #   projects/<id>/secrets/<name>/versions/<version>
#   # If version is omitted, the script automatically uses /versions/latest.
#
#   # Optional. Defaults to "{prefix}-tiny-webhook" when not set.
#   export WEBHOOK_BUCKET_TEMPLATE='{prefix}-tiny-webhook'
#
# Optional payload enrichment map for CNPJ by store (not required for backfill to work):
#   export STORE_CNPJ_MAP='{"z316":"22945440000154","z500":"00999999000100"}'
#
# Recommended first execution for both stores (dry run):
#   python scripts/backfill_to_webhook_bucket.py \
#     --start-date 2024-01-01 \
#     --end-date 2024-01-31 \
#     --store-cnpj-map "$STORE_CNPJ_MAP" \
#     --dry-run \
#     --log-level INFO
#
# Real execution for both stores (sequential):
#   python scripts/backfill_to_webhook_bucket.py \
#     --start-date 2024-01-01 \
#     --end-date 2024-01-31 \
#     --store-cnpj-map "$STORE_CNPJ_MAP" \
#     --log-level INFO
#
# Single-store dry run example:
#   python scripts/backfill_to_webhook_bucket.py \
#     --store-prefix z316 \
#     --start-date 2024-01-01 \
#     --end-date 2024-01-31 \
#     --dry-run
#
# Dry-run note:
#   - Dry-run defaults to a single cycle (`--dry-run-cycles 1`) to avoid
#     infinite repeats while missing IDs remain unmodified.
#   - Increase with `--dry-run-cycles N` if you explicitly want more cycles.
#
# Behavior highlights:
#   - Efficiently lists existing webhook files via prefix listing in GCS.
#   - Compares Tiny pedidos IDs against bucket IDs and uploads only missing ones.
#   - Runs convergence cycles until N consecutive zero-missing cycles.
#   - Supports sequential all-store execution when --store-prefix is omitted.
#   - Throttles uploads (default 5s between objects) to protect downstream Tiny API limits.
#   - Throttles Tiny page listing calls (default 1.5s between pages) to stay
#     under Tiny API limits during large historical scans.
# ---------------------------------------------------------------------------

import argparse
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple

import requests
from google.cloud import secretmanager, storage
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

STORE_CONFIGS_ENV = "STORE_CONFIGS"
WEBHOOK_BUCKET_TEMPLATE_ENV = "WEBHOOK_BUCKET_TEMPLATE"
DEFAULT_WEBHOOK_BUCKET_TEMPLATE = "{prefix}-tiny-webhook"
WEBHOOK_PREFIX_TEMPLATE = "vendas/{prefix}-tiny-webhook-vendas-"
REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_SETTLE_MINUTES = 10
DEFAULT_SKIP_RECENT_MINUTES = 5
DEFAULT_CONSECUTIVE_ZERO_TARGET = 2
DEFAULT_EMIT_INTERVAL_SECONDS = 5.0
DEFAULT_DRY_RUN_CYCLES = 1
DEFAULT_LIST_PAGE_INTERVAL_SECONDS = 1.5

WEBHOOK_FILENAME_PATTERN = re.compile(
    r"^vendas/(?P<prefix>.+?)-tiny-webhook-vendas-(?P<dados_id>\d+)-"
    r"(?P<timestamp>\d{8}T\d{6})-(?P<uuid>[0-9a-fA-F-]{36})\.json$"
)
SECRET_WITH_VERSION_PATTERN = re.compile(r"^projects/[^/]+/secrets/[^/]+/versions/[^/]+$")
SECRET_WITHOUT_VERSION_PATTERN = re.compile(r"^projects/[^/]+/secrets/[^/]+$")

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Raised when Tiny returns a non-retryable validation-level API error."""


class InvalidTokenError(Exception):
    """Raised when Tiny indicates token/authentication problems."""


class RetryableError(Exception):
    """Raised when Tiny responds with an error that should be retried."""


@dataclass(frozen=True)
class StoreConfig:
    """Subset of store configuration needed for this backfill script."""

    base_url: str
    secret_path: str


@dataclass
class PedidoRecord:
    """Small normalized representation of pedido metadata used by reconciliation."""

    pedido_id: str
    numero: Optional[str]
    data_pedido: Optional[str]
    situacao: Optional[str]


@dataclass
class CycleStats:
    """Metrics captured for one reconciliation cycle."""

    tiny_count: int
    bucket_count: int
    missing_count: int
    emitted_count: int


def setup_logging(level: str) -> None:
    resolved = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=resolved, format="%(asctime)s %(levelname)s %(message)s")


def parse_store_configs(raw_mapping: str) -> Dict[str, StoreConfig]:
    try:
        mapping = json.loads(raw_mapping)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid {STORE_CONFIGS_ENV} JSON: {exc}") from exc

    if not isinstance(mapping, dict) or not mapping:
        raise ValueError(f"{STORE_CONFIGS_ENV} must be a non-empty JSON object")

    parsed: Dict[str, StoreConfig] = {}
    for prefix, cfg in mapping.items():
        if not isinstance(cfg, dict):
            raise ValueError(f"Config for prefix '{prefix}' must be a JSON object")
        parsed[prefix] = StoreConfig(
            base_url=cfg["base_url"],
            secret_path=cfg["secret_path"],
        )
    return parsed


def load_store_configs() -> Dict[str, StoreConfig]:
    raw_mapping = os.getenv(STORE_CONFIGS_ENV)
    if not raw_mapping:
        raise ValueError(f"Missing required env var {STORE_CONFIGS_ENV}")
    return parse_store_configs(raw_mapping)


def resolve_bucket_name(prefix: str) -> str:
    template = os.getenv(WEBHOOK_BUCKET_TEMPLATE_ENV, DEFAULT_WEBHOOK_BUCKET_TEMPLATE)
    return template.format(prefix=prefix)


def normalize_secret_path(secret_path: str) -> str:
    """Accept Secret Manager path with or without explicit version.

    Valid formats:
      - projects/<project>/secrets/<secret>
      - projects/<project>/secrets/<secret>/versions/<version>
    """
    if SECRET_WITH_VERSION_PATTERN.match(secret_path):
        return secret_path
    if SECRET_WITHOUT_VERSION_PATTERN.match(secret_path):
        normalized = f"{secret_path}/versions/latest"
        logger.info("Secret path has no version; using latest | secret_path=%s", normalized)
        return normalized
    raise ValueError(
        "Invalid secret_path format. Expected 'projects/*/secrets/*' "
        "or 'projects/*/secrets/*/versions/*'."
    )


def parse_iso_date(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d")


def tiny_date(raw: datetime) -> str:
    return raw.strftime("%d/%m/%Y")


def parse_tiny_order_date(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%d/%m/%Y")
    except ValueError:
        return None


def build_timestamp_from_order_date(data_pedido: Optional[str]) -> str:
    parsed = parse_tiny_order_date(data_pedido)
    if parsed:
        return parsed.strftime("%Y%m%dT000000")
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")


def webhook_prefix(prefix: str) -> str:
    return WEBHOOK_PREFIX_TEMPLATE.format(prefix=prefix)


def get_api_token(secret_client: secretmanager.SecretManagerServiceClient, secret_path: str) -> str:
    normalized_secret_path = normalize_secret_path(secret_path)
    response = secret_client.access_secret_version(request={"name": normalized_secret_path})
    return response.payload.data.decode("utf-8")


def validate_json_payload(json_data: dict) -> None:
    status_processamento = json_data.get("retorno", {}).get("status_processamento")

    if status_processamento == "3":
        return
    if status_processamento == "2":
        raise ValidationError("Invalid query parameter.")
    if status_processamento == "1":
        codigo_erro = json_data.get("retorno", {}).get("codigo_erro")
        erros = json_data.get("retorno", {}).get("erros", [])
        erro_message = erros[0].get("erro") if erros else "Unknown error"
        if codigo_erro == "1":
            raise InvalidTokenError(f"Token is not valid: {erro_message}")
        raise RetryableError(f"Tiny returned retryable error: {erro_message}")


@retry(
    wait=wait_exponential(multiplier=2, min=2, max=60),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type((requests.exceptions.RequestException, RetryableError)),
)
def make_api_call(url: str) -> dict:
    sanitized_url = url.split("?token=")[0]
    logger.debug("Requesting Tiny endpoint: %s", sanitized_url)
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    validate_json_payload(payload)
    return payload


def iter_pedidos(
    base_url: str,
    token: str,
    start_date: Optional[datetime],
    end_date: Optional[datetime],
    max_pages: Optional[int],
    list_page_interval_seconds: float,
) -> Iterator[PedidoRecord]:
    page = 1
    total_pages = 1
    expected_total_pages: Optional[int] = None
    yielded = 0

    while page <= total_pages:
        query = ["formato=JSON", f"pagina={page}"]
        if start_date:
            query.append(f"dataInicial={tiny_date(start_date)}")
        if end_date:
            query.append(f"dataFinal={tiny_date(end_date)}")

        url = f"{base_url}pedidos.pesquisa.php?token={token}&{'&'.join(query)}"
        data = make_api_call(url)
        retorno = data.get("retorno", {})
        reported_total_pages = int(retorno.get("numero_paginas", 1))
        if expected_total_pages is None:
            expected_total_pages = reported_total_pages
        elif reported_total_pages < expected_total_pages:
            logger.warning(
                "Tiny reported lower numero_paginas mid-scan; keeping initial page target "
                "| page=%s reported=%s expected=%s",
                page,
                reported_total_pages,
                expected_total_pages,
            )
        elif reported_total_pages > expected_total_pages:
            logger.info(
                "Tiny reported higher numero_paginas mid-scan; expanding page target "
                "| page=%s reported=%s previous_expected=%s",
                page,
                reported_total_pages,
                expected_total_pages,
            )
            expected_total_pages = reported_total_pages

        total_pages = expected_total_pages
        page_count = 0

        for wrapper in retorno.get("pedidos", []):
            pedido = wrapper.get("pedido", {})
            pedido_id = str(pedido.get("id", "")).strip()
            if not pedido_id:
                continue
            page_count += 1
            yielded += 1
            yield PedidoRecord(
                pedido_id=pedido_id,
                numero=str(pedido.get("numero")) if pedido.get("numero") is not None else None,
                data_pedido=pedido.get("data_pedido") or pedido.get("data"),
                situacao=pedido.get("situacao"),
            )

        logger.info(
            "Tiny listing page processed | page=%s/%s pedidos_in_page=%s cumulative=%s",
            page,
            total_pages,
            page_count,
            yielded,
        )
        page += 1

        if max_pages and page > max_pages:
            logger.warning("Stopping early due to --max-pages=%s", max_pages)
            break

        if list_page_interval_seconds > 0:
            time.sleep(list_page_interval_seconds)


def filter_recent_pedidos(pedidos: Dict[str, PedidoRecord], skip_recent_minutes: int) -> Dict[str, PedidoRecord]:
    if skip_recent_minutes <= 0:
        return pedidos

    cutoff = datetime.now() - timedelta(minutes=skip_recent_minutes)
    filtered: Dict[str, PedidoRecord] = {}
    skipped = 0

    for pedido_id, pedido in pedidos.items():
        order_date = parse_tiny_order_date(pedido.data_pedido)
        if order_date and order_date > cutoff:
            skipped += 1
            continue
        filtered[pedido_id] = pedido

    if skipped:
        logger.info("Skipped %s pedidos newer than %s minutes", skipped, skip_recent_minutes)
    return filtered


def build_webhook_payload(store_cnpj: str, pedido: PedidoRecord) -> dict:
    payload = {
        "versao": "backfill",
        "tipo": "inclusao_pedido",
        "dados": {
            "id": pedido.pedido_id,
            "numero": pedido.numero,
            "data": pedido.data_pedido,
            "descricaoSituacao": pedido.situacao or "Faturado",
        },
    }
    if store_cnpj:
        payload["cnpj"] = store_cnpj
    return payload


def deterministic_uuid(prefix: str, pedido_id: str) -> str:
    namespace = uuid.UUID("5e31a651-64b1-4cca-84b5-bf94f95f7dd7")
    return str(uuid.uuid5(namespace, f"{prefix}:{pedido_id}"))


def build_filename(prefix: str, pedido: PedidoRecord, use_deterministic_uuid: bool) -> str:
    timestamp = build_timestamp_from_order_date(pedido.data_pedido)
    unique_id = deterministic_uuid(prefix, pedido.pedido_id) if use_deterministic_uuid else str(uuid.uuid4())
    return f"vendas/{prefix}-tiny-webhook-vendas-{pedido.pedido_id}-{timestamp}-{unique_id}.json"


def extract_dados_id_from_blob_name(blob_name: str) -> Optional[str]:
    match = WEBHOOK_FILENAME_PATTERN.match(blob_name)
    if not match:
        return None
    return match.group("dados_id")


def fetch_existing_webhook_ids(storage_client: storage.Client, bucket_name: str, prefix: str) -> Set[str]:
    ids: Set[str] = set()
    object_prefix = webhook_prefix(prefix)
    scanned_objects = 0
    for blob in storage_client.list_blobs(bucket_name, prefix=object_prefix):
        scanned_objects += 1
        dados_id = extract_dados_id_from_blob_name(blob.name)
        if dados_id:
            ids.add(dados_id)
    logger.info(
        "GCS webhook scan complete | bucket=%s object_prefix=%s scanned_objects=%s matched_ids=%s",
        bucket_name,
        object_prefix,
        scanned_objects,
        len(ids),
    )
    return ids


@retry(
    wait=wait_exponential(multiplier=2, min=2, max=30),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(Exception),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def fetch_existing_webhook_ids_with_retry(
    storage_client: storage.Client,
    bucket_name: str,
    prefix: str,
) -> Set[str]:
    """Retry wrapper for bucket scans.

    This protects long-running backfill cycles from transient auth/transport
    errors that can appear during GCS listing pagination.
    """
    return fetch_existing_webhook_ids(storage_client, bucket_name, prefix)


def upload_payload(
    storage_client: storage.Client,
    bucket_name: str,
    object_name: str,
    payload: dict,
    dry_run: bool,
) -> None:
    if dry_run:
        logger.info("[dry-run] would upload gs://%s/%s", bucket_name, object_name)
        return

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(json.dumps(payload, ensure_ascii=False), content_type="application/json")
    logger.info("uploaded gs://%s/%s", bucket_name, object_name)


def sleep_between_emits(interval_seconds: float, dry_run: bool) -> None:
    """Throttle object uploads to protect downstream Tiny API limits.

    In dry-run mode, sleep is skipped to keep feedback fast.
    """
    if dry_run:
        return
    if interval_seconds <= 0:
        return
    time.sleep(interval_seconds)


def reconcile_one_store(
    store_prefix: str,
    cfg: StoreConfig,
    storage_client: storage.Client,
    secret_client: secretmanager.SecretManagerServiceClient,
    store_cnpj: str,
    args: argparse.Namespace,
) -> None:
    bucket_name = resolve_bucket_name(store_prefix)
    token = get_api_token(secret_client, cfg.secret_path)

    logger.info(
        "Store reconciliation start | store_prefix=%s bucket=%s start=%s end=%s dry_run=%s emit_interval_seconds=%s",
        store_prefix,
        bucket_name,
        args.start_date,
        args.end_date,
        args.dry_run,
        args.emit_interval_seconds,
    )

    consecutive_zero = 0
    cycle = 0

    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)

    while True:
        cycle += 1
        logger.info("Cycle start | store_prefix=%s cycle=%s", store_prefix, cycle)

        pedidos = {
            pedido.pedido_id: pedido
            for pedido in iter_pedidos(
                cfg.base_url,
                token,
                start_date,
                end_date,
                args.max_pages,
                args.list_page_interval_seconds,
            )
        }
        logger.info(
            "Tiny listing complete | store_prefix=%s cycle=%s unique_pedidos=%s",
            store_prefix,
            cycle,
            len(pedidos),
        )
        pedidos = filter_recent_pedidos(pedidos, args.skip_recent_minutes)
        logger.info(
            "Post-filter pedidos count | store_prefix=%s cycle=%s skip_recent_minutes=%s eligible_pedidos=%s",
            store_prefix,
            cycle,
            args.skip_recent_minutes,
            len(pedidos),
        )

        try:
            existing_ids = fetch_existing_webhook_ids_with_retry(storage_client, bucket_name, store_prefix)
        except Exception as exc:
            logger.exception(
                "Failed to scan webhook bucket after retries | store_prefix=%s bucket=%s error=%s",
                store_prefix,
                bucket_name,
                exc,
            )
            if consecutive_zero > 0:
                logger.warning(
                    "Exiting gracefully after prior zero-missing cycle due to transient scan failure "
                    "| store_prefix=%s consecutive_zero=%s target=%s",
                    store_prefix,
                    consecutive_zero,
                    args.consecutive_zero_target,
                )
                break
            raise
        missing_ids = sorted(set(pedidos.keys()) - existing_ids)
        logger.info(
            "Diff computed | store_prefix=%s cycle=%s tiny_ids=%s bucket_ids=%s missing_ids=%s",
            store_prefix,
            cycle,
            len(pedidos),
            len(existing_ids),
            len(missing_ids),
        )
        if missing_ids:
            logger.info(
                "Missing ID sample | store_prefix=%s cycle=%s sample=%s",
                store_prefix,
                cycle,
                missing_ids[:10],
            )

        emitted = 0
        for pedido_id in missing_ids:
            pedido = pedidos[pedido_id]
            payload = build_webhook_payload(store_cnpj, pedido)
            object_name = build_filename(
                store_prefix,
                pedido,
                use_deterministic_uuid=not args.random_uuid,
            )
            upload_payload(storage_client, bucket_name, object_name, payload, args.dry_run)
            emitted += 1
            sleep_between_emits(args.emit_interval_seconds, args.dry_run)

            if args.max_orders and emitted >= args.max_orders:
                logger.warning(
                    "Stopping uploads early for cycle due to --max-orders=%s",
                    args.max_orders,
                )
                break

        stats = CycleStats(
            tiny_count=len(pedidos),
            bucket_count=len(existing_ids),
            missing_count=len(missing_ids),
            emitted_count=emitted,
        )
        logger.info(
            "Cycle summary | store_prefix=%s cycle=%s tiny_ids=%s bucket_ids=%s missing=%s emitted=%s",
            store_prefix,
            cycle,
            stats.tiny_count,
            stats.bucket_count,
            stats.missing_count,
            stats.emitted_count,
        )

        if args.dry_run and cycle >= args.dry_run_cycles:
            logger.info(
                "Dry-run cycle limit reached | store_prefix=%s cycles=%s dry_run_cycles=%s",
                store_prefix,
                cycle,
                args.dry_run_cycles,
            )
            break

        if stats.missing_count == 0:
            consecutive_zero += 1
            logger.info(
                "Zero-missing cycle achieved | store_prefix=%s cycle=%s consecutive_zero=%s target=%s",
                store_prefix,
                cycle,
                consecutive_zero,
                args.consecutive_zero_target,
            )
        else:
            consecutive_zero = 0

        if consecutive_zero >= args.consecutive_zero_target:
            logger.info(
                "Store reconciliation complete | store_prefix=%s cycles=%s",
                store_prefix,
                cycle,
            )
            break

        if args.max_cycles and cycle >= args.max_cycles:
            logger.warning(
                "Reached max cycles before convergence | store_prefix=%s cycles=%s",
                store_prefix,
                cycle,
            )
            break

        logger.info(
            "Waiting settle period before next cycle | store_prefix=%s minutes=%s",
            store_prefix,
            args.settle_minutes,
        )
        if args.dry_run:
            logger.info("Dry-run mode enabled; skipping settle sleep")
        else:
            time.sleep(args.settle_minutes * 60)


def resolve_store_list(store_configs: Dict[str, StoreConfig], store_prefix: Optional[str]) -> List[str]:
    if store_prefix:
        if store_prefix not in store_configs:
            raise ValueError(f"Unknown --store-prefix '{store_prefix}'. Available: {sorted(store_configs)}")
        return [store_prefix]
    return sorted(store_configs.keys())


def format_auth_troubleshooting_hint(exc: Exception) -> Optional[str]:
    """Return an actionable hint for common ADC/metadata credential failures."""
    message = str(exc)
    lowered = message.lower()
    if "compute_engine" in lowered or "metadata" in lowered or "'email'" in lowered:
        return (
            "Detected Google ADC metadata credential failure. In Cloud Shell run: "
            "`gcloud auth login`, `gcloud auth application-default login`, and "
            "`gcloud config set project <PROJECT_ID>`. Also ensure any "
            "`GOOGLE_APPLICATION_CREDENTIALS` path points to a valid key file "
            "or unset it if not needed."
        )
    return None


def run(args: argparse.Namespace) -> None:
    store_configs = load_store_configs()

    if args.start_date and args.end_date:
        start_date = parse_iso_date(args.start_date)
        end_date = parse_iso_date(args.end_date)
        if start_date > end_date:
            raise ValueError("--start-date must be <= --end-date")

    stores = resolve_store_list(store_configs, args.store_prefix)
    logger.info("Backfill run start | stores=%s", stores)

    secret_client = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    for store_prefix in stores:
        cfg = store_configs[store_prefix]
        store_cnpj = args.store_cnpj_map.get(store_prefix, "") if args.store_cnpj_map else ""
        try:
            reconcile_one_store(
                store_prefix=store_prefix,
                cfg=cfg,
                storage_client=storage_client,
                secret_client=secret_client,
                store_cnpj=store_cnpj,
                args=args,
            )
        except Exception as exc:
            hint = format_auth_troubleshooting_hint(exc)
            if hint:
                logger.error("%s", hint)
            raise

    logger.info("Backfill run complete")


def parse_cnpj_map(raw: Optional[str]) -> Dict[str, str]:
    if not raw:
        return {}
    mapping = json.loads(raw)
    if not isinstance(mapping, dict):
        raise ValueError("--store-cnpj-map must be a JSON object")
    return {str(k): str(v) for k, v in mapping.items()}


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store-prefix", help="Single store prefix to process (default: all stores sequentially)")
    parser.add_argument(
        "--start-date",
        help="Filter start date (YYYY-MM-DD). If omitted, scan from earliest available Tiny history.",
    )
    parser.add_argument(
        "--end-date",
        help="Filter end date (YYYY-MM-DD). If omitted, scan through latest available Tiny history.",
    )
    parser.add_argument("--max-pages", type=int, help="Stop Tiny page scanning after this many pages per cycle")
    parser.add_argument(
        "--list-page-interval-seconds",
        type=float,
        default=DEFAULT_LIST_PAGE_INTERVAL_SECONDS,
        help=(
            "Seconds to wait between Tiny pedidos.pesquisa page calls "
            f"(default: {DEFAULT_LIST_PAGE_INTERVAL_SECONDS})"
        ),
    )
    parser.add_argument("--max-orders", type=int, help="Stop uploads after this many missing orders per cycle")
    parser.add_argument(
        "--skip-recent-minutes",
        type=int,
        default=DEFAULT_SKIP_RECENT_MINUTES,
        help=f"Skip very recent Tiny pedidos (default: {DEFAULT_SKIP_RECENT_MINUTES})",
    )
    parser.add_argument(
        "--settle-minutes",
        type=int,
        default=DEFAULT_SETTLE_MINUTES,
        help=f"Minutes to wait between cycles (default: {DEFAULT_SETTLE_MINUTES})",
    )
    parser.add_argument(
        "--consecutive-zero-target",
        type=int,
        default=DEFAULT_CONSECUTIVE_ZERO_TARGET,
        help=f"Number of consecutive zero-missing cycles required (default: {DEFAULT_CONSECUTIVE_ZERO_TARGET})",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        help="Optional safety brake: stop after this many cycles even if not converged",
    )
    parser.add_argument(
        "--emit-interval-seconds",
        type=float,
        default=DEFAULT_EMIT_INTERVAL_SECONDS,
        help=f"Seconds to wait between webhook uploads (default: {DEFAULT_EMIT_INTERVAL_SECONDS})",
    )
    parser.add_argument(
        "--dry-run-cycles",
        type=int,
        default=DEFAULT_DRY_RUN_CYCLES,
        help=f"Max cycles when --dry-run is enabled (default: {DEFAULT_DRY_RUN_CYCLES})",
    )
    parser.add_argument(
        "--emit-rate-per-minute",
        type=float,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--random-uuid",
        action="store_true",
        help="Disable deterministic UUID and use random UUID4 in filenames",
    )
    parser.add_argument(
        "--store-cnpj-map",
        help="Optional JSON map prefix->cnpj for payload enrichment, e.g. '{\"z316\":\"229...\"}'",
    )
    parser.add_argument("--dry-run", action="store_true", help="Log actions without uploading files")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")

    args = parser.parse_args(argv)
    if args.dry_run_cycles <= 0:
        raise ValueError("--dry-run-cycles must be > 0")
    if args.list_page_interval_seconds < 0:
        raise ValueError("--list-page-interval-seconds must be >= 0")
    if args.emit_rate_per_minute is not None:
        if args.emit_rate_per_minute <= 0:
            raise ValueError("--emit-rate-per-minute must be > 0")
        args.emit_interval_seconds = 60.0 / args.emit_rate_per_minute
        logger.warning(
            "--emit-rate-per-minute is deprecated; converted to --emit-interval-seconds=%s",
            args.emit_interval_seconds,
        )
    args.store_cnpj_map = parse_cnpj_map(args.store_cnpj_map)
    return args


if __name__ == "__main__":
    cli_args = parse_args()
    setup_logging(cli_args.log_level)
    run(cli_args)
