"""Shared identity and normalization helpers for validation/remediation workflows.

Chunk 1 scope:
- Central event identity contract.
- Strict webhook filename parsing.
- Payload extraction helpers for webhook, PDV and Pesquisa payloads.
- Canonical normalization utilities for UUIDs, IDs and timestamps.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

WEBHOOK_FILENAME_PATTERN = re.compile(
    r"^vendas/(?P<prefix>.+?)-tiny-webhook-vendas-(?P<dados_id>\d+)-"
    r"(?P<timestamp>\d{8}T\d{6})-(?P<uuid>[0-9a-fA-F-]{36})\.json$"
)


class IdentityValidationError(ValueError):
    """Raised when identity data cannot be parsed or normalized strictly."""


@dataclass(frozen=True)
class EventIdentity:
    """Canonical identity tracked across all pipeline stages."""

    store_prefix: str
    uuid: str
    dados_id: str
    pedido_id: Optional[str]
    event_timestamp: str


@dataclass(frozen=True)
class StageRecord:
    """Per-stage existence/result record linked to one EventIdentity."""

    identity: EventIdentity
    stage_name: str
    exists: bool
    metadata: Dict[str, str]


def normalize_uuid(value: Any) -> str:
    """Return a canonical lowercase UUID string or raise IdentityValidationError."""

    if value in (None, ""):
        raise IdentityValidationError("UUID is required")
    try:
        return str(uuid.UUID(str(value))).lower()
    except (ValueError, AttributeError, TypeError) as exc:
        raise IdentityValidationError(f"Invalid UUID: {value}") from exc


def normalize_id(value: Any, *, field_name: str) -> str:
    """Normalize identifier values to non-empty strings."""

    if value in (None, ""):
        raise IdentityValidationError(f"{field_name} is required")
    normalized = str(value).strip()
    if not normalized:
        raise IdentityValidationError(f"{field_name} is required")
    return normalized


def normalize_optional_id(value: Any) -> Optional[str]:
    """Normalize optional identifier values to stripped strings or None."""

    if value in (None, ""):
        return None
    normalized = str(value).strip()
    return normalized or None


def normalize_timestamp(value: Any) -> str:
    """Normalize supported timestamp formats to ISO-8601 UTC (Z suffix)."""

    if value in (None, ""):
        raise IdentityValidationError("event_timestamp is required")

    if isinstance(value, datetime):
        dt = value
    else:
        raw = str(value).strip()
        if not raw:
            raise IdentityValidationError("event_timestamp is required")

        if re.fullmatch(r"\d{8}T\d{6}", raw):
            dt = datetime.strptime(raw, "%Y%m%dT%H%M%S")
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            try:
                # Accept both "...Z" and full ISO values.
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError as exc:
                raise IdentityValidationError(f"Invalid timestamp: {value}") from exc

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.isoformat().replace("+00:00", "Z")


def parse_webhook_filename(file_name: str) -> Dict[str, str]:
    """Parse webhook filename and return normalized identity components strictly."""

    match = WEBHOOK_FILENAME_PATTERN.match(file_name)
    if not match:
        raise IdentityValidationError(
            f"Webhook filename does not match expected format: {file_name}"
        )

    prefix = normalize_id(match.group("prefix"), field_name="store_prefix")
    dados_id = normalize_id(match.group("dados_id"), field_name="dados_id")
    timestamp = normalize_timestamp(match.group("timestamp"))
    uuid_value = normalize_uuid(match.group("uuid"))

    return {
        "store_prefix": prefix,
        "dados_id": dados_id,
        "event_timestamp": timestamp,
        "uuid": uuid_value,
    }


def extract_pedido_id_from_pdv_payload(pdv_payload: Dict[str, Any]) -> Optional[str]:
    """Extract pedido id from pdv payload if available."""

    pedido = pdv_payload.get("retorno", {}).get("pedido", {})
    return normalize_optional_id(pedido.get("id"))


def extract_pedido_ids_from_pesquisa_payload(
    pesquisa_payload: Dict[str, Any],
) -> set[str]:
    """Extract all pedido IDs present in pedidos.pesquisa payload."""

    pedidos = pesquisa_payload.get("retorno", {}).get("pedidos", [])
    ids: set[str] = set()

    if not isinstance(pedidos, Iterable):
        return ids

    for item in pedidos:
        if not isinstance(item, dict):
            continue
        pedido = item.get("pedido", {})
        if not isinstance(pedido, dict):
            continue
        pedido_id = normalize_optional_id(pedido.get("id"))
        if pedido_id:
            ids.add(pedido_id)

    return ids


def build_identity_from_webhook(
    file_name: str,
    payload: Dict[str, Any],
) -> EventIdentity:
    """Build EventIdentity from webhook filename + payload fields."""

    from_file = parse_webhook_filename(file_name)

    payload_dados_id = normalize_id(
        payload.get("dados", {}).get("id"),
        field_name="payload.dados.id",
    )
    if payload_dados_id != from_file["dados_id"]:
        raise IdentityValidationError(
            "Mismatch between filename dados_id and payload.dados.id: "
            f"{from_file['dados_id']} != {payload_dados_id}"
        )

    pedido_id = normalize_optional_id(payload.get("dados", {}).get("idPedido"))

    return EventIdentity(
        store_prefix=from_file["store_prefix"],
        uuid=from_file["uuid"],
        dados_id=from_file["dados_id"],
        pedido_id=pedido_id,
        event_timestamp=from_file["event_timestamp"],
    )
