from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any


def utcnow() -> datetime:
    return datetime.now(UTC)


def normalize_hex(value: str, byte_length: int | None = None) -> str:
    normalized = value.lower()
    if not normalized.startswith("0x"):
        normalized = f"0x{normalized}"
    payload = normalized[2:]
    if any(character not in "0123456789abcdef" for character in payload):
        raise ValueError(f"not hexadecimal: {value!r}")
    if byte_length is not None and len(payload) != byte_length * 2:
        raise ValueError(f"expected {byte_length} bytes, got {len(payload) // 2}")
    return normalized


def normalize_address(value: str) -> str:
    return normalize_hex(value, 20)


def normalize_hash(value: str) -> str:
    return normalize_hex(value, 32)


def normalize_order_uid(value: str) -> str:
    return normalize_hex(value, 56)


def normalize_auction_order(value: Any) -> tuple[str, dict[str, Any]] | None:
    """Normalize API auction orders, which may be UIDs or expanded order objects."""
    if isinstance(value, str):
        uid = value
        payload: dict[str, Any] = {"uid": value}
    elif isinstance(value, dict):
        uid = value.get("uid") or value.get("orderUid")
        payload = value
    else:
        return None
    if not isinstance(uid, str):
        return None
    try:
        return normalize_order_uid(uid), payload
    except ValueError:
        return None


def validate_order_uid(value: str, owner: str | None = None, valid_to: int | None = None) -> str:
    uid = normalize_order_uid(value)
    raw = bytes.fromhex(uid[2:])
    if owner is not None and raw[32:52] != bytes.fromhex(normalize_address(owner)[2:]):
        raise ValueError("order UID owner suffix does not match owner")
    if valid_to is not None and int.from_bytes(raw[52:56], "big") != valid_to:
        raise ValueError("order UID validTo suffix does not match valid_to")
    return uid


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=json_default)


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, memoryview):
        return f"0x{bytes(value).hex()}"
    if isinstance(value, bytes):
        return f"0x{value.hex()}"
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def batched[T](values: Iterable[T], size: int) -> Iterator[list[T]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    batch: list[T] = []
    for value in values:
        batch.append(value)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def parse_datetime(value: str | datetime | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)
