from __future__ import annotations

from collections import defaultdict
from typing import Any

from cow_indexer.models import ImportStats
from cow_indexer.utils import (
    canonical_json,
    normalize_address,
    normalize_hash,
    normalize_order_uid,
    parse_datetime,
    sha256_json,
    utcnow,
    validate_order_uid,
)

SUPPORTED_DATASETS = {
    "orders",
    "order_events",
    "auctions",
    "auction_orders",
    "solver_competitions",
    "trades",
    "app_data",
    "quotes",
}


def _get(row: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in row and row[name] is not None:
            return row[name]
    return default


def _order(row: dict[str, Any], manifest: Any, now) -> tuple[dict[str, Any], str]:
    owner = normalize_address(_get(row, "owner"))
    valid_to = int(_get(row, "valid_to", "validTo"))
    uid = validate_order_uid(_get(row, "order_uid", "uid"), owner, valid_to)
    app_data = _get(row, "app_data_hash", "appDataHash", default="")
    if app_data:
        app_data = normalize_hash(app_data)
    immutable = {
        "uid": uid,
        "owner": owner,
        "sellToken": normalize_address(_get(row, "sell_token", "sellToken")),
        "buyToken": normalize_address(_get(row, "buy_token", "buyToken")),
        "receiver": normalize_address(_get(row, "receiver")) if _get(row, "receiver") else None,
        "sellAmount": str(_get(row, "sell_amount", "sellAmount")),
        "buyAmount": str(_get(row, "buy_amount", "buyAmount")),
        "validTo": valid_to,
        "appDataHash": app_data,
        "kind": _get(row, "kind"),
        "partiallyFillable": bool(_get(row, "partially_fillable", "partiallyFillable")),
    }
    immutable_hash = sha256_json(immutable)
    return {
        "environment": manifest.environment,
        "chain_id": manifest.chain_id,
        "order_uid": uid,
        "owner": owner,
        "sell_token": immutable["sellToken"],
        "buy_token": immutable["buyToken"],
        "receiver": immutable["receiver"],
        "sell_amount": int(immutable["sellAmount"]),
        "buy_amount": int(immutable["buyAmount"]),
        "valid_to": valid_to,
        "app_data_hash": app_data,
        "fee_amount": int(_get(row, "fee_amount", "feeAmount", default=0)),
        "kind": str(immutable["kind"]),
        "partially_fillable": immutable["partiallyFillable"],
        "sell_token_balance": str(
            _get(row, "sell_token_balance", "sellTokenBalance", default="erc20")
        ),
        "buy_token_balance": str(
            _get(row, "buy_token_balance", "buyTokenBalance", default="erc20")
        ),
        "signing_scheme": str(_get(row, "signing_scheme", "signingScheme", default="")),
        "signature": str(_get(row, "signature", default="")),
        "creation_date": parse_datetime(_get(row, "creation_date", "creationDate"))
        or manifest.snapshot_at,
        "status": str(_get(row, "status", default="unknown")),
        "class": str(_get(row, "class", default="unknown")),
        "executed_sell_amount": int(
            _get(row, "executed_sell_amount", "executedSellAmount", default=0)
        ),
        "executed_buy_amount": int(
            _get(row, "executed_buy_amount", "executedBuyAmount", default=0)
        ),
        "executed_fee_amount": int(
            _get(row, "executed_fee_amount", "executedFeeAmount", default=0)
        ),
        "immutable_hash": immutable_hash,
        "source": "export",
        "raw_payload": canonical_json(row),
        "source_updated_at": parse_datetime(_get(row, "source_updated_at", "updated_at"))
        or manifest.snapshot_at,
        "observed_at": now,
    }, immutable_hash


async def normalize_export_rows(
    store: Any,
    manifest: Any,
    dataset: str,
    rows: list[dict[str, Any]],
    bundle_id: str,
) -> tuple[ImportStats, dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    if dataset not in SUPPORTED_DATASETS:
        raise ValueError(f"unsupported export dataset: {dataset}")
    stats = ImportStats()
    output: dict[str, list[dict[str, Any]]] = defaultdict(list)
    conflicts: list[dict[str, Any]] = []
    now = utcnow()

    for row in rows:
        try:
            if dataset == "orders":
                normalized, immutable_hash = _order(row, manifest, now)
                existing = await store.order_immutable_hash(
                    manifest.environment, manifest.chain_id, normalized["order_uid"]
                )
                if existing == immutable_hash:
                    stats.duplicates += 1
                    continue
                if existing is not None:
                    stats.conflicts += 1
                    conflicts.append(
                        {
                            "bundle_id": bundle_id,
                            "environment": manifest.environment,
                            "chain_id": manifest.chain_id,
                            "dataset": dataset,
                            "record_key": normalized["order_uid"],
                            "conflict_type": "immutable_order_mismatch",
                            "existing_payload": canonical_json({"immutable_hash": existing}),
                            "incoming_payload": canonical_json(row),
                            "detected_at": now,
                        }
                    )
                    continue
                output["orders"].append(normalized)
            elif dataset == "order_events":
                event_id = str(_get(row, "event_id", default=sha256_json(row)))
                output["order_events"].append(
                    {
                        "event_id": event_id,
                        "environment": manifest.environment,
                        "chain_id": manifest.chain_id,
                        "order_uid": normalize_order_uid(_get(row, "order_uid"))
                        if _get(row, "order_uid")
                        else "",
                        "owner": normalize_address(_get(row, "owner"))
                        if _get(row, "owner")
                        else "",
                        "event_type": str(_get(row, "event_type", "type")),
                        "source": "export",
                        "block_number": int(_get(row, "block_number"))
                        if _get(row, "block_number") is not None
                        else None,
                        "transaction_hash": normalize_hash(_get(row, "transaction_hash"))
                        if _get(row, "transaction_hash")
                        else "",
                        "log_index": int(_get(row, "log_index"))
                        if _get(row, "log_index") is not None
                        else None,
                        "event_timestamp": parse_datetime(
                            _get(row, "event_timestamp", "timestamp")
                        ),
                        "payload": canonical_json(row),
                        "observed_at": now,
                    }
                )
            elif dataset == "auctions":
                output["auctions"].append(
                    {
                        "environment": manifest.environment,
                        "chain_id": manifest.chain_id,
                        "auction_id": int(_get(row, "auction_id", "id")),
                        "block_number": int(_get(row, "block_number", "block", default=0)),
                        "deadline": parse_datetime(_get(row, "deadline")),
                        "source": "export",
                        "payload": canonical_json(row),
                        "observed_at": now,
                    }
                )
            elif dataset == "auction_orders":
                output["auction_orders"].append(
                    {
                        "environment": manifest.environment,
                        "chain_id": manifest.chain_id,
                        "auction_id": int(_get(row, "auction_id")),
                        "order_uid": normalize_order_uid(_get(row, "order_uid", "uid")),
                        "payload": canonical_json(row),
                        "observed_at": now,
                    }
                )
            elif dataset == "solver_competitions":
                tx_hash = _get(row, "tx_hash", "transaction_hash", default="")
                winner = _get(row, "winner", default="")
                output["solver_competitions"].append(
                    {
                        "environment": manifest.environment,
                        "chain_id": manifest.chain_id,
                        "auction_id": int(_get(row, "auction_id")),
                        "tx_hash": normalize_hash(tx_hash) if tx_hash else "",
                        "winner": normalize_address(winner) if winner else "",
                        "reference_score": str(_get(row, "reference_score", default="")),
                        "auction_block": int(_get(row, "auction_block", default=0)),
                        "source": "export",
                        "raw_payload": canonical_json(row),
                        "observed_at": now,
                    }
                )
            elif dataset == "trades":
                output["trades"].append(
                    {
                        "environment": manifest.environment,
                        "chain_id": manifest.chain_id,
                        "order_uid": normalize_order_uid(_get(row, "order_uid")),
                        "tx_hash": normalize_hash(_get(row, "tx_hash", "transaction_hash")),
                        "log_index": int(_get(row, "log_index", default=0)),
                        "block_number": int(_get(row, "block_number", default=0)),
                        "block_hash": normalize_hash(_get(row, "block_hash"))
                        if _get(row, "block_hash")
                        else "",
                        "block_timestamp": parse_datetime(_get(row, "block_timestamp")),
                        "owner": normalize_address(_get(row, "owner")),
                        "sell_token": normalize_address(_get(row, "sell_token")),
                        "buy_token": normalize_address(_get(row, "buy_token")),
                        "sell_amount": int(_get(row, "sell_amount")),
                        "buy_amount": int(_get(row, "buy_amount")),
                        "fee_amount": int(_get(row, "fee_amount", default=0)),
                        "source": "export",
                        "raw_payload": canonical_json(row),
                        "observed_at": now,
                    }
                )
            elif dataset == "app_data":
                output["app_data"].append(
                    {
                        "environment": manifest.environment,
                        "chain_id": manifest.chain_id,
                        "app_data_hash": normalize_hash(_get(row, "app_data_hash")),
                        "full_app_data": canonical_json(_get(row, "full_app_data", "app_data")),
                        "source": "export",
                        "observed_at": now,
                    }
                )
            elif dataset == "quotes":
                output["quotes"].append(
                    {
                        "environment": manifest.environment,
                        "chain_id": manifest.chain_id,
                        "quote_id": str(_get(row, "quote_id", "id")),
                        "owner": normalize_address(_get(row, "owner"))
                        if _get(row, "owner")
                        else "",
                        "sell_token": normalize_address(_get(row, "sell_token")),
                        "buy_token": normalize_address(_get(row, "buy_token")),
                        "sell_amount": int(_get(row, "sell_amount", default=0)),
                        "buy_amount": int(_get(row, "buy_amount", default=0)),
                        "fee_amount": int(_get(row, "fee_amount", default=0)),
                        "payload": canonical_json(row),
                        "source": "export",
                        "created_at": parse_datetime(_get(row, "created_at"))
                        or manifest.snapshot_at,
                        "observed_at": now,
                    }
                )
            stats.accepted += 1
        except (KeyError, TypeError, ValueError):
            stats.rejected += 1
    return stats, dict(output), conflicts
