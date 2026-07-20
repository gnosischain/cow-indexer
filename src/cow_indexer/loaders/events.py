from __future__ import annotations

from typing import Any

from cow_indexer.config import ChainConfig
from cow_indexer.models import DecodedEvent
from cow_indexer.storage.base import Storage

WorkKey = tuple[str, str, dict[str, Any] | None]


def _work_for_event(event: DecodedEvent) -> list[WorkKey]:
    """Deterministic API-enrichment work implied by a single decoded event."""
    args = event.args
    name = event.event_name
    work: list[WorkKey] = []
    if name == "Trade":
        work.append(("order_uid", args["orderUid"], None))
        work.append(("owner", args["owner"], None))
        work.append(("tx_hash", event.transaction_hash, None))
        work.append(("token", args["sellToken"], None))
        work.append(("token", args["buyToken"], None))
    elif name == "Settlement":
        work.append(("tx_hash", event.transaction_hash, None))
        # Solver competition for this settlement transaction (its own work kind so a
        # public-API 404 can be classified terminally without failing order lookups).
        work.append(("tx_competition", event.transaction_hash, None))
    elif name in {"OrderInvalidated", "OrderInvalidation", "OrderRefund"}:
        work.append(("order_uid", args["orderUid"], None))
        if owner := args.get("owner"):
            work.append(("owner", owner, None))
    elif name == "PreSignature":
        work.append(("order_uid", args["orderUid"], None))
        work.append(("owner", args["owner"], None))
    elif name == "OrderPlacement":
        work.append(("tx_hash", event.transaction_hash, None))
        work.append(("owner", args["sender"], None))
        order = args["order"]
        work.append(("token", order["sellToken"], None))
        work.append(("token", order["buyToken"], None))
        work.append(("app_data", order["appData"], None))
    elif name in {"ConditionalOrderCreated", "MerkleRootSet", "SwapGuardSet"}:
        work.append(("owner", args["owner"], None))
    return work


class EventProcessor:
    def __init__(self, store: Storage, chain: ChainConfig) -> None:
        self.store = store
        self.chain = chain

    async def process(self, event: DecodedEvent) -> None:
        await self.process_many([event])

    async def process_many(self, events: list[DecodedEvent]) -> None:
        """Persist a whole scanned range at once: one INSERT per event table and one
        INSERT for the derived work queue, instead of per-log round-trips."""
        if not events:
            return
        await self.store.store_events(events)
        work: list[WorkKey] = []
        for event in events:
            work.extend(_work_for_event(event))
        if work:
            await self.store.enqueue_work_many(self.chain, work)
