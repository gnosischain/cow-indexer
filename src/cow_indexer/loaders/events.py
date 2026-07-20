from __future__ import annotations

from typing import Any

from cow_indexer.config import ChainConfig
from cow_indexer.models import DecodedEvent
from cow_indexer.storage.base import Storage


class EventProcessor:
    def __init__(self, store: Storage, chain: ChainConfig) -> None:
        self.store = store
        self.chain = chain

    async def process(self, event: DecodedEvent) -> None:
        await self.store.store_event(event)
        args = event.args
        event_name = event.event_name

        if event_name == "Trade":
            await self._enqueue("order_uid", args["orderUid"])
            await self._enqueue("owner", args["owner"])
            await self._enqueue("tx_hash", event.transaction_hash)
            await self._enqueue("token", args["sellToken"])
            await self._enqueue("token", args["buyToken"])
        elif event_name == "Settlement":
            await self._enqueue("tx_hash", event.transaction_hash)
        elif event_name in {"OrderInvalidated", "OrderInvalidation", "OrderRefund"}:
            await self._enqueue("order_uid", args["orderUid"])
            if owner := args.get("owner"):
                await self._enqueue("owner", owner)
        elif event_name == "PreSignature":
            await self._enqueue("order_uid", args["orderUid"])
            await self._enqueue("owner", args["owner"])
        elif event_name == "OrderPlacement":
            await self._enqueue("tx_hash", event.transaction_hash)
            await self._enqueue("owner", args["sender"])
            order = args["order"]
            await self._enqueue("token", order["sellToken"])
            await self._enqueue("token", order["buyToken"])
            await self._enqueue("app_data", order["appData"])
        elif event_name in {"ConditionalOrderCreated", "MerkleRootSet", "SwapGuardSet"}:
            await self._enqueue("owner", args["owner"])

    async def _enqueue(self, kind: str, key: str, payload: dict[str, Any] | None = None) -> None:
        await self.store.enqueue_work(self.chain, kind, key, payload)
