from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import structlog

from cow_indexer.config import ChainConfig, RuntimeConfig
from cow_indexer.models import WorkItem
from cow_indexer.sources.cow_api import CompetitionUnavailable, CowApiClient
from cow_indexer.storage.base import Storage
from cow_indexer.utils import normalize_auction_order, normalize_order_uid, utcnow

log = structlog.get_logger()


class EnrichmentService:
    def __init__(
        self,
        chain: ChainConfig,
        api: CowApiClient,
        store: Storage,
        runtime: RuntimeConfig,
        concurrency: int = 8,
    ) -> None:
        self.chain = chain
        self.api = api
        self.store = store
        self.runtime = runtime
        self.concurrency = concurrency

    async def run_once(self, limit: int = 20) -> int:
        items = await self.store.lease_work(self.chain, self.runtime.worker_id, limit)
        if not items:
            return 0
        # Fetch the base orders for every leased order_uid item in one batched
        # request (get_orders_by_uids chunks at 128) instead of one call per item.
        prefetched = await self._prefetch_orders(
            [item.key for item in items if item.kind == "order_uid"]
        )
        semaphore = asyncio.Semaphore(self.concurrency)

        async def guarded(item: WorkItem) -> None:
            async with semaphore:
                await self._process_and_finish(item, prefetched)

        await asyncio.gather(*(guarded(item) for item in items))
        return len(items)

    async def _prefetch_orders(self, keys: list[str]) -> dict[str, dict[str, Any]] | None:
        """Batch-fetch base orders for leased order_uid items. Returns a uid->order
        map on success (absent uid == the order does not exist, like a 404), or None
        if the batch call fails so each item falls back to a per-item fetch and its
        own retry/terminal handling."""
        if not keys:
            return {}
        try:
            orders = await self.api.get_orders_by_uids(keys)
        except Exception:
            return None
        result: dict[str, dict[str, Any]] = {}
        for order in orders:
            uid = order.get("uid")
            if not uid:
                continue
            try:
                result[normalize_order_uid(uid)] = order
            except ValueError:
                continue
        return result

    async def _resolve_order(
        self, key: str, prefetched: dict[str, dict[str, Any]] | None
    ) -> dict[str, Any] | None:
        if prefetched is None:
            # Batch prefetch failed: fall back to a per-item fetch so this item's own
            # error (if any) drives its retry/dead-letter path.
            return await self.api.get_order(key)
        return prefetched.get(normalize_order_uid(key))

    async def _process_and_finish(
        self, item: WorkItem, prefetched: dict[str, dict[str, Any]] | None = None
    ) -> None:
        try:
            await self.process(item, prefetched)
            await self.store.finish_work(item, True)
        except CompetitionUnavailable as exc:
            # Not an error: the public API does not serve this competition. Record a
            # terminal, non-retried classification instead of dead-lettering it.
            await self.store.finish_work(
                item, False, error=str(exc), terminal="unavailable_from_public_api"
            )
            log.info(
                "competition_unavailable",
                chain=self.chain.key,
                kind=item.kind,
                key=item.key,
            )
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            if item.attempts >= self.runtime.max_attempts:
                await self.store.finish_work(item, False, error)
            else:
                delay = min(3600, 2**item.attempts)
                await self.store.finish_work(
                    item, False, error, retry_at=utcnow() + timedelta(seconds=delay)
                )
            log.warning(
                "enrichment_failed",
                chain=self.chain.key,
                kind=item.kind,
                key=item.key,
                attempts=item.attempts,
                error=error,
            )

    async def process(
        self, item: WorkItem, prefetched: dict[str, dict[str, Any]] | None = None
    ) -> None:
        if item.kind == "order_uid":
            order = await self._resolve_order(item.key, prefetched)
            await self.store.store_api_payload(self.chain, "order", item.key, order)
            if order:
                await self.store.store_orders(self.chain, [order], "api")
                await self._fanout_order(order)
            async for trades in self.api.iter_trades(order_uid=item.key):
                await self.store.store_api_payload(self.chain, "trades:order", item.key, trades)
                await self.store.store_api_trades(self.chain, trades, "api")
            status = await self.api.get_order_status(item.key)
            await self.store.store_api_payload(self.chain, "order_status", item.key, status)
        elif item.kind == "owner":
            async for orders in self.api.iter_account_orders(item.key):
                await self.store.store_api_payload(self.chain, "account_orders", item.key, orders)
                await self.store.store_orders(self.chain, orders, "api")
                for order in orders:
                    await self._fanout_order(order, include_owner=False)
            async for trades in self.api.iter_trades(owner=item.key):
                await self.store.store_api_payload(self.chain, "trades:owner", item.key, trades)
                await self.store.store_api_trades(self.chain, trades, "api")
        elif item.kind == "tx_hash":
            orders = await self.api.get_orders_by_transaction(item.key)
            await self.store.store_api_payload(self.chain, "transaction_orders", item.key, orders)
            await self.store.store_orders(self.chain, orders, "api")
            for order in orders:
                await self._fanout_order(order)
        elif item.kind == "tx_competition":
            # Raises CompetitionUnavailable on a 404, handled terminally upstream.
            competition = await self.api.competition_by_transaction(item.key)
            await self.store.store_api_payload(self.chain, "competition:tx", item.key, competition)
            await self.store.store_competition(self.chain, competition, "api")
            await self._fanout_competition(competition)
        elif item.kind == "app_data":
            payload = await self.api.app_data(item.key)
            await self.store.store_api_payload(self.chain, "app_data", item.key, payload)
            if payload:
                await self.store.store_app_data(self.chain, item.key, payload, "api")
        elif item.kind == "token":
            payload = await self.api.native_price(item.key)
            await self.store.store_api_payload(self.chain, "native_price", item.key, payload)
            if payload:
                await self.store.store_native_price(self.chain, item.key, payload, "api")
        else:
            raise ValueError(f"unsupported work kind: {item.kind}")

    async def _fanout_order(self, order: dict[str, Any], include_owner: bool = True) -> None:
        if uid := order.get("uid"):
            await self.store.enqueue_work(self.chain, "order_uid", uid)
        if include_owner and (owner := order.get("owner")):
            await self.store.enqueue_work(self.chain, "owner", owner)
        for key in ("sellToken", "buyToken"):
            if token := order.get(key):
                await self.store.enqueue_work(self.chain, "token", token)
        app_data = order.get("appDataHash") or order.get("appData")
        if isinstance(app_data, str) and len(app_data) == 66:
            await self.store.enqueue_work(self.chain, "app_data", app_data)

    async def _fanout_competition(self, payload: dict[str, Any]) -> None:
        auction = payload.get("auction") or {}
        # auction.orders may be bare UID strings or expanded order objects; normalize
        # both (order.get() on a str raises 'str' object has no attribute 'get').
        for order in auction.get("orders", []):
            normalized = normalize_auction_order(order)
            if normalized:
                await self.store.enqueue_work(self.chain, "order_uid", normalized[0])
