from __future__ import annotations

from datetime import timedelta
from typing import Any

import structlog

from cow_indexer.config import ChainConfig, RuntimeConfig
from cow_indexer.models import WorkItem
from cow_indexer.sources.cow_api import CowApiClient
from cow_indexer.storage.base import Storage
from cow_indexer.utils import utcnow

log = structlog.get_logger()


class EnrichmentService:
    def __init__(
        self,
        chain: ChainConfig,
        api: CowApiClient,
        store: Storage,
        runtime: RuntimeConfig,
    ) -> None:
        self.chain = chain
        self.api = api
        self.store = store
        self.runtime = runtime

    async def run_once(self, limit: int = 20) -> int:
        items = await self.store.lease_work(self.chain, self.runtime.worker_id, limit)
        for item in items:
            try:
                await self.process(item)
                await self.store.finish_work(item, True)
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
        return len(items)

    async def process(self, item: WorkItem) -> None:
        if item.kind == "order_uid":
            order = await self.api.get_order(item.key)
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
            competition = await self.api.competition_by_transaction(item.key)
            await self.store.store_api_payload(self.chain, "competition:tx", item.key, competition)
            if competition:
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
        for order in auction.get("orders", []):
            if uid := order.get("uid") or order.get("orderUid"):
                await self.store.enqueue_work(self.chain, "order_uid", uid)
