"""Enrichment worker: order_uid items share one batched get_orders_by_uids call,
while per-item trades/status and the retry/terminal state machine stay intact."""

from __future__ import annotations

from pathlib import Path

import pytest

from cow_indexer.config import RuntimeConfig, load_config
from cow_indexer.models import WorkItem
from cow_indexer.services.enrichment import EnrichmentService
from cow_indexer.sources.cow_api import CowApiError

ROOT = Path(__file__).parents[2]
OWNER = "0x" + "11" * 20


def uid(index: int) -> str:
    return "0x" + index.to_bytes(56, "big").hex()


def work(kind: str, key: str) -> WorkItem:
    return WorkItem(work_id="w-" + key, environment="production", chain_id=100, kind=kind, key=key)


class FakeStore:
    def __init__(self, items: list[WorkItem]) -> None:
        self._items = list(items)
        self.finished: list[tuple[str, bool, str | None]] = []
        self.stored_orders: list[str] = []
        self.released: list[str] = []

    async def lease_work(self, chain, worker, limit):
        leased, self._items = self._items[:limit], self._items[limit:]
        return leased

    async def finish_work(self, item, success, error=None, retry_at=None, terminal=None):
        self.finished.append((item.key, success, terminal))

    async def store_api_payload(self, chain, endpoint, key, payload):
        pass

    async def store_orders(self, chain, rows, source):
        self.stored_orders.extend(row.get("uid") for row in rows)

    async def store_api_trades(self, chain, rows, source):
        pass

    async def enqueue_work(self, chain, kind, key, payload=None):
        pass

    async def release_work(self, items):
        self.released.extend(item.key for item in items)


class FakeApi:
    def __init__(self, *, fail_batch: bool = False, present: set[str] | None = None) -> None:
        self.by_uids_calls: list[list[str]] = []
        self.get_order_calls: list[str] = []
        self.fail_batch = fail_batch
        self.present = present  # None -> every requested uid exists

    async def get_orders_by_uids(self, uids):
        uids = list(uids)
        self.by_uids_calls.append(uids)
        if self.fail_batch:
            raise CowApiError(500, "/api/v1/orders/by_uids", "boom")
        return [{"uid": u, "owner": OWNER} for u in uids if self.present is None or u in self.present]

    async def get_order(self, key):
        self.get_order_calls.append(key)
        return {"uid": key, "owner": OWNER}

    async def iter_trades(self, *, owner=None, order_uid=None, limit=1000):
        for _ in ():
            yield {}

    async def get_order_status(self, key):
        return {"status": "open"}


def service(store: FakeStore, api: FakeApi) -> EnrichmentService:
    chain = load_config(ROOT / "config" / "chains.yaml").select("sepolia")[0]
    return EnrichmentService(chain, api, store, RuntimeConfig())


@pytest.mark.asyncio
async def test_order_uid_items_use_single_batched_fetch() -> None:
    store = FakeStore([work("order_uid", uid(i)) for i in range(3)])
    api = FakeApi()
    n = await service(store, api).run_once(limit=10)
    assert n == 3
    assert len(api.by_uids_calls) == 1
    assert sorted(api.by_uids_calls[0]) == sorted(uid(i) for i in range(3))
    assert api.get_order_calls == []  # no per-item base-order fetch
    assert sorted(store.stored_orders) == sorted(uid(i) for i in range(3))
    assert [ok for _, ok, _ in store.finished] == [True, True, True]


@pytest.mark.asyncio
async def test_batch_failure_falls_back_to_per_item_fetch() -> None:
    store = FakeStore([work("order_uid", uid(i)) for i in range(3)])
    api = FakeApi(fail_batch=True)
    n = await service(store, api).run_once(limit=10)
    assert n == 3
    assert len(api.by_uids_calls) == 1  # attempted once
    assert sorted(api.get_order_calls) == sorted(uid(i) for i in range(3))  # then per item
    assert sorted(store.stored_orders) == sorted(uid(i) for i in range(3))
    assert all(ok for _, ok, _ in store.finished)


@pytest.mark.asyncio
async def test_missing_order_in_batch_resolves_to_none() -> None:
    store = FakeStore([work("order_uid", uid(1)), work("order_uid", uid(2))])
    api = FakeApi(present={uid(1)})  # uid(2) is not returned by the batch
    n = await service(store, api).run_once(limit=10)
    assert n == 2
    assert api.get_order_calls == []  # batch succeeded, so no per-item fallback
    assert store.stored_orders == [uid(1)]  # only the existing order is stored
    assert all(ok for _, ok, _ in store.finished)  # both still finish successfully


@pytest.mark.asyncio
async def test_slow_item_cannot_wedge_the_batch() -> None:
    """A hanging item must not hold run_once past the deadline.

    Regression for the 2026-09-16 live-ingestion stall: `process()` makes serial
    per-item calls (iter_trades, get_order_status) that can each hit the transport's
    30s timeout and then six retries backing off to 30s. With no ceiling, a few such
    items held the gather for minutes, and because the caller cannot lease again until
    run_once returns, that chain's ingestion stopped -- without raising, so
    _resilient_loop never logged and nothing alerted.
    """
    import asyncio

    class HangingApi(FakeApi):
        async def get_order_status(self, key):
            if key == uid(0):
                await asyncio.sleep(30)  # never completes within the deadline
            return {"status": "open"}

    store = FakeStore([work("order_uid", uid(i)) for i in range(3)])
    chain = load_config(ROOT / "config" / "chains.yaml").select("sepolia")[0]
    runtime = RuntimeConfig(enrich_batch_timeout_seconds=0.25, enrich_concurrency=3)
    svc = EnrichmentService(chain, HangingApi(), store, runtime)

    started = asyncio.get_running_loop().time()
    done = await svc.run_once(limit=10)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 5, "run_once must return at the deadline, not wait for the hang"
    assert done == 2, "the two healthy items still completed"
    assert store.released == [uid(0)], "the stranded item is released, not abandoned"
    # Released, not finished: finish_work would record the burned attempt and six such
    # rounds would dead-letter an item nothing ever actually processed.
    assert uid(0) not in [key for key, _, _ in store.finished]


@pytest.mark.asyncio
async def test_healthy_batch_reports_no_release() -> None:
    store = FakeStore([work("order_uid", uid(i)) for i in range(3)])
    done = await service(store, FakeApi()).run_once(limit=10)
    assert done == 3
    assert store.released == []


@pytest.mark.asyncio
async def test_token_miss_is_recorded_so_it_is_not_re_asked() -> None:
    """A 404 price must be written to the negative cache.

    native_price uses allow_404, so a miss returns None with no exception and
    store_native_price is never called. tokens_with_fresh_price reads native_prices,
    which is only written on success -- so an unrecorded miss is never "fresh" and the
    sweep re-asks it every price_interval_seconds (900s) forever. Measured 2026-09-16:
    that path was 42-46% of ALL CoW API traffic at a ~65-72% 404 rate.
    """

    class NoPriceApi(FakeApi):
        async def native_price(self, token):
            return None

    class TokenStore(FakeStore):
        def __init__(self, items):
            super().__init__(items)
            self.price_misses: list[str] = []
            self.prices: list[str] = []

        async def store_native_price(self, chain, token, payload, source):
            self.prices.append(token)

        async def store_native_price_miss(self, chain, token, reason="absent"):
            self.price_misses.append(token)

    token = "0x" + "ab" * 20
    store = TokenStore([work("token", token)])
    done = await service(store, NoPriceApi()).run_once(limit=10)

    assert done == 1
    assert store.price_misses == [token], "the miss must be cached"
    assert store.prices == [], "nothing should land in native_prices"
    assert [ok for _, ok, _ in store.finished] == [True], "a miss is data, not a failure"


@pytest.mark.asyncio
async def test_token_hit_is_not_recorded_as_a_miss() -> None:
    class PricedApi(FakeApi):
        async def native_price(self, token):
            return {"price": "123"}

    class TokenStore(FakeStore):
        def __init__(self, items):
            super().__init__(items)
            self.price_misses: list[str] = []
            self.prices: list[str] = []

        async def store_native_price(self, chain, token, payload, source):
            self.prices.append(token)

        async def store_native_price_miss(self, chain, token, reason="absent"):
            self.price_misses.append(token)

    token = "0x" + "cd" * 20
    store = TokenStore([work("token", token)])
    await service(store, PricedApi()).run_once(limit=10)
    assert store.prices == [token]
    assert store.price_misses == []
