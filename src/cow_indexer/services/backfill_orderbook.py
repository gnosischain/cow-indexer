"""Historical orderbook backfill: replay years of off-chain order history through the
public CoW API using the existing work-queue machinery.

Two passes, both fully resumable through work-item terminal states:

- ``order_uids_batch``: every traded-but-unknown order uid, batched 128 per item
  (one POST /api/v1/orders/by_uids call each), seeded newest embedded-validTo first
  so depth coverage grows contiguously backward from the live-capture floor.
- ``owner_orders_backfill``: every distinct trader address, one item per owner
  (GET /api/v1/account/{owner}/orders pagination), recovering expired/cancelled
  never-executed orders of owners who traded at least once.

The drain runs OUTSIDE ``continuous`` with its own rate limiter (COW_BACKFILL_*
knobs) so the historical sweep can never starve live ingestion; conversely
``lease_work`` hides the backfill kinds from the live enrichment loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import timedelta
from typing import Any

import structlog

from cow_indexer.config import ChainConfig, RuntimeConfig
from cow_indexer.models import BACKFILL_WORK_KINDS, WorkItem
from cow_indexer.sources.cow_api import AsyncRateLimiter, CowApiClient
from cow_indexer.storage.clickhouse import ClickHouseStore
from cow_indexer.utils import batched, normalize_order_uid, utcnow

log = structlog.get_logger()

# Documented maximum for POST /api/v1/orders/by_uids.
UID_BATCH_SIZE = 128
# Work items per enqueue_work_many INSERT. Chunks are enqueued sequentially, so
# next_attempt_at (stamped at insert time) increases monotonically chunk to chunk and
# lease_work's ORDER BY next_attempt_at drains the queue in seeding (newest-first) order.
ENQUEUE_CHUNK = 1_000
# Lease conservatively: an owner item may take max-pages API calls; with 2-way
# concurrency at ~2 RPS, 32 worst-case items still finish inside the 5-minute lease.
DRAIN_LEASE_LIMIT = 32
DRAIN_IDLE_SLEEP_SECONDS = 5.0

# An order uid is '0x' + 112 hex chars encoding 56 bytes: 32-byte order digest,
# 20-byte owner, 4-byte big-endian validTo. The validTo hex therefore spans
# full-string chars 107..114 in 1-based SQL substring terms — Python slice
# [106:114] — mirrored by storage.clickhouse.ORDER_UID_VALID_TO_SQL.
VALID_TO_HEX_SLICE = slice(106, 114)


def decode_valid_to(order_uid: str) -> int:
    """The big-endian uint32 validTo epoch embedded in the last 4 uid bytes."""
    return int(normalize_order_uid(order_uid)[VALID_TO_HEX_SLICE], 16)


def encode_uid_batch_key(uids: Iterable[str]) -> str:
    """Canonical work key for a uid batch: comma-joined, sorted within the batch so
    the same uid set always hashes to the same work_id (idempotent re-seeds)."""
    batch = sorted(normalize_order_uid(uid) for uid in uids)
    if not batch:
        raise ValueError("uid batch must not be empty")
    if len(batch) > UID_BATCH_SIZE:
        raise ValueError(f"uid batch exceeds {UID_BATCH_SIZE} uids: {len(batch)}")
    return ",".join(batch)


def decode_uid_batch_key(key: str) -> list[str]:
    return [normalize_order_uid(uid) for uid in key.split(",") if uid]


class BackfillOrderbookService:
    def __init__(self, store: ClickHouseStore, runtime: RuntimeConfig) -> None:
        self.store = store
        self.runtime = runtime

    def _limiter(self) -> AsyncRateLimiter:
        """A limiter DEDICATED to the backfill sweep. Never share the live loop's
        limiter: both back off independently on 429/403, so a throttled backfill
        cannot slow live ingestion (and vice versa)."""
        return AsyncRateLimiter(
            self.runtime.backfill_interval_seconds,
            self.runtime.backfill_max_interval_seconds,
        )

    def _client(self, chain: ChainConfig, limiter: AsyncRateLimiter) -> CowApiClient:
        return CowApiClient(
            chain.api_base_url,
            chain.key,
            max_attempts=self.runtime.max_attempts,
            api_key=self.runtime.api_key,
            limiter=limiter,
        )

    async def probe(self, chains: list[ChainConfig], per_chain: int = 5) -> dict[str, Any]:
        """Feasibility gate, repeatable: fetch the oldest-traded uids per chain via
        by_uids and report the hit rate. Old uids that 404 mark the epoch the public
        API no longer serves (observed: pre-2022-Q3 / pre-migration uids)."""
        limiter = self._limiter()
        summary: dict[str, Any] = {}
        for chain in chains:
            rows = await self.store.oldest_traded_order_uids(chain, per_chain)
            if not rows:
                summary[chain.key] = {"requested": 0, "found": 0, "note": "no trades indexed"}
                continue
            requested = [uid for uid, _ in rows]
            api = self._client(chain, limiter)
            try:
                orders = await api.get_orders_by_uids(requested)
            finally:
                await api.close()
            found = _returned_uids(orders)
            hits = [uid for uid in requested if uid in found]
            summary[chain.key] = {
                "requested": len(requested),
                "found": len(hits),
                "hit_rate": round(len(hits) / len(requested), 3),
                "oldest_trade": rows[0][1],
                "missing_uids": [uid for uid in requested if uid not in found],
            }
            log.info(
                "backfill_probe",
                chain=chain.key,
                requested=len(requested),
                found=len(hits),
                hit_rate=summary[chain.key]["hit_rate"],
                oldest_trade=str(rows[0][1]),
            )
        return summary

    async def seed_orders(
        self,
        chain: ChainConfig,
        limit: int | None = None,
        batch_size: int = UID_BATCH_SIZE,
    ) -> dict[str, Any]:
        """Enqueue one ``order_uids_batch`` item per <=batch_size traded uids missing
        from `orders`. The anti-join runs SQL-side (see
        stream_missing_traded_order_uids) and the stream arrives newest embedded
        validTo first, so the drain extends coverage contiguously backward."""
        if not 1 <= batch_size <= UID_BATCH_SIZE:
            raise ValueError(f"batch_size must be within 1..{UID_BATCH_SIZE}")
        seeded = 0
        batches = 0
        buffer: list[str] = []
        items: list[tuple[str, str, dict[str, Any] | None]] = []

        async def flush_items() -> None:
            nonlocal items
            if items:
                await self.store.enqueue_work_many(chain, items)
                items = []

        def take_batch(batch: list[str]) -> None:
            nonlocal seeded, batches
            items.append(("order_uids_batch", encode_uid_batch_key(batch), {"uids": len(batch)}))
            seeded += len(batch)
            batches += 1

        async for block in self.store.stream_missing_traded_order_uids(chain, limit):
            buffer.extend(block)
            while len(buffer) >= batch_size:
                take_batch(buffer[:batch_size])
                buffer = buffer[batch_size:]
                if len(items) >= ENQUEUE_CHUNK:
                    await flush_items()
        if buffer:
            take_batch(buffer)
        await flush_items()

        already_covered: int | None = None
        if limit is None:
            total = await self.store.count_distinct_traded_order_uids(chain)
            already_covered = max(0, total - seeded)
        result = {
            "chain": chain.key,
            "seeded_uids": seeded,
            "batches": batches,
            "skipped_existing": already_covered,
        }
        log.info("backfill_seed_orders", **result)
        return result

    async def seed_owners(self, chain: ChainConfig, limit: int | None = None) -> dict[str, Any]:
        """Enqueue one ``owner_orders_backfill`` item per distinct trader. No
        client-side anti-join: work identity IS the dedup — re-enqueueing an owner
        whose work_id already carries a terminal (done/dead) revision writes a
        revision-0 row that loses the ReplacingMergeTree merge, so completed owners
        are never re-processed. Owners who never traded stay invisible (disclosed)."""
        owners = await self.store.distinct_trade_owners(chain, limit)
        for chunk in batched(owners, ENQUEUE_CHUNK):
            await self.store.enqueue_work_many(
                chain, [("owner_orders_backfill", owner, None) for owner in chunk]
            )
        result = {"chain": chain.key, "seeded_owners": len(owners)}
        log.info("backfill_seed_owners", **result)
        return result

    async def drain(
        self, chains: list[ChainConfig], run_seconds: float | None = None
    ) -> dict[str, Any]:
        """Lease and process ONLY the two backfill kinds until the queue is empty (or
        ``run_seconds`` elapses), with a dedicated client + rate limiter per run so
        the live ingestion budget is untouched. Safe to run beside ``continuous``."""
        limiter = self._limiter()
        clients = {chain.key: self._client(chain, limiter) for chain in chains}
        loop = asyncio.get_running_loop()
        deadline = loop.time() + run_seconds if run_seconds is not None else None
        processed_total: dict[str, int] = {chain.key: 0 for chain in chains}
        stopped = "drained"
        try:
            while True:
                if deadline is not None and loop.time() >= deadline:
                    stopped = "deadline"
                    break
                processed = 0
                for chain in chains:
                    items = await self.store.lease_work(
                        chain, self.runtime.worker_id, DRAIN_LEASE_LIMIT, kinds=BACKFILL_WORK_KINDS
                    )
                    if not items:
                        continue
                    semaphore = asyncio.Semaphore(self.runtime.backfill_concurrency)
                    api = clients[chain.key]

                    async def guarded(
                        item: WorkItem,
                        chain: ChainConfig = chain,
                        api: CowApiClient = api,
                        semaphore: asyncio.Semaphore = semaphore,
                    ) -> None:
                        async with semaphore:
                            await self._process_and_finish(chain, api, item)

                    await asyncio.gather(*(guarded(item) for item in items))
                    processed += len(items)
                    processed_total[chain.key] += len(items)
                if processed:
                    continue
                # Nothing leasable right now: finished, or only retry-scheduled items
                # remain. Exit when no open item exists; otherwise wait for retries.
                if not await self._outstanding(chains):
                    break
                await asyncio.sleep(DRAIN_IDLE_SLEEP_SECONDS)
        finally:
            await asyncio.gather(
                *(client.close() for client in clients.values()), return_exceptions=True
            )
        result = {"processed": processed_total, "stopped": stopped}
        log.info("backfill_drain_finished", **result)
        return result

    async def _outstanding(self, chains: list[ChainConfig]) -> int:
        open_items = 0
        for chain in chains:
            counts = await self.store.backfill_work_counts(chain)
            for statuses in counts.values():
                open_items += sum(
                    count
                    for status, count in statuses.items()
                    if status in ("pending", "running")
                )
        return open_items

    async def status(self, chains: list[ChainConfig]) -> list[dict[str, Any]]:
        """Per chain: backfill work-item counts by kind/status plus orders coverage
        per source (min/max creation_date) — the acceptance metric for the backfill."""
        output: list[dict[str, Any]] = []
        for chain in chains:
            work = await self.store.backfill_work_counts(chain)
            coverage = await self.store.coverage(chain)
            output.append({"chain": chain.key, "work": work, "orders": coverage["orders"]})
        return output

    async def _process_and_finish(
        self, chain: ChainConfig, api: CowApiClient, item: WorkItem
    ) -> None:
        try:
            await self.process(chain, api, item)
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
                "backfill_item_failed",
                chain=chain.key,
                kind=item.kind,
                key=item.key[:97],  # a uid batch key is up to ~14KB; log a prefix
                attempts=item.attempts,
                error=error,
            )

    async def process(self, chain: ChainConfig, api: CowApiClient, item: WorkItem) -> None:
        """Backfill processors. Deliberately NO store_api_payload calls: archiving
        ~12M raw-JSON rows for a replay of API-shaped data is pure bloat. store_orders
        still emits the ``status:{status}`` order_events rows."""
        if item.kind == "order_uids_batch":
            uids = decode_uid_batch_key(item.key)
            # ONE call (the key holds <=128 uids). by_uids answers per uid: entries
            # carry the order when the API still serves it; uids from the aged-out
            # pre-migration epoch simply come back without one. Missing uids are
            # DATA (counted in the finish payload), never a batch failure.
            orders = await api.get_orders_by_uids(uids)
            if orders:
                await self.store.store_orders(chain, orders, "backfill")
            found = _returned_uids(orders)
            missing = sum(1 for uid in uids if uid not in found)
            item.payload = {**item.payload, "found": len(found), "missing": missing}
            log.info(
                "backfill_uid_batch",
                chain=chain.key,
                requested=len(uids),
                found=len(found),
                missing=missing,
            )
        elif item.kind == "owner_orders_backfill":
            pages = 0
            stored = 0
            truncated = False
            async for page in api.iter_account_orders(item.key):
                await self.store.store_orders(chain, page, "backfill")
                stored += len(page)
                pages += 1
                if pages >= self.runtime.backfill_max_pages_per_owner:
                    truncated = True
                    break
            item.payload = {
                **item.payload,
                "orders": stored,
                "pages": pages,
                "truncated": truncated,
            }
            log.info(
                "backfill_owner",
                chain=chain.key,
                owner=item.key,
                orders=stored,
                pages=pages,
                truncated=truncated,
            )
        else:
            raise ValueError(f"unsupported backfill work kind: {item.kind}")


def _returned_uids(orders: list[dict[str, Any]]) -> set[str]:
    found: set[str] = set()
    for order in orders:
        uid = order.get("uid")
        if not isinstance(uid, str):
            continue
        try:
            found.add(normalize_order_uid(uid))
        except ValueError:
            continue
    return found
