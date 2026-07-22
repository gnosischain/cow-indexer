from __future__ import annotations

import asyncio
import random
from datetime import timedelta

import structlog

from cow_indexer.config import ChainConfig, RuntimeConfig
from cow_indexer.observability import HealthServer
from cow_indexer.services.enrichment import EnrichmentService
from cow_indexer.services.historical import HistoricalIndexer
from cow_indexer.sources.cow_api import AsyncRateLimiter, CowApiClient
from cow_indexer.sources.rpc import RpcClient
from cow_indexer.storage.clickhouse import ClickHouseStore
from cow_indexer.utils import normalize_auction_order, utcnow

log = structlog.get_logger()

_MAX_BACKOFF_SECONDS = 300.0


async def _resilient_loop(name: str, chain: ChainConfig, action, interval: float) -> None:
    # On repeated failures, back off exponentially (capped) instead of hammering at the
    # base interval — a deliberately memory-capped FINAL that fails must not become a
    # query storm. A clean run resets the backoff.
    failures = 0
    while True:
        try:
            await action()
            failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            log.error(
                "continuous_worker_error",
                worker=name,
                chain=chain.key,
                failures=failures,
                error=f"{type(exc).__name__}: {exc}",
            )
        if failures == 0:
            delay = interval
        else:
            delay = min(_MAX_BACKOFF_SECONDS, interval * (2 ** min(failures, 10)))
        await asyncio.sleep(delay)


async def _purge_loop(
    chains: list[ChainConfig], store: ClickHouseStore, runtime: RuntimeConfig
) -> None:
    """One process-wide maintenance task that trims terminal work_items so the queue
    stays small. It visits chains sequentially (work_items is a single unpartitioned
    table and mutations serialize), runs one bounded batch per chain per sweep, never
    overlaps another purge, and backs off on error. Disabled via COW_PURGE_ENABLED
    during a one-time backlog cleanup so it does not race the `purge-work` CLI."""
    if not runtime.purge_enabled:
        log.info("purge_disabled")
        return
    # Wait before the first sweep so startup isn't competing with a mutation.
    await asyncio.sleep(runtime.purge_interval_seconds)
    failures = 0
    while True:
        try:
            cutoff = utcnow() - timedelta(hours=runtime.purge_grace_hours)
            purged = 0
            for chain in chains:
                purged += await store.purge_finished_work(chain, cutoff, runtime.purge_batch)
            failures = 0
            log.info("purge_sweep", purged=purged, chains=len(chains))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            failures += 1
            log.error(
                "purge_error", failures=failures, error=f"{type(exc).__name__}: {exc}"
            )
        base = runtime.purge_interval_seconds
        delay = base if failures == 0 else min(3600.0, base * (2 ** min(failures, 6)))
        # Jitter so restarts/replicas don't align their sweeps on the shared table.
        await asyncio.sleep(delay * random.uniform(0.9, 1.1))


async def run_continuous(
    chains: list[ChainConfig], store: ClickHouseStore, runtime: RuntimeConfig
) -> None:
    server = HealthServer(runtime.metrics_host, runtime.metrics_port, store.ping)
    await server.start()
    clients: list[tuple[RpcClient, CowApiClient]] = []
    # One limiter shared by every chain: all CoW API calls target the same
    # api.cow.fi host/key, so the rate budget must be global. Otherwise N chains
    # run at N x the per-chain rate and blow past the key's allowance.
    api_limiter = AsyncRateLimiter(runtime.api_interval_seconds, runtime.api_max_interval_seconds)
    try:
        async with asyncio.TaskGroup() as group:
            for chain in chains:
                rpc = RpcClient(chain.rpc_url, chain.key)
                api = CowApiClient(
                    chain.api_base_url,
                    chain.key,
                    max_attempts=runtime.max_attempts,
                    api_key=runtime.api_key,
                    limiter=api_limiter,
                )
                clients.append((rpc, api))
                historical = HistoricalIndexer(chain, rpc, store)
                enrichment = EnrichmentService(
                    chain, api, store, runtime, concurrency=runtime.enrich_concurrency
                )

                async def scan_action(indexer=historical) -> None:
                    await indexer.scan()
                    await indexer.rescan_finality_window()

                async def competition_action(current_chain=chain, current_api=api) -> None:
                    payload = await current_api.latest_competition()
                    if payload:
                        auction_id = str(payload.get("auctionId", "latest"))
                        await store.store_api_payload(
                            current_chain, "competition:latest", auction_id, payload
                        )
                        await store.store_competition(current_chain, payload, "api")
                        auction = payload.get("auction") or {}
                        for order in auction.get("orders", []):
                            normalized = normalize_auction_order(order)
                            if normalized:
                                await store.enqueue_work(
                                    current_chain, "order_uid", normalized[0]
                                )

                async def enrich_action(service=enrichment) -> None:
                    await service.run_once(limit=runtime.enrich_batch)

                async def token_metadata_action(current_chain=chain, current_rpc=rpc) -> None:
                    have = set(await store.tokens_with_metadata(current_chain))
                    pending = [
                        token
                        for token in await store.known_tokens(current_chain)
                        if token not in have
                    ]
                    for token in pending[:50]:
                        metadata = await current_rpc.fetch_token_metadata(token)
                        if metadata is not None:
                            await store.store_token_metadata(
                                current_chain, token, metadata, "rpc"
                            )

                async def active_action(current_chain=chain, current_api=api) -> None:
                    uids = await store.active_order_uids(current_chain)
                    if not uids:
                        return
                    orders = await current_api.get_orders_by_uids(uids)
                    await store.store_api_payload(
                        current_chain, "active_orders", "periodic", orders
                    )
                    await store.store_orders(current_chain, orders, "api")

                async def token_price_action(current_chain=chain, current_api=api) -> None:
                    for token in await store.known_tokens(current_chain):
                        payload = await current_api.native_price(token)
                        if payload:
                            await store.store_native_price(current_chain, token, payload, "api")

                group.create_task(_resilient_loop("rpc", chain, scan_action, 12.0))
                group.create_task(_resilient_loop("competition", chain, competition_action, 30.0))
                group.create_task(
                    _resilient_loop(
                        "enrichment", chain, enrich_action, runtime.enrich_interval_seconds
                    )
                )
                group.create_task(_resilient_loop("active-orders", chain, active_action, 60.0))
                group.create_task(_resilient_loop("token-prices", chain, token_price_action, 300.0))
                group.create_task(
                    _resilient_loop("token-metadata", chain, token_metadata_action, 300.0)
                )
            # One global, serialized retention task (NOT per-chain) trims terminal
            # work_items so lease_work's FINAL never scans an unbounded table.
            group.create_task(_purge_loop(chains, store, runtime))
    finally:
        await server.close()
        await asyncio.gather(
            *(client.close() for pair in clients for client in pair), return_exceptions=True
        )
