from __future__ import annotations

import asyncio

import structlog

from cow_indexer.config import ChainConfig, RuntimeConfig
from cow_indexer.observability import WORK_QUEUE, HealthServer
from cow_indexer.services.enrichment import EnrichmentService
from cow_indexer.services.historical import HistoricalIndexer
from cow_indexer.sources.cow_api import CowApiClient
from cow_indexer.sources.rpc import RpcClient
from cow_indexer.storage.clickhouse import ClickHouseStore
from cow_indexer.utils import normalize_auction_order

log = structlog.get_logger()


async def _resilient_loop(name: str, chain: ChainConfig, action, interval: float) -> None:
    while True:
        try:
            await action()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error(
                "continuous_worker_error",
                worker=name,
                chain=chain.key,
                error=f"{type(exc).__name__}: {exc}",
            )
        await asyncio.sleep(interval)


async def run_continuous(
    chains: list[ChainConfig], store: ClickHouseStore, runtime: RuntimeConfig
) -> None:
    server = HealthServer(runtime.metrics_host, runtime.metrics_port, store.ping)
    await server.start()
    clients: list[tuple[RpcClient, CowApiClient]] = []
    try:
        async with asyncio.TaskGroup() as group:
            for chain in chains:
                rpc = RpcClient(chain.rpc_url, chain.key)
                api = CowApiClient(
                    chain.api_base_url,
                    chain.key,
                    interval_seconds=runtime.api_interval_seconds,
                    max_attempts=runtime.max_attempts,
                )
                clients.append((rpc, api))
                historical = HistoricalIndexer(chain, rpc, store)
                enrichment = EnrichmentService(chain, api, store, runtime)

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

                async def enrich_action(service=enrichment, current_chain=chain) -> None:
                    processed = await service.run_once()
                    WORK_QUEUE.labels(current_chain.key).set(processed)

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
                group.create_task(_resilient_loop("enrichment", chain, enrich_action, 1.0))
                group.create_task(_resilient_loop("active-orders", chain, active_action, 60.0))
                group.create_task(_resilient_loop("token-prices", chain, token_price_action, 300.0))
    finally:
        await server.close()
        await asyncio.gather(
            *(client.close() for pair in clients for client in pair), return_exceptions=True
        )
