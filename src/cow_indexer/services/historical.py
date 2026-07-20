from __future__ import annotations

import uuid

import structlog

from cow_indexer.config import ChainConfig
from cow_indexer.decoders import MultiContractDecoder
from cow_indexer.loaders.events import EventProcessor
from cow_indexer.models import DecodedEvent
from cow_indexer.observability import CHAIN_LAG
from cow_indexer.sources.rpc import RpcClient, RpcRangeTooLarge
from cow_indexer.storage.base import Storage
from cow_indexer.utils import utcnow

log = structlog.get_logger()


class HistoricalIndexer:
    def __init__(
        self,
        chain: ChainConfig,
        rpc: RpcClient,
        store: Storage,
        initial_range: int = 5_000,
        maximum_range: int = 50_000,
        minimum_range: int = 50,
    ) -> None:
        if chain.deployment is None:
            chain.load_deployment()
        assert chain.deployment is not None
        self.chain = chain
        self.rpc = rpc
        self.store = store
        self.decoder = MultiContractDecoder(chain.deployment, chain.environment)
        self.processor = EventProcessor(store, chain)
        self.initial_range = initial_range
        self.maximum_range = maximum_range
        self.minimum_range = minimum_range

    async def scan(
        self,
        from_block: int | None = None,
        to_block: int | None = None,
        *,
        update_checkpoint: bool = True,
        store_all_blocks: bool = False,
    ) -> int:
        assert self.chain.deployment is not None
        safe_head = await self.rpc.safe_head(self.chain.finality_blocks)
        checkpoint = await self.store.get_checkpoint(self.chain)
        start = from_block
        if start is None:
            start = checkpoint + 1 if checkpoint is not None else self.chain.deployment.start_block
        end = min(to_block, safe_head.number) if to_block is not None else safe_head.number
        if start > end:
            CHAIN_LAG.labels(self.chain.key).set(max(0, safe_head.number - (checkpoint or 0)))
            return 0

        addresses = [contract.address for contract in self.chain.deployment.contracts]
        cursor = start
        range_size = self.initial_range
        total_logs = 0
        while cursor <= end:
            lower = cursor
            upper = min(cursor + range_size - 1, end)
            started = utcnow()
            run_id = str(uuid.uuid4())
            try:
                logs = await self.rpc.get_logs(cursor, upper, addresses)
            except RpcRangeTooLarge:
                if range_size <= self.minimum_range:
                    raise
                range_size = max(self.minimum_range, range_size // 2)
                log.warning(
                    "rpc_range_reduced",
                    chain=self.chain.key,
                    cursor=cursor,
                    range_size=range_size,
                )
                continue

            block_numbers = set(range(cursor, upper + 1)) if store_all_blocks else {upper}
            block_numbers.update(item.block_number for item in logs)
            blocks = await self.rpc.get_blocks(block_numbers)
            await self.store.store_blocks(self.chain, blocks.values())
            await self.store.store_raw_logs(self.chain, logs)
            decoded: list[DecodedEvent] = []
            for rpc_log in logs:
                event = self.decoder.decode(rpc_log)
                if event is None:
                    continue
                event.block_timestamp = blocks[rpc_log.block_number].timestamp
                decoded.append(event)
            await self.processor.process_many(decoded)
            if update_checkpoint:
                await self.store.checkpoint(self.chain, blocks[upper])
            if hasattr(self.store, "record_range"):
                await self.store.record_range(
                    self.chain, run_id, cursor, upper, len(logs), "complete", started
                )
            total_logs += len(logs)
            cursor = upper + 1
            range_size = min(self.maximum_range, range_size * 2)
            # Refresh lag after every committed range so a long catch-up scan does not
            # leave a stale gauge until the whole scan returns.
            CHAIN_LAG.labels(self.chain.key).set(max(0, safe_head.number - upper))
            log.info(
                "range_indexed",
                chain=self.chain.key,
                from_block=lower,
                to_block=upper,
                logs=len(logs),
            )
        CHAIN_LAG.labels(self.chain.key).set(max(0, safe_head.number - end))
        return total_logs

    async def rescan_finality_window(self) -> int:
        head = await self.rpc.safe_head(self.chain.finality_blocks)
        start = max(0, head.number - self.chain.finality_blocks + 1)
        return await self.scan(
            start,
            head.number,
            update_checkpoint=False,
            store_all_blocks=True,
        )
