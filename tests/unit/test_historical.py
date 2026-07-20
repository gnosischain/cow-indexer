from datetime import UTC, datetime
from pathlib import Path

import pytest

from cow_indexer.config import load_config
from cow_indexer.models import BlockHeader
from cow_indexer.services.historical import HistoricalIndexer
from cow_indexer.sources.rpc import RpcRangeTooLarge

ROOT = Path(__file__).parents[2]


class FakeRpc:
    def __init__(self) -> None:
        self.ranges = []

    async def safe_head(self, finality_blocks):
        return self.header(200)

    async def get_logs(self, start, end, addresses):
        self.ranges.append((start, end))
        if end - start + 1 > 100:
            raise RpcRangeTooLarge(-32005, "too many results")
        return []

    async def get_blocks(self, numbers):
        return {number: self.header(number) for number in numbers}

    @staticmethod
    def header(number):
        return BlockHeader(
            number=number,
            block_hash="0x" + f"{number:064x}",
            parent_hash="0x" + f"{max(0, number - 1):064x}",
            timestamp=datetime.fromtimestamp(number, UTC),
        )


class FakeStore:
    def __init__(self) -> None:
        self.checkpoints = []
        self.ranges = []

    async def get_checkpoint(self, chain):
        return None

    async def store_blocks(self, chain, blocks):
        pass

    async def store_raw_logs(self, chain, logs):
        pass

    async def checkpoint(self, chain, block):
        self.checkpoints.append(block.number)

    async def record_range(self, chain, run_id, start, end, rows, status, started):
        self.ranges.append((start, end))


@pytest.mark.asyncio
async def test_adaptive_scanner_reduces_provider_rejected_ranges() -> None:
    chain = load_config(ROOT / "config" / "chains.yaml").select("sepolia")[0]
    rpc = FakeRpc()
    store = FakeStore()
    count = await HistoricalIndexer(
        chain, rpc, store, initial_range=500, maximum_range=500, minimum_range=50
    ).scan(0, 200)
    assert count == 0
    assert rpc.ranges[:4] == [(0, 200), (0, 200), (0, 124), (0, 61)]
    assert store.checkpoints[-1] == 200
