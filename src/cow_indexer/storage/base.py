from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any, Protocol

from cow_indexer.config import ChainConfig
from cow_indexer.models import BlockHeader, DecodedEvent, ImportStats, RpcLog, WorkItem


class Storage(Protocol):
    async def ping(self) -> bool: ...
    async def store_raw_logs(self, chain: ChainConfig, logs: list[RpcLog]) -> None: ...
    async def store_blocks(self, chain: ChainConfig, blocks: Iterable[BlockHeader]) -> None: ...
    async def store_event(self, event: DecodedEvent) -> None: ...
    async def enqueue_work(
        self, chain: ChainConfig, kind: str, key: str, payload: dict[str, Any] | None = None
    ) -> None: ...
    async def checkpoint(self, chain: ChainConfig, block: BlockHeader) -> None: ...
    async def record_range(
        self,
        chain: ChainConfig,
        run_id: str,
        from_block: int,
        to_block: int,
        rows: int,
        status: str,
        started_at: datetime,
        error: str = "",
    ) -> None: ...
    async def get_checkpoint(self, chain: ChainConfig) -> int | None: ...
    async def store_api_payload(
        self, chain: ChainConfig, endpoint: str, source_key: str, payload: Any
    ) -> None: ...
    async def store_orders(
        self, chain: ChainConfig, rows: list[dict[str, Any]], source: str
    ) -> None: ...
    async def store_api_trades(
        self, chain: ChainConfig, rows: list[dict[str, Any]], source: str
    ) -> None: ...
    async def store_competition(
        self, chain: ChainConfig, payload: dict[str, Any], source: str
    ) -> None: ...
    async def store_app_data(
        self, chain: ChainConfig, app_data_hash: str, payload: dict[str, Any], source: str
    ) -> None: ...
    async def store_native_price(
        self, chain: ChainConfig, token: str, payload: dict[str, Any], source: str
    ) -> None: ...
    async def lease_work(
        self, chain: ChainConfig, worker_id: str, limit: int
    ) -> list[WorkItem]: ...
    async def finish_work(
        self,
        item: WorkItem,
        success: bool,
        error: str | None = None,
        retry_at: datetime | None = None,
    ) -> None: ...
    async def active_order_uids(self, chain: ChainConfig, limit: int = 1000) -> list[str]: ...
    async def known_tokens(self, chain: ChainConfig, limit: int = 500) -> list[str]: ...
    async def import_rows(
        self,
        manifest: Any,
        dataset: str,
        rows: list[dict[str, Any]],
        bundle_id: str,
    ) -> ImportStats: ...
