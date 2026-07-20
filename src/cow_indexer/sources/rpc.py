from __future__ import annotations

import asyncio
import itertools
from typing import Any

from cow_indexer.models import BlockHeader, RpcLog
from cow_indexer.observability import REQUEST_LATENCY, RPC_REQUESTS
from cow_indexer.sources.http import CurlTransport, HttpTransport


class RpcError(RuntimeError):
    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"RPC {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class RpcRangeTooLarge(RpcError):
    pass


def is_range_error(code: int, message: str) -> bool:
    lowered = message.lower()
    markers = (
        "query returned more than",
        "response size exceeded",
        "block range",
        "too many results",
        "limit exceeded",
        "please limit",
        "request timed out",
    )
    return code in {-32005, -32016} or any(marker in lowered for marker in markers)


class RpcClient:
    def __init__(
        self,
        url: str,
        chain_key: str,
        transport: HttpTransport | None = None,
        concurrency: int = 10,
    ) -> None:
        self.url = url
        self.chain_key = chain_key
        self.transport = transport or CurlTransport()
        self._ids = itertools.count(1)
        self._semaphore = asyncio.Semaphore(concurrency)

    async def close(self) -> None:
        await self.transport.close()

    async def call(self, method: str, params: list[Any]) -> Any:
        payload = {"jsonrpc": "2.0", "id": next(self._ids), "method": method, "params": params}
        with REQUEST_LATENCY.labels("rpc", self.chain_key).time():
            async with self._semaphore:
                response = await self.transport.request("POST", self.url, json=payload)
        if response.status != 200:
            RPC_REQUESTS.labels(self.chain_key, method, str(response.status)).inc()
            raise RpcError(response.status, response.text[:500])
        if not isinstance(response.data, dict):
            RPC_REQUESTS.labels(self.chain_key, method, "invalid").inc()
            raise RpcError(-1, "invalid JSON-RPC response")
        if error := response.data.get("error"):
            code = int(error.get("code", -1))
            message = str(error.get("message", "unknown error"))
            RPC_REQUESTS.labels(self.chain_key, method, str(code)).inc()
            error_type = (
                RpcRangeTooLarge
                if method == "eth_getLogs" and is_range_error(code, message)
                else RpcError
            )
            raise error_type(code, message, error.get("data"))
        RPC_REQUESTS.labels(self.chain_key, method, "ok").inc()
        return response.data.get("result")

    async def get_block_number(self) -> int:
        return int(await self.call("eth_blockNumber", []), 16)

    async def get_block(self, block: int | str) -> BlockHeader:
        tag = hex(block) if isinstance(block, int) else block
        payload = await self.call("eth_getBlockByNumber", [tag, False])
        if payload is None:
            raise RpcError(-1, f"block not found: {tag}")
        return BlockHeader.from_rpc(payload)

    async def safe_head(self, finality_blocks: int) -> BlockHeader:
        try:
            return await self.get_block("safe")
        except RpcError:
            latest = await self.get_block_number()
            return await self.get_block(max(0, latest - finality_blocks))

    async def get_logs(
        self,
        from_block: int,
        to_block: int,
        addresses: list[str],
        topics: list[str | list[str] | None] | None = None,
    ) -> list[RpcLog]:
        if to_block < from_block:
            return []
        params: dict[str, Any] = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": addresses[0] if len(addresses) == 1 else addresses,
        }
        if topics is not None:
            params["topics"] = topics
        result = await self.call("eth_getLogs", [params])
        return [RpcLog.from_rpc(log) for log in result]

    async def get_blocks(self, numbers: set[int]) -> dict[int, BlockHeader]:
        headers = await asyncio.gather(*(self.get_block(number) for number in sorted(numbers)))
        return {header.number: header for header in headers}
