from __future__ import annotations

import asyncio
import itertools
from typing import Any

from eth_abi import decode as abi_decode

from cow_indexer.models import BlockHeader, RpcLog
from cow_indexer.observability import REQUEST_LATENCY, RPC_REQUESTS
from cow_indexer.sources.http import CurlTransport, HttpTransport

# ERC-20 zero-argument getter selectors (keccak(signature)[:4]).
ERC20_NAME = "0x06fdde03"
ERC20_SYMBOL = "0x95d89b41"
ERC20_DECIMALS = "0x313ce567"


def _decode_erc20_string(data: str) -> str | None:
    """Decode a name()/symbol() return, tolerating the bytes32 variant (e.g. MKR)."""
    raw = bytes.fromhex(data[2:]) if isinstance(data, str) and data.startswith("0x") else b""
    if not raw:
        return None
    # A real dynamic string is >= 64 bytes (offset + length + data); a 32-byte
    # return is the fixed bytes32 variant, so skip the string attempt for it.
    if len(raw) != 32:
        try:
            value = abi_decode(["string"], raw)[0]
            if value:
                return value
        except Exception:  # noqa: BLE001 - fall back to the fixed-width variant
            pass
    try:
        text = abi_decode(["bytes32"], raw)[0].rstrip(b"\x00").decode("utf-8", "replace")
        return text or None
    except Exception:  # noqa: BLE001 - non-compliant token
        return None


def _decode_erc20_uint(data: str) -> int | None:
    raw = bytes.fromhex(data[2:]) if isinstance(data, str) and data.startswith("0x") else b""
    if not raw:
        return None
    try:
        return int(abi_decode(["uint256"], raw)[0])
    except Exception:  # noqa: BLE001 - non-compliant token
        return None


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
        request_timeout: float = 60.0,
    ) -> None:
        self.url = url
        self.chain_key = chain_key
        self.transport = transport or CurlTransport()
        self._ids = itertools.count(1)
        self._semaphore = asyncio.Semaphore(concurrency)
        self.request_timeout = request_timeout

    async def close(self) -> None:
        await self.transport.close()

    async def call(self, method: str, params: list[Any]) -> Any:
        payload = {"jsonrpc": "2.0", "id": next(self._ids), "method": method, "params": params}
        with REQUEST_LATENCY.labels("rpc", self.chain_key).time():
            async with self._semaphore:
                # Hard ceiling above the transport timeout: a provider that accepts
                # the connection but never responds must raise (retryable) rather
                # than park the scan coroutine forever holding a semaphore slot.
                response = await asyncio.wait_for(
                    self.transport.request("POST", self.url, json=payload),
                    timeout=self.request_timeout,
                )
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

    async def eth_call(self, to: str, data: str, block: str = "latest") -> str:
        result = await self.call("eth_call", [{"to": to, "data": data}, block])
        return result if isinstance(result, str) else "0x"

    async def _call_getter(self, token: str, selector: str) -> str | None:
        try:
            return await self.eth_call(token, selector)
        except RpcError:
            return None

    async def fetch_token_metadata(self, token: str) -> dict[str, Any] | None:
        """Read symbol/name/decimals via eth_call. Returns None if the token
        answers none of the getters (non-ERC-20 or self-destructed)."""
        symbol_data = await self._call_getter(token, ERC20_SYMBOL)
        name_data = await self._call_getter(token, ERC20_NAME)
        decimals_data = await self._call_getter(token, ERC20_DECIMALS)
        symbol = _decode_erc20_string(symbol_data) if symbol_data is not None else None
        name = _decode_erc20_string(name_data) if name_data is not None else None
        decimals = _decode_erc20_uint(decimals_data) if decimals_data is not None else None
        if symbol is None and name is None and decimals is None:
            return None
        return {
            "symbol": symbol or "",
            "name": name or "",
            "decimals": decimals if decimals is not None else 0,
        }
