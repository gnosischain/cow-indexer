"""Unit tests for the correctness/throughput remediation:
competition normalization, token-metadata decoding, and the competition-404 signal."""

from __future__ import annotations

import pytest

from cow_indexer.sources.cow_api import CompetitionUnavailable, CowApiClient
from cow_indexer.sources.http import HttpResponse
from cow_indexer.sources.rpc import (
    ERC20_DECIMALS,
    ERC20_NAME,
    ERC20_SYMBOL,
    RpcClient,
    _decode_erc20_string,
    _decode_erc20_uint,
)
from cow_indexer.storage.clickhouse import _competition_tx_hashes, _winning_solution


def _abi_string(text: str) -> str:
    body = text.encode()
    offset = (32).to_bytes(32, "big")
    length = len(body).to_bytes(32, "big")
    padded = body + b"\x00" * ((32 - len(body) % 32) % 32 if len(body) % 32 else 0)
    return "0x" + (offset + length + padded).hex()


def test_winning_solution_prefers_is_winner_then_ranking_then_last() -> None:
    winner = {"solverAddress": "0x" + "22" * 20, "ranking": 1, "isWinner": True}
    assert _winning_solution([{"solverAddress": "0x" + "11" * 20}, winner]) is winner
    assert _winning_solution([{"ranking": 2}, {"ranking": 1}])["ranking"] == 1
    assert _winning_solution([{"a": 1}, {"a": 2}]) == {"a": 2}
    assert _winning_solution([]) is None


def test_competition_tx_hashes_array_singular_and_invalid() -> None:
    h1 = "0x" + "ab" * 32
    h2 = "0x" + "cd" * 32
    assert _competition_tx_hashes({"transactionHashes": [h1, h2, h1]}) == [h1, h2]
    assert _competition_tx_hashes({"transactionHash": h1}) == [h1]
    assert _competition_tx_hashes({}) == []
    assert _competition_tx_hashes({"transactionHashes": ["not-a-hash"]}) == []


def test_decode_erc20_string_dynamic_and_bytes32() -> None:
    assert _decode_erc20_string(_abi_string("USDC")) == "USDC"
    assert _decode_erc20_string(_abi_string("USD Coin")) == "USD Coin"
    # bytes32 variant (e.g. MKR): 32-byte right-padded ASCII.
    assert _decode_erc20_string("0x" + (b"MKR" + b"\x00" * 29).hex()) == "MKR"
    assert _decode_erc20_string("0x") is None
    assert _decode_erc20_string("0x" + "00" * 32) is None


def test_decode_erc20_uint() -> None:
    assert _decode_erc20_uint("0x" + (18).to_bytes(32, "big").hex()) == 18
    assert _decode_erc20_uint("0x") is None


class _FakeApiTransport:
    async def request(self, method, url, *, params=None, json=None):
        return HttpResponse(404, None, text="missing")

    async def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_competition_by_transaction_raises_unavailable_on_404() -> None:
    client = CowApiClient(
        "https://api.example", "test", transport=_FakeApiTransport(), interval_seconds=0
    )
    with pytest.raises(CompetitionUnavailable):
        await client.competition_by_transaction("0x" + "ab" * 32)


class _FakeRpcTransport:
    def __init__(self, by_selector: dict[str, str | None]) -> None:
        self.by_selector = by_selector

    async def request(self, method, url, *, params=None, json=None):
        selector = json["params"][0]["data"]
        return HttpResponse(200, {"jsonrpc": "2.0", "id": json["id"], "result": self.by_selector.get(selector)})

    async def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_fetch_token_metadata_decodes_getters() -> None:
    transport = _FakeRpcTransport(
        {
            ERC20_SYMBOL: _abi_string("USDC"),
            ERC20_NAME: _abi_string("USD Coin"),
            ERC20_DECIMALS: "0x" + (6).to_bytes(32, "big").hex(),
        }
    )
    client = RpcClient("https://rpc.example", "test", transport=transport)
    assert await client.fetch_token_metadata("0x" + "12" * 20) == {
        "symbol": "USDC",
        "name": "USD Coin",
        "decimals": 6,
    }


@pytest.mark.asyncio
async def test_fetch_token_metadata_none_for_non_erc20() -> None:
    transport = _FakeRpcTransport({ERC20_SYMBOL: "0x", ERC20_NAME: "0x", ERC20_DECIMALS: "0x"})
    client = RpcClient("https://rpc.example", "test", transport=transport)
    assert await client.fetch_token_metadata("0x" + "34" * 20) is None
