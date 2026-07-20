import asyncio
from typing import Any

import pytest

from cow_indexer.sources.cow_api import AsyncRateLimiter, CowApiClient
from cow_indexer.sources.http import HttpResponse


def uid(index: int) -> str:
    return "0x" + index.to_bytes(56, "big").hex()


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None, Any]] = []

    async def request(self, method, url, *, params=None, json=None):
        self.calls.append((method, url, params, json))
        if url.endswith("/api/v1/orders/by_uids"):
            return HttpResponse(200, [{"order": {"uid": value}} for value in json])
        if "/api/v1/account/" in url:
            rows = (
                [{"uid": uid(index)} for index in range(1000)]
                if params["offset"] == 0
                else [{"uid": uid(1001)}]
            )
            return HttpResponse(200, rows)
        return HttpResponse(404, None, text="missing")

    async def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_order_uid_requests_are_split_at_128() -> None:
    transport = FakeTransport()
    client = CowApiClient("https://api.example", "test", transport=transport, interval_seconds=0)
    rows = await client.get_orders_by_uids(uid(index) for index in range(129))
    assert len(rows) == 129
    assert [len(call[3]) for call in transport.calls] == [128, 1]


@pytest.mark.asyncio
async def test_account_orders_paginate_until_short_page() -> None:
    transport = FakeTransport()
    client = CowApiClient("https://api.example", "test", transport=transport, interval_seconds=0)
    pages = [page async for page in client.iter_account_orders("0x" + "11" * 20)]
    assert [len(page) for page in pages] == [1000, 1]
    assert [call[2]["offset"] for call in transport.calls] == [0, 1000]


@pytest.mark.asyncio
async def test_trades_requires_exactly_one_selector() -> None:
    client = CowApiClient(
        "https://api.example", "test", transport=FakeTransport(), interval_seconds=0
    )
    with pytest.raises(ValueError):
        await anext(client.iter_trades())


@pytest.mark.asyncio
async def test_api_key_sets_auth_header() -> None:
    client = CowApiClient("https://api.example", "test", interval_seconds=0, api_key="secret-key")
    try:
        assert client.transport.headers.get("X-API-Key") == "secret-key"
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_no_api_key_sends_no_auth_header() -> None:
    client = CowApiClient("https://api.example", "test", interval_seconds=0)
    try:
        assert "X-API-Key" not in client.transport.headers
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_clients_can_share_one_limiter() -> None:
    limiter = AsyncRateLimiter(0.1)
    a = CowApiClient("https://api.example/mainnet", "mainnet", transport=FakeTransport(), limiter=limiter)
    b = CowApiClient("https://api.example/xdai", "gnosis", transport=FakeTransport(), limiter=limiter)
    assert a.limiter is limiter and b.limiter is limiter  # one global rate budget


def test_rate_limiter_backs_off_and_recovers() -> None:
    limiter = AsyncRateLimiter(0.1, max_interval_seconds=1.0)
    for _ in range(10):
        limiter.slow_down()
    assert limiter.interval_seconds == 1.0  # capped at max_interval
    for _ in range(100):
        limiter.speed_up()
    assert limiter.interval_seconds == 0.1  # floored at base_interval


class _BlockThenOk:
    def __init__(self) -> None:
        self.calls = 0

    async def request(self, method, url, *, params=None, json=None):
        self.calls += 1
        if self.calls == 1:
            return HttpResponse(403, None, text="Request blocked")
        return HttpResponse(200, {"ok": True})

    async def close(self) -> None:
        pass


@pytest.mark.asyncio
async def test_403_is_retried_and_throttles(monkeypatch) -> None:
    async def fast_sleep(_delay):
        return

    monkeypatch.setattr(asyncio, "sleep", fast_sleep)
    transport = _BlockThenOk()
    client = CowApiClient(
        "https://api.example", "test", transport=transport, interval_seconds=0.001
    )
    base = client.limiter.base_interval
    result = await client._request("GET", "/api/v1/version", route="/api/v1/version")
    assert result == {"ok": True}
    assert transport.calls == 2  # 403 was retried, not raised
    assert client.limiter.interval_seconds > base  # and it slowed the global rate
