from typing import Any

import pytest

from cow_indexer.sources.cow_api import CowApiClient
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
