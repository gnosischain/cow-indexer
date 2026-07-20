from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator, Iterable
from typing import Any

from cow_indexer.observability import API_REQUESTS, REQUEST_LATENCY
from cow_indexer.sources.http import CurlTransport, HttpResponse, HttpTransport
from cow_indexer.utils import batched, normalize_address, normalize_hash, normalize_order_uid


class CowApiError(RuntimeError):
    def __init__(self, status: int, path: str, detail: str) -> None:
        super().__init__(f"CoW API {status} for {path}: {detail}")
        self.status = status
        self.path = path


class CompetitionUnavailable(RuntimeError):
    """The public API does not (or no longer) serves a competition for this key.

    Raised so the enrichment worker can mark the work item terminally
    ``unavailable_from_public_api`` instead of retrying or dead-lettering it.
    """

    def __init__(self, key: str) -> None:
        super().__init__(f"solver competition unavailable from public API: {key}")
        self.key = key


class AsyncRateLimiter:
    """One request per ``interval_seconds``, adaptively backing off toward
    ``max_interval_seconds`` when the upstream edge throttles us (429/403) and
    recovering toward the base rate as requests succeed."""

    def __init__(self, interval_seconds: float, max_interval_seconds: float = 5.0) -> None:
        self.base_interval = interval_seconds
        self.max_interval = max(max_interval_seconds, interval_seconds)
        self.interval_seconds = interval_seconds
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        async with self._lock:
            delay = self._next - loop.time()
            if delay > 0:
                await asyncio.sleep(delay)
            self._next = loop.time() + self.interval_seconds

    def slow_down(self, factor: float = 2.0) -> None:
        self.interval_seconds = min(
            self.max_interval, max(self.base_interval, self.interval_seconds * factor)
        )

    def speed_up(self, factor: float = 0.9) -> None:
        self.interval_seconds = max(self.base_interval, self.interval_seconds * factor)


class CowApiClient:
    def __init__(
        self,
        base_url: str,
        chain_key: str,
        transport: HttpTransport | None = None,
        interval_seconds: float = 0.05,
        max_attempts: int = 6,
        max_interval_seconds: float = 5.0,
        api_key: str | None = None,
        limiter: AsyncRateLimiter | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.chain_key = chain_key
        self.api_key = api_key
        if transport is None:
            headers = {"X-API-Key": api_key} if api_key else None
            transport = CurlTransport(headers=headers)
        self.transport = transport
        # A shared limiter lets multiple chains share one api.cow.fi rate budget
        # (they hit the same host/key); otherwise each chain limits independently.
        self.limiter = limiter or AsyncRateLimiter(interval_seconds, max_interval_seconds)
        self.max_attempts = max_attempts

    async def close(self) -> None:
        await self.transport.close()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        allow_404: bool = False,
        route: str | None = None,
    ) -> Any:
        response: HttpResponse | None = None
        # 403 is included because CoW's CloudFront edge returns "Request blocked" with a
        # 403 when it rate-limits a client; 403/429 additionally slow the global rate.
        retryable = {403, 408, 425, 429, 500, 502, 503, 504}
        throttling = {403, 429}
        for attempt in range(self.max_attempts):
            await self.limiter.wait()
            with REQUEST_LATENCY.labels("api", self.chain_key).time():
                response = await self.transport.request(
                    method, f"{self.base_url}{path}", params=params, json=json
                )
            # Label with the templated route, not the concrete path, so per-UID /
            # per-tx URLs do not explode Prometheus series cardinality.
            API_REQUESTS.labels(self.chain_key, route or path, str(response.status)).inc()
            if 200 <= response.status < 300:
                self.limiter.speed_up()
                return response.data if response.data is not None else response.text
            if allow_404 and response.status == 404:
                return None
            if response.status not in retryable:
                break
            if response.status in throttling:
                self.limiter.slow_down()
            retry_after = response.headers.get("retry-after")
            delay = float(retry_after) if retry_after and retry_after.isdigit() else 2**attempt
            await asyncio.sleep(min(30.0, delay + random.random() * 0.2))
        assert response is not None
        raise CowApiError(response.status, path, response.text[:500])

    async def get_orders_by_uids(self, order_uids: Iterable[str]) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        normalized = [normalize_order_uid(uid) for uid in order_uids]
        for batch in batched(normalized, 128):
            rows = await self._request("POST", "/api/v1/orders/by_uids", json=batch)
            output.extend(row["order"] for row in rows if "order" in row)
        return output

    async def get_order(self, order_uid: str) -> dict[str, Any] | None:
        uid = normalize_order_uid(order_uid)
        return await self._request(
            "GET", f"/api/v1/orders/{uid}", allow_404=True, route="/api/v1/orders/{uid}"
        )

    async def get_order_status(self, order_uid: str) -> dict[str, Any] | None:
        uid = normalize_order_uid(order_uid)
        return await self._request(
            "GET",
            f"/api/v1/orders/{uid}/status",
            allow_404=True,
            route="/api/v1/orders/{uid}/status",
        )

    async def get_orders_by_transaction(self, tx_hash: str) -> list[dict[str, Any]]:
        tx = normalize_hash(tx_hash)
        return await self._request(
            "GET", f"/api/v1/transactions/{tx}/orders", route="/api/v1/transactions/{tx}/orders"
        )

    async def iter_account_orders(
        self, owner: str, *, limit: int = 1000
    ) -> AsyncIterator[list[dict[str, Any]]]:
        address = normalize_address(owner)
        offset = 0
        while True:
            rows = await self._request(
                "GET",
                f"/api/v1/account/{address}/orders",
                params={"offset": offset, "limit": limit},
                route="/api/v1/account/{address}/orders",
            )
            if rows:
                yield rows
            if len(rows) < limit:
                return
            offset += len(rows)

    async def iter_trades(
        self,
        *,
        owner: str | None = None,
        order_uid: str | None = None,
        limit: int = 1000,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        if (owner is None) == (order_uid is None):
            raise ValueError("exactly one of owner or order_uid is required")
        selector = (
            {"owner": normalize_address(owner)}
            if owner
            else {"orderUid": normalize_order_uid(order_uid or "")}
        )
        offset = 0
        while True:
            rows = await self._request(
                "GET", "/api/v2/trades", params={**selector, "offset": offset, "limit": limit}
            )
            if rows:
                yield rows
            if len(rows) < limit:
                return
            offset += len(rows)

    async def latest_competition(self) -> dict[str, Any] | None:
        return await self._request("GET", "/api/v2/solver_competition/latest", allow_404=True)

    async def competition_by_transaction(self, tx_hash: str) -> dict[str, Any]:
        tx = normalize_hash(tx_hash)
        result = await self._request(
            "GET",
            f"/api/v2/solver_competition/by_tx_hash/{tx}",
            allow_404=True,
            route="/api/v2/solver_competition/by_tx_hash/{tx}",
        )
        if not result:
            raise CompetitionUnavailable(tx)
        return result

    async def app_data(self, app_data_hash: str) -> dict[str, Any] | None:
        value = normalize_hash(app_data_hash)
        return await self._request(
            "GET", f"/api/v1/app_data/{value}", allow_404=True, route="/api/v1/app_data/{hash}"
        )

    async def native_price(self, token: str) -> dict[str, Any] | None:
        address = normalize_address(token)
        return await self._request(
            "GET",
            f"/api/v1/token/{address}/native_price",
            allow_404=True,
            route="/api/v1/token/{address}/native_price",
        )

    async def version(self) -> str:
        return str(await self._request("GET", "/api/v1/version"))
