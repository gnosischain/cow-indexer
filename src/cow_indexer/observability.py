from __future__ import annotations

import logging
import os
from collections.abc import Awaitable, Callable

import structlog
from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

RPC_REQUESTS = Counter("cow_rpc_requests_total", "RPC requests", ["chain", "method", "status"])
API_REQUESTS = Counter("cow_api_requests_total", "CoW API requests", ["chain", "route", "status"])
ROWS_WRITTEN = Counter("cow_rows_written_total", "Rows written", ["chain", "table"])
CHAIN_LAG = Gauge("cow_chain_lag_blocks", "Safe head minus checkpoint", ["chain"])
WORK_QUEUE = Gauge("cow_work_queue_items", "Pending work items", ["chain"])
REQUEST_LATENCY = Histogram("cow_request_seconds", "External request latency", ["source", "chain"])


def configure_logging() -> None:
    level = os.getenv("COW_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(level=level, format="%(message)s")
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level)),
    )


class HealthServer:
    def __init__(
        self,
        host: str,
        port: int,
        readiness: Callable[[], Awaitable[bool]],
    ) -> None:
        self.host = host
        self.port = port
        self.readiness = readiness
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/health", self._health)
        app.router.add_get("/ready", self._ready)
        app.router.add_get("/metrics", self._metrics)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()

    async def close(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()

    async def _health(self, _: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def _ready(self, _: web.Request) -> web.Response:
        ready = await self.readiness()
        return web.json_response(
            {"status": "ready" if ready else "not-ready"}, status=200 if ready else 503
        )

    async def _metrics(self, _: web.Request) -> web.Response:
        return web.Response(body=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})
