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
REQUEST_LATENCY = Histogram("cow_request_seconds", "External request latency", ["source", "chain"])
# Retention health. A failing purge is invisible in the data until work_items is
# already huge — a rejected DELETE deletes nothing, and lease_work FINAL degrades
# gradually rather than breaking — and error logs alone did not surface an outage that
# ran long enough for the queue to reach 2.9M rows. These are the alertable signals:
# rate(cow_purge_sweeps_total{status="error"}[30m]) catches a raising sweep, and
# increase(cow_work_items_purged_total[6h]) == 0 catches one that stops draining
# without raising at all.
PURGE_SWEEPS = Counter("cow_purge_sweeps_total", "Retention sweeps", ["status"])
WORK_ITEMS_PURGED = Counter(
    "cow_work_items_purged_total", "Terminal work items deleted by retention", ["chain"]
)

# Enrichment batch shape. run_once returned len(items) and every caller discarded it, so
# a batch that leased 200 and a batch that leased 0 looked identical from outside -- which
# is why a live-ingestion stall (5-9 items/min against 260K leaseable, 2026-09-16) could
# not be attributed without redeploying. ENRICH_BATCH_SECONDS times the whole batch,
# ENRICH_PREFETCH_SECONDS the batched by_uids call alone; the difference is per-item work.
ENRICH_BATCH_SECONDS = Histogram(
    "cow_enrich_batch_seconds", "run_once wall time", ["chain"],
    buckets=(0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600),
)
ENRICH_PREFETCH_SECONDS = Histogram(
    "cow_enrich_prefetch_seconds", "batched by_uids wall time", ["chain"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)
ENRICH_ITEMS = Counter(
    "cow_enrich_items_total", "Work items leased and their outcome", ["chain", "outcome"]
)
ENRICH_BATCH_TIMEOUTS = Counter(
    "cow_enrich_batch_timeouts_total", "Batches abandoned at the deadline", ["chain"]
)


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
