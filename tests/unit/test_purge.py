import asyncio
from datetime import UTC, datetime
from pathlib import Path

import pytest

from cow_indexer.config import ClickHouseConfig, RuntimeConfig, load_config
from cow_indexer.storage.clickhouse import PURGE_DELETE_CHUNK, ClickHouseStore

ROOT = Path(__file__).parents[2]


class _FakeResult:
    def __init__(self, rows: list[list]) -> None:
        self.result_rows = rows


class _FakeClient:
    """Records queries/commands and replays canned SELECT results in order."""

    def __init__(self, query_rows: list[list[list]] | None = None) -> None:
        self._query_rows = list(query_rows or [])
        self.queries: list[tuple[str, dict | None, dict | None]] = []
        self.commands: list[tuple[str, dict | None, dict | None]] = []
        self.inserts: list[tuple[str, list]] = []

    async def query(self, sql, parameters=None, settings=None):
        self.queries.append((sql, parameters, settings))
        rows = self._query_rows.pop(0) if self._query_rows else []
        return _FakeResult(rows)

    async def command(self, sql, parameters=None, settings=None):
        self.commands.append((sql, parameters, settings))

    async def insert(self, table, data, column_names=None, settings=None):
        self.inserts.append((table, data))


def _store(fake: _FakeClient) -> ClickHouseStore:
    store = ClickHouseStore(ClickHouseConfig.from_env(), ROOT)
    store.client = fake  # bypass connect()
    return store


def _chain():
    return load_config(ROOT / "config" / "chains.yaml").select("sepolia")[0]


@pytest.mark.asyncio
async def test_purge_selects_bounded_then_deletes_all_versions() -> None:
    fake = _FakeClient(query_rows=[[["id1"], ["id2"]]])
    store = _store(fake)
    cutoff = datetime(2026, 1, 1, tzinfo=UTC)

    purged = await store.purge_finished_work(_chain(), cutoff, batch=50_000)

    assert purged == 2
    # The SELECT is bounded (LIMIT), FINAL-free, and memory-capped.
    select_sql, _, select_settings = fake.queries[0]
    assert "FINAL" not in select_sql
    assert "LIMIT" in select_sql
    assert select_settings == store._final_settings
    # The DELETE removes every version of the selected work_ids and carries the
    # bounded-set + synchronous-lightweight-delete settings.
    assert len(fake.commands) == 1
    delete_sql, _, delete_settings = fake.commands[0]
    assert delete_sql.startswith("DELETE FROM")
    assert "('id1','id2')" in delete_sql
    assert delete_settings == store._purge_settings
    assert delete_settings["lightweight_deletes_sync"] == 2
    # Never force a lightweight_delete_mode: 'lightweight_update_force' requires a
    # block-number column work_items does not have, and ClickHouse rejects the whole
    # statement (code 344) rather than degrading — retention then silently stops.
    assert "lightweight_delete_mode" not in delete_settings


# ClickHouse's default max_query_size. The retention DELETE is POSTed as the request
# body and parsed under this limit, so it is the bound every statement must respect.
MAX_QUERY_SIZE = 256 * 1024


@pytest.mark.asyncio
async def test_purge_delete_statements_fit_max_query_size() -> None:
    """A full 50_000-id batch inlined into one DELETE is ~3.35 MB, which ClickHouse
    rejects with `Max query size exceeded` (code 62) — deleting nothing, every sweep,
    silently. The ids must be chunked so no statement can exceed the server's limit
    however large `batch` is."""
    work_ids = [f"{index:064x}" for index in range(50_000)]
    fake = _FakeClient(query_rows=[[[work_id] for work_id in work_ids]])
    store = _store(fake)

    cutoff = datetime(2026, 1, 1, tzinfo=UTC)
    purged = await store.purge_finished_work(_chain(), cutoff, batch=50_000)

    # The caller still sees one batch of work — chunking is an implementation detail,
    # and the CLI drain loop terminates on `purged < batch`.
    assert purged == 50_000
    # Assertions compare scalars, never the statements themselves: a regression here
    # makes these multi-MB, and pytest rendering that diff is its own kind of hang.
    assert max(len(sql.encode()) for sql, _, _ in fake.commands) < MAX_QUERY_SIZE
    assert all(settings == store._purge_settings for _, _, settings in fake.commands)
    # Chunks are full except the last, which pins the slice arithmetic.
    full, remainder = divmod(50_000, PURGE_DELETE_CHUNK)
    assert [sql.count("','") + 1 for sql, _, _ in fake.commands] == (
        [PURGE_DELETE_CHUNK] * full + ([remainder] if remainder else [])
    )
    # Every selected work_id is deleted exactly once, across all chunks.
    deleted = [
        work_id
        for delete_sql, _, _ in fake.commands
        for work_id in delete_sql.split("IN ('")[1].rstrip("')").split("','")
    ]
    assert len(deleted) == len(work_ids)
    assert len(set(deleted) ^ set(work_ids)) == 0


@pytest.mark.asyncio
async def test_purge_uses_one_statement_below_the_chunk_size() -> None:
    """Chunking must not fragment an ordinary sweep into extra mutations."""
    work_ids = [f"{index:064x}" for index in range(PURGE_DELETE_CHUNK)]
    fake = _FakeClient(query_rows=[[[work_id] for work_id in work_ids]])
    store = _store(fake)

    await store.purge_finished_work(_chain(), datetime(2026, 1, 1, tzinfo=UTC), 50_000)

    assert len(fake.commands) == 1
    assert len(fake.commands[0][0].encode()) < MAX_QUERY_SIZE


@pytest.mark.asyncio
async def test_purge_noop_when_nothing_aged() -> None:
    fake = _FakeClient(query_rows=[[]])
    store = _store(fake)

    purged = await store.purge_finished_work(_chain(), datetime(2026, 1, 1, tzinfo=UTC))

    assert purged == 0
    assert fake.commands == []  # no DELETE issued when nothing is selected


@pytest.mark.asyncio
async def test_lease_work_is_memory_capped_and_gated() -> None:
    fake = _FakeClient(query_rows=[[]])  # no pending work
    store = _store(fake)

    leased = await store.lease_work(_chain(), "worker-1", 20)

    assert leased == []
    _, _, settings = fake.queries[0]
    assert settings == store._final_settings
    assert settings["max_threads"] == store.config.final_query_threads
    # The process-wide FINAL gate was created and bounds concurrency.
    assert store._final_semaphore is not None


@pytest.mark.asyncio
async def test_known_tokens_and_active_orders_are_memory_capped() -> None:
    fake = _FakeClient(query_rows=[[["0xtoken"]], [["0xuid"]]])
    store = _store(fake)
    chain = _chain()

    await store.known_tokens(chain)
    await store.active_order_uids(chain)

    for _, _, settings in fake.queries:
        assert settings == store._final_settings


def test_final_settings_defaults() -> None:
    store = ClickHouseStore(ClickHouseConfig(), ROOT)
    # 1 GiB per-query ceiling (headroom to read a large queue), low threads to keep the
    # FINAL peak bounded; the purge DELETE layers the bounded-set + sync-delete settings.
    assert store._final_settings == {"max_memory_usage": 1024 * 1024 * 1024, "max_threads": 2}
    assert store._purge_settings["max_memory_usage"] == 1024 * 1024 * 1024
    assert store._purge_settings["max_threads"] == 2
    assert store._purge_settings["lightweight_deletes_sync"] == 2
    assert store._purge_settings["max_rows_in_set"] == 50_000
    # The retention DELETE must carry only settings this server accepts; a forced
    # lightweight_delete_mode makes every sweep raise and delete nothing.
    assert "lightweight_delete_mode" not in store._purge_settings


@pytest.mark.asyncio
async def test_live_lane_leases_newest_first_backfill_oldest_first() -> None:
    """The live lane must track the tip; the backfill drain must not.

    Ordering the live lane ascending put newly discovered orders behind every older
    queued item. Measured 2026-09-16: 278K pending whose oldest dated to 09-09 kept
    orders.creation_date frozen for ten hours on six chains while completions ran at
    ~31/min -- the tip was six days of queue away. History belongs to
    backfill-orderbook, which has its own kinds, client and limiter.
    """
    from cow_indexer.models import BACKFILL_WORK_KINDS

    config = load_config(ROOT / "config" / "chains.yaml")
    chain = config.select("sepolia")[0]

    client = _FakeClient(query_rows=[[], []])
    store = ClickHouseStore(ClickHouseConfig(host="h", user="u", password="p", database="cow_db"), ROOT)
    store.client = client

    await store.lease_work(chain, "w", 10)
    live_sql = client.queries[-1][0]
    assert "ORDER BY next_attempt_at DESC" in live_sql, "live lane must be newest-first"

    await store.lease_work(chain, "w", 10, kinds=BACKFILL_WORK_KINDS)
    backfill_sql = client.queries[-1][0]
    assert "ORDER BY next_attempt_at ASC" in backfill_sql, "backfill stays oldest-first"


@pytest.mark.asyncio
async def test_bulk_inserts_do_not_wait_for_flush_but_ledgers_do() -> None:
    """wait_for_async_insert=0 for bulk data; ledgers stay synchronous.

    Measured 2026-09-17 on ClickHouse Cloud (async_insert already on, 1000ms busy
    timeout): every insert cost ~7s uniformly across chains because the client waited
    for the durable flush, and an enrichment item made 46 of them -- ~319s of ~324s.
    work_items is the lease state machine and indexing_checkpoints bounds the committed
    views, so those must still return only after a durable write.
    """
    from cow_indexer.storage.clickhouse import SYNC_INSERT_TABLES

    class _InsertClient(_FakeClient):
        def __init__(self):
            super().__init__()
            self.calls = []
        async def insert(self, table, data, column_names=None, settings=None):
            self.calls.append((table, settings))

    store = ClickHouseStore(ClickHouseConfig(host="h", user="u", password="p", database="cow_db"), ROOT)
    client = _InsertClient()
    store.client = client
    row = {"environment": "production", "chain_id": 1, "x": 1}
    await store._insert("raw_api_payloads", [row])
    await store._insert("orders", [row])
    for ledger in sorted(SYNC_INSERT_TABLES):
        await store._insert(ledger, [row])

    by = dict(client.calls)
    assert by["cow_db.raw_api_payloads"] == {"wait_for_async_insert": 0}
    assert by["cow_db.orders"] == {"wait_for_async_insert": 0}
    for ledger in SYNC_INSERT_TABLES:
        assert by[f"cow_db.{ledger}"] is None, f"{ledger} must stay synchronous"

    # The env/config kill switch restores full waiting everywhere without a rebuild.
    store2 = ClickHouseStore(ClickHouseConfig(host="h", user="u", password="p", database="cow_db", async_insert_wait=True), ROOT)
    client2 = _InsertClient()
    store2.client = client2
    await store2._insert("orders", [row])
    assert dict(client2.calls)["cow_db.orders"] is None


@pytest.mark.asyncio
async def test_enqueue_is_async_ack_but_lease_and_finish_stay_durable() -> None:
    """Classify by WRITE: an enqueue (revision-0 pending, rediscoverable if lost) acks at
    the async buffer even though work_items is a ledger; lease/finish/release stay durable."""
    from cow_indexer.models import WorkItem

    class _InsertClient(_FakeClient):
        def __init__(self):
            super().__init__()
            self.calls = []
        async def insert(self, table, data, column_names=None, settings=None):
            self.calls.append((table, settings))

    chain = load_config(ROOT / "config" / "chains.yaml").select("sepolia")[0]
    store = ClickHouseStore(ClickHouseConfig(host="h", user="u", password="p", database="cow_db"), ROOT)
    client = _InsertClient()
    store.client = client

    await store.enqueue_work_many(chain, [("order_uid", "0x" + "ab" * 56, None)])
    assert client.calls[-1] == ("cow_db.work_items", {"wait_for_async_insert": 0})

    item = WorkItem(work_id="w", environment="production", chain_id=chain.chain_id, kind="order_uid", key="k", attempts=1)
    await store.finish_work(item, True)
    assert client.calls[-1] == ("cow_db.work_items", None), "finish must be durable"
    await store.release_work([item])
    assert client.calls[-1] == ("cow_db.work_items", None), "release must be durable"


@pytest.mark.asyncio
async def test_purge_delete_carries_no_unsupported_setting() -> None:
    """Regression for the silent retention outage of 2026-09-17.

    The sweep set lightweight_delete_mode='lightweight_update_force'. work_items has no
    block-number column, so ClickHouse refused the statement with code 344 and deleted
    nothing; the loop counted the failure, backed off to an hour, and retention stopped
    while work_items grew to 951K rows. Assert the statement carries no delete-mode
    override at all, so it runs under the server's own supported default.
    """
    fake = _FakeClient(query_rows=[[["id1"], ["id2"]]])
    store = _store(fake)
    await store.purge_finished_work(_chain(), datetime(2026, 1, 1, tzinfo=UTC), batch=10)
    assert fake.commands, "no DELETE was issued"
    for _, _, settings in fake.commands:
        assert "lightweight_delete_mode" not in settings


# --- recent-enqueue suppression -------------------------------------------------
# The fan-out re-enqueues ids it already queued (an unfilled order reappears in ~95
# successive auctions). Measured 2026-09-17: 83.8% of enqueue writes were for ids
# already in work_items and changed nothing, and they were most of the pod's DB calls.


class _ColumnRecordingClient(_FakeClient):
    """_FakeClient records (table, data) only; the dedup tests need the column names to
    read a written row back as a dict."""

    def __init__(self) -> None:
        super().__init__()
        self.writes: list[tuple[str, list[dict]]] = []

    async def insert(self, table, data, column_names=None, settings=None):
        await super().insert(table, data, column_names=column_names, settings=settings)
        self.writes.append(
            (table, [dict(zip(column_names, row, strict=True)) for row in data])
        )


def _dedup_store(fake: _FakeClient, ttl: float = 900.0, cap: int = 250_000) -> ClickHouseStore:
    config = ClickHouseConfig.from_env()
    config.enqueue_dedup_ttl_seconds = ttl
    config.enqueue_dedup_max_entries = cap
    store = ClickHouseStore(config, ROOT)
    store.client = fake
    return store


@pytest.mark.asyncio
async def test_repeat_enqueue_is_written_once() -> None:
    fake = _ColumnRecordingClient()
    store = _dedup_store(fake)
    chain = _chain()
    for _ in range(5):
        await store.enqueue_work_many(chain, [("order_uid", "0xabc", None)])
    work_inserts = [rows for table, rows in fake.writes if table.endswith("work_items")]
    assert len(work_inserts) == 1, "the same work_id was written more than once"
    assert len(work_inserts[0]) == 1


@pytest.mark.asyncio
async def test_suppression_does_not_hide_new_work() -> None:
    fake = _ColumnRecordingClient()
    store = _dedup_store(fake)
    chain = _chain()
    await store.enqueue_work_many(chain, [("order_uid", "0xabc", None)])
    await store.enqueue_work_many(
        chain, [("order_uid", "0xabc", None), ("order_uid", "0xdef", None)]
    )
    written = [r["key"] for _, rows in fake.writes for r in rows]
    assert written == ["0xabc", "0xdef"], written


@pytest.mark.asyncio
async def test_suppression_expires_so_rediscovery_still_works() -> None:
    """Rediscovery is the only recovery for a dropped async enqueue and for a purged
    item, so an id must become writable again once the TTL lapses."""
    fake = _ColumnRecordingClient()
    store = _dedup_store(fake, ttl=0.05)
    chain = _chain()
    await store.enqueue_work_many(chain, [("order_uid", "0xabc", None)])
    await asyncio.sleep(0.08)
    await store.enqueue_work_many(chain, [("order_uid", "0xabc", None)])
    assert len([1 for table, _ in fake.writes if table.endswith("work_items")]) == 2


@pytest.mark.asyncio
async def test_dedup_ttl_stays_far_below_the_purge_grace() -> None:
    """The suppression window must never outlive retention's grace, or a purged item
    could be suppressed instead of rediscovered."""
    ch = ClickHouseConfig()
    assert ch.enqueue_dedup_ttl_seconds < RuntimeConfig().purge_grace_hours * 3600 / 10


@pytest.mark.asyncio
async def test_size_cap_evicts_and_only_costs_a_rewrite() -> None:
    fake = _ColumnRecordingClient()
    store = _dedup_store(fake, cap=2)
    chain = _chain()
    await store.enqueue_work_many(chain, [("order_uid", "0xaaa", None)])
    await store.enqueue_work_many(chain, [("order_uid", "0xbbb", None)])
    await store.enqueue_work_many(chain, [("order_uid", "0xccc", None)])  # evicts 0xaaa
    await store.enqueue_work_many(chain, [("order_uid", "0xaaa", None)])  # written again
    keys = [r["key"] for _, rows in fake.writes for r in rows]
    assert keys == ["0xaaa", "0xbbb", "0xccc", "0xaaa"], keys


@pytest.mark.asyncio
async def test_suppression_can_be_disabled() -> None:
    fake = _ColumnRecordingClient()
    store = _dedup_store(fake, ttl=0)
    chain = _chain()
    for _ in range(3):
        await store.enqueue_work_many(chain, [("order_uid", "0xabc", None)])
    assert len([1 for table, _ in fake.writes if table.endswith("work_items")]) == 3
