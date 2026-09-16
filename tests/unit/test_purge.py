from datetime import UTC, datetime
from pathlib import Path

import pytest

from cow_indexer.config import ClickHouseConfig, load_config
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

    async def insert(self, table, data, column_names=None):
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
