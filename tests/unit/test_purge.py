from datetime import UTC, datetime
from pathlib import Path

import pytest

from cow_indexer.config import ClickHouseConfig, load_config
from cow_indexer.storage.clickhouse import ClickHouseStore

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
