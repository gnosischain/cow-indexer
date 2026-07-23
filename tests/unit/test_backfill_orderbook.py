"""Historical orderbook backfill: embedded-validTo decoding, canonical uid-batch
keys, seeding (SQL-side anti-join, batching, idempotent re-seed), and a drain that
leases only the backfill kinds and tolerates per-uid misses from by_uids."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from cow_indexer.config import ClickHouseConfig, RuntimeConfig, load_config
from cow_indexer.models import BACKFILL_WORK_KINDS, WorkItem
from cow_indexer.services.backfill_orderbook import (
    UID_BATCH_SIZE,
    BackfillOrderbookService,
    decode_uid_batch_key,
    decode_valid_to,
    encode_uid_batch_key,
)
from cow_indexer.storage.clickhouse import ORDER_UID_VALID_TO_SQL, ClickHouseStore
from cow_indexer.utils import sha256_json

ROOT = Path(__file__).parents[2]
# Real mainnet uid whose embedded validTo is 0x610ab36f = 1628091247 = 2021-08-04.
HISTORIC_UID = (
    "0x81dbb9631c0bec7d9762e170ff5feb2c9b366d5ca9502036d20d99c91cafb8bf"
    "b54872859733a3dfbb2c5401ac68cd9ca84b3cd1610ab36f"
)


def uid(index: int) -> str:
    return "0x" + index.to_bytes(56, "big").hex()


def _chain():
    return load_config(ROOT / "config" / "chains.yaml").select("sepolia")[0]


def work(kind: str, key: str) -> WorkItem:
    return WorkItem(work_id="w-" + key[:16], environment="testnet", chain_id=11155111, kind=kind, key=key)


# --- validTo decoding -----------------------------------------------------------


def test_valid_to_decode_matches_real_uid() -> None:
    assert decode_valid_to(HISTORIC_UID) == 1628091247
    assert datetime.fromtimestamp(1628091247, UTC).date() == date(2021, 8, 4)


def test_valid_to_offsets_agree_between_python_and_sql() -> None:
    # uid = '0x' + 112 hex chars; the validTo hex is the LAST 8 chars = Python slice
    # [106:114] = 1-based SQL substring(order_uid, 107, 8).
    assert len(HISTORIC_UID) == 114
    assert HISTORIC_UID[106:114] == HISTORIC_UID[-8:] == "610ab36f"
    assert "substring(order_uid, 107, 8)" in ORDER_UID_VALID_TO_SQL


# --- batch key encoding ---------------------------------------------------------


def test_batch_key_round_trips_and_is_set_canonical() -> None:
    uids = [uid(3), uid(1), uid(2)]
    key = encode_uid_batch_key(uids)
    assert decode_uid_batch_key(key) == sorted(uids)
    # Same set in any order -> same key -> same sha256 work_id: re-seeds are no-ops.
    assert encode_uid_batch_key(reversed(uids)) == key


def test_batch_key_rejects_empty_and_oversized_batches() -> None:
    with pytest.raises(ValueError):
        encode_uid_batch_key([])
    with pytest.raises(ValueError):
        encode_uid_batch_key(uid(index) for index in range(UID_BATCH_SIZE + 1))


# --- fakes ----------------------------------------------------------------------


class FakeStore:
    def __init__(
        self,
        *,
        missing_uids: list[str] | None = None,
        owners: list[str] | None = None,
        total_traded: int = 0,
        lease_batches: list[list[WorkItem]] | None = None,
    ) -> None:
        self.missing_uids = missing_uids or []
        self.owners = owners or []
        self.total_traded = total_traded
        self.lease_batches = list(lease_batches or [])
        self.stream_limits: list[int | None] = []
        self.enqueued: list[tuple[str, str, dict | None]] = []
        self.enqueue_calls = 0
        self.leases: list[tuple[str, int, tuple[str, ...] | None]] = []
        self.finished: list[tuple[str, bool, Any, dict]] = []
        self.stored: list[tuple[list[str], str]] = []

    async def stream_missing_traded_order_uids(self, chain, limit=None):
        self.stream_limits.append(limit)
        uids = self.missing_uids[:limit] if limit is not None else self.missing_uids
        for start in range(0, len(uids), 7):  # odd block size exercises the buffer
            yield uids[start : start + 7]

    async def count_distinct_traded_order_uids(self, chain):
        return self.total_traded

    async def distinct_trade_owners(self, chain, limit=None):
        return self.owners[:limit] if limit is not None else self.owners

    async def enqueue_work_many(self, chain, items):
        self.enqueue_calls += 1
        self.enqueued.extend(items)

    async def lease_work(self, chain, worker, limit, kinds=None):
        self.leases.append((chain.key, limit, tuple(kinds) if kinds else None))
        return self.lease_batches.pop(0) if self.lease_batches else []

    async def finish_work(self, item, success, error=None, retry_at=None, terminal=None):
        self.finished.append((item.kind, success, retry_at, dict(item.payload)))

    async def store_orders(self, chain, rows, source):
        self.stored.append(([row.get("uid") for row in rows], source))

    async def backfill_work_counts(self, chain):
        return {}

    async def coverage(self, chain):
        return {"chain": chain.key, "orders": []}


class FakeApi:
    def __init__(
        self,
        *,
        present: set[str] | None = None,
        owner_pages: list[list[dict]] | None = None,
        fail: Exception | None = None,
    ) -> None:
        self.present = present  # None -> every requested uid exists
        self.owner_pages = owner_pages or []
        self.fail = fail
        self.by_uids_calls: list[list[str]] = []
        self.closed = False

    async def get_orders_by_uids(self, uids):
        uids = list(uids)
        self.by_uids_calls.append(uids)
        if self.fail:
            raise self.fail
        return [
            {"uid": value} for value in uids if self.present is None or value in self.present
        ]

    async def iter_account_orders(self, owner, *, limit=1000):
        for page in self.owner_pages:
            yield page

    async def close(self) -> None:
        self.closed = True


def service(store: FakeStore, runtime: RuntimeConfig | None = None) -> BackfillOrderbookService:
    return BackfillOrderbookService(store, runtime or RuntimeConfig())


# --- seeding --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_seed_orders_batches_newest_first_and_reports_counts() -> None:
    missing = [uid(index) for index in range(300, 0, -1)]  # stream is newest-first
    store = FakeStore(missing_uids=missing, total_traded=450)
    result = await service(store).seed_orders(_chain(), batch_size=128)

    assert result["seeded_uids"] == 300
    assert result["batches"] == 3
    assert result["skipped_existing"] == 150  # 450 traded - 300 missing = anti-joined
    assert [kind for kind, _, _ in store.enqueued] == ["order_uids_batch"] * 3
    decoded = [decode_uid_batch_key(key) for _, key, _ in store.enqueued]
    assert [len(batch) for batch in decoded] == [128, 128, 44]
    # Batch composition follows the stream (newest validTo first), sorted within the
    # batch so the key is canonical.
    assert set(decoded[0]) == set(missing[:128])
    assert decoded[0] == sorted(missing[:128])
    assert [payload for _, _, payload in store.enqueued] == [
        {"uids": 128},
        {"uids": 128},
        {"uids": 44},
    ]


@pytest.mark.asyncio
async def test_seed_orders_passes_limit_and_skips_total_count() -> None:
    store = FakeStore(missing_uids=[uid(index) for index in range(10)], total_traded=999)
    result = await service(store).seed_orders(_chain(), limit=5, batch_size=3)
    assert store.stream_limits == [5]
    assert result["seeded_uids"] == 5
    assert result["batches"] == 2
    assert result["skipped_existing"] is None  # not attributable under a --limit


@pytest.mark.asyncio
async def test_seed_owners_enqueues_every_distinct_owner() -> None:
    owners = ["0x" + f"{index:040x}" for index in range(5)]
    store = FakeStore(owners=owners)
    result = await service(store).seed_owners(_chain())
    assert result["seeded_owners"] == 5
    assert store.enqueued == [("owner_orders_backfill", owner, None) for owner in owners]


@pytest.mark.asyncio
async def test_reseed_produces_identical_work_ids() -> None:
    """Same uid set (any order) -> same canonical key -> same work_id row at revision
    0, which loses the ReplacingMergeTree merge against a terminal revision: re-seeding
    completed work is a no-op by design."""

    class _RecordingClient:
        def __init__(self) -> None:
            self.inserts: list[tuple[str, list, list]] = []

        async def insert(self, table, data, column_names=None):
            self.inserts.append((table, data, column_names))

    client = _RecordingClient()
    store = ClickHouseStore(ClickHouseConfig.from_env(), ROOT)
    store.client = client  # bypass connect()
    chain = _chain()

    batch = [uid(2), uid(1), uid(3)]
    key_a = encode_uid_batch_key(batch)
    key_b = encode_uid_batch_key(reversed(batch))
    await store.enqueue_work_many(chain, [("order_uids_batch", key_a, None)])
    await store.enqueue_work_many(chain, [("order_uids_batch", key_b, None)])

    rows = [
        dict(zip(columns, row, strict=True))
        for _, data, columns in client.inserts
        for row in data
    ]
    assert len(rows) == 2
    assert rows[0]["work_id"] == rows[1]["work_id"]
    expected = sha256_json([chain.environment, chain.chain_id, "order_uids_batch", key_a])
    assert rows[0]["work_id"] == expected
    assert all(row["revision"] == 0 for row in rows)  # never clobbers a terminal row


# --- processors -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_uid_batch_processor_counts_missing_without_failing() -> None:
    uids = [uid(1), uid(2), uid(3)]
    item = work("order_uids_batch", encode_uid_batch_key(uids))
    store = FakeStore()
    api = FakeApi(present={uid(1), uid(3)})  # uid(2) aged out of the public API

    await service(store)._process_and_finish(_chain(), api, item)

    assert len(api.by_uids_calls) == 1  # ONE by_uids call for the whole batch
    assert sorted(api.by_uids_calls[0]) == uids
    assert store.stored == [([uid(1), uid(3)], "backfill")]
    kind, success, retry_at, payload = store.finished[0]
    assert (kind, success, retry_at) == ("order_uids_batch", True, None)
    assert payload["found"] == 2
    assert payload["missing"] == 1  # missing uids are data, not a batch failure


@pytest.mark.asyncio
async def test_owner_processor_stores_pages_up_to_the_cap() -> None:
    owner = "0x" + "22" * 20
    pages = [[{"uid": uid(index)}] for index in range(5)]
    store = FakeStore()
    runtime = RuntimeConfig(backfill_max_pages_per_owner=2)

    await service(store, runtime)._process_and_finish(
        _chain(), FakeApi(owner_pages=pages), work("owner_orders_backfill", owner)
    )

    assert store.stored == [([uid(0)], "backfill"), ([uid(1)], "backfill")]
    kind, success, _, payload = store.finished[0]
    assert (kind, success) == ("owner_orders_backfill", True)
    assert payload == {"orders": 2, "pages": 2, "truncated": True}


@pytest.mark.asyncio
async def test_processor_failures_keep_retry_then_dead_semantics() -> None:
    store = FakeStore()
    api = FakeApi(fail=RuntimeError("boom"))
    svc = service(store)
    retried = work("order_uids_batch", encode_uid_batch_key([uid(1)]))
    retried.attempts = 1
    await svc._process_and_finish(_chain(), api, retried)
    dead = work("order_uids_batch", encode_uid_batch_key([uid(2)]))
    dead.attempts = svc.runtime.max_attempts
    await svc._process_and_finish(_chain(), api, dead)

    (_, ok_a, retry_a, _), (_, ok_b, retry_b, _) = store.finished
    assert (ok_a, ok_b) == (False, False)
    assert retry_a is not None  # below max_attempts -> scheduled retry
    assert retry_b is None  # at max_attempts -> dead


# --- drain ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_drain_leases_only_backfill_kinds_with_dedicated_client(monkeypatch) -> None:
    item = work("order_uids_batch", encode_uid_batch_key([uid(1)]))
    store = FakeStore(lease_batches=[[item]])
    svc = service(store)
    api = FakeApi()
    monkeypatch.setattr(svc, "_client", lambda chain, limiter: api)

    result = await svc.drain([_chain()])

    assert store.leases  # leased at least once, and never without the kind filter
    assert all(kinds == BACKFILL_WORK_KINDS for _, _, kinds in store.leases)
    assert result["processed"] == {"sepolia": 1}
    assert result["stopped"] == "drained"
    assert api.closed  # the dedicated client is torn down with the drain


@pytest.mark.asyncio
async def test_drain_deadline_stops_before_leasing(monkeypatch) -> None:
    store = FakeStore(lease_batches=[[work("order_uids_batch", encode_uid_batch_key([uid(1)]))]])
    svc = service(store)
    monkeypatch.setattr(svc, "_client", lambda chain, limiter: FakeApi())
    result = await svc.drain([_chain()], run_seconds=0.0)
    assert result["stopped"] == "deadline"
    assert store.leases == []  # deadline elapsed before any lease


# --- storage query shapes -------------------------------------------------------


class _FakeStreamContext:
    def __init__(self, blocks: list[list[tuple]]) -> None:
        self._iterator = iter(blocks)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iterator)


class _FakeQueryClient:
    def __init__(self, *, query_rows=None, stream_blocks=None) -> None:
        self._query_rows = list(query_rows or [])
        self.queries: list[tuple[str, dict | None, dict | None]] = []
        self.stream_queries: list[tuple[str, dict | None]] = []
        self._stream_blocks = stream_blocks or []
        self.inserts: list[tuple[str, list]] = []

    async def query(self, sql, parameters=None, settings=None):
        self.queries.append((sql, parameters, settings))
        rows = self._query_rows.pop(0) if self._query_rows else []
        return type("Result", (), {"result_rows": rows})

    async def query_row_block_stream(self, sql, parameters=None, settings=None):
        self.stream_queries.append((sql, parameters))
        return _FakeStreamContext(self._stream_blocks)

    async def insert(self, table, data, column_names=None):
        self.inserts.append((table, data))


def _real_store(client) -> ClickHouseStore:
    store = ClickHouseStore(ClickHouseConfig.from_env(), ROOT)
    store.client = client  # bypass connect()
    return store


@pytest.mark.asyncio
async def test_stream_missing_uids_anti_joins_and_orders_newest_first() -> None:
    client = _FakeQueryClient(stream_blocks=[[(uid(2),), (uid(1),)], [(uid(0),)]])
    store = _real_store(client)

    blocks = [block async for block in store.stream_missing_traded_order_uids(_chain(), limit=3)]

    assert blocks == [[uid(2), uid(1)], [uid(0)]]
    sql, parameters = client.stream_queries[0]
    assert "NOT IN" in sql and ".orders" in sql  # SQL-side anti-join against orders
    assert "GROUP BY order_uid" in sql
    assert f"ORDER BY {ORDER_UID_VALID_TO_SQL} DESC, order_uid" in sql  # newest first
    assert "LIMIT" in sql
    assert parameters["limit"] == 3


@pytest.mark.asyncio
async def test_lease_work_excludes_backfill_kinds_unless_requested() -> None:
    client = _FakeQueryClient(query_rows=[[], []])
    store = _real_store(client)
    chain = _chain()

    await store.lease_work(chain, "worker-1", 10)
    await store.lease_work(chain, "worker-1", 10, kinds=BACKFILL_WORK_KINDS)

    default_sql, default_parameters, _ = client.queries[0]
    assert "kind NOT IN {excluded_kinds:Array(String)}" in default_sql
    assert default_parameters["excluded_kinds"] == list(BACKFILL_WORK_KINDS)
    filtered_sql, filtered_parameters, _ = client.queries[1]
    assert "kind IN {kinds:Array(String)}" in filtered_sql
    assert "NOT IN" not in filtered_sql
    assert filtered_parameters["kinds"] == list(BACKFILL_WORK_KINDS)


# --- config ---------------------------------------------------------------------


def test_backfill_config_defaults(monkeypatch) -> None:
    for name in (
        "COW_BACKFILL_INTERVAL_SECONDS",
        "COW_BACKFILL_MAX_INTERVAL_SECONDS",
        "COW_BACKFILL_CONCURRENCY",
        "COW_BACKFILL_MAX_PAGES_PER_OWNER",
    ):
        monkeypatch.delenv(name, raising=False)
    for runtime in (RuntimeConfig(), RuntimeConfig.from_env()):
        assert runtime.backfill_interval_seconds == 0.5
        assert runtime.backfill_max_interval_seconds == 10.0
        assert runtime.backfill_concurrency == 2
        assert runtime.backfill_max_pages_per_owner == 20


def test_backfill_config_reads_env(monkeypatch) -> None:
    monkeypatch.setenv("COW_BACKFILL_INTERVAL_SECONDS", "1.5")
    monkeypatch.setenv("COW_BACKFILL_MAX_INTERVAL_SECONDS", "30")
    monkeypatch.setenv("COW_BACKFILL_CONCURRENCY", "4")
    monkeypatch.setenv("COW_BACKFILL_MAX_PAGES_PER_OWNER", "5")
    runtime = RuntimeConfig.from_env()
    assert runtime.backfill_interval_seconds == 1.5
    assert runtime.backfill_max_interval_seconds == 30.0
    assert runtime.backfill_concurrency == 4
    assert runtime.backfill_max_pages_per_owner == 5
