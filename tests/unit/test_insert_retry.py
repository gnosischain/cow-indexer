"""Regression tests for the intermittent `Empty query` (code 62) insert failures.

clickhouse-connect streams an insert as a one-shot body generator that carries the
`INSERT INTO ... FORMAT Native` statement in its first chunk. When the server closes an
expired keep-alive connection, the driver retries once with that already-consumed
generator, so the second attempt posts an empty body and ClickHouse answers
`Code: 62 ... Empty query`. Nothing was written, so _insert must send it again.
"""

from pathlib import Path

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from cow_indexer.config import ClickHouseConfig
from cow_indexer.storage.clickhouse import INSERT_ATTEMPTS, ClickHouseStore

ROOT = Path(__file__).parents[2]

EMPTY_QUERY_ERROR = (
    "Received ClickHouse exception, code: 62, server response: Code: 62. "
    "DB::Exception: Empty query. (SYNTAX_ERROR) (version 26.4.1.2359) "
    "(for url https://p23lvwl5g8.europe-west4.p.gcp.clickhouse.cloud:8443)"
)


class _FlakyClient:
    """Raises the driver's burned-body error for the first `failures` inserts."""

    def __init__(self, failures: int, error: Exception | None = None) -> None:
        self._remaining = failures
        self._error = error or DatabaseError(EMPTY_QUERY_ERROR)
        self.inserts: list[tuple[str, list]] = []

    async def insert(self, table, data, column_names=None, settings=None):
        if self._remaining > 0:
            self._remaining -= 1
            raise self._error
        self.inserts.append((table, data))


def _store(fake) -> ClickHouseStore:
    store = ClickHouseStore(ClickHouseConfig.from_env(), ROOT)
    store.client = fake  # bypass connect()
    return store


ROWS = [{"chain_id": 8453, "order_uid": "0xabc"}]


@pytest.mark.asyncio
async def test_empty_query_insert_is_retried_and_lands() -> None:
    fake = _FlakyClient(failures=1)
    store = _store(fake)
    await store._insert("orders", ROWS)

    assert fake.inserts == [(f"{store.database}.orders", [[8453, "0xabc"]])]


@pytest.mark.asyncio
async def test_empty_query_insert_gives_up_after_the_attempt_budget() -> None:
    fake = _FlakyClient(failures=INSERT_ATTEMPTS)
    with pytest.raises(DatabaseError, match="Empty query"):
        await _store(fake)._insert("orders", ROWS)

    assert fake.inserts == []


@pytest.mark.asyncio
async def test_other_database_errors_are_not_retried() -> None:
    # A real schema/type error must surface on the first attempt, not be masked.
    fake = _FlakyClient(
        failures=1,
        error=OperationalError("Received ClickHouse exception, code: 60, server response: "
                               "Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)"),
    )
    with pytest.raises(OperationalError, match="UNKNOWN_TABLE"):
        await _store(fake)._insert("orders", ROWS)
