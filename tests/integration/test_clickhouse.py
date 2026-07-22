import hashlib
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cow_indexer.config import ClickHouseConfig, load_config
from cow_indexer.models import BlockHeader, DecodedEvent
from cow_indexer.services.export_import import ExportImportService
from cow_indexer.services.validation import ValidationService
from cow_indexer.storage.clickhouse import ClickHouseStore

pytestmark = pytest.mark.skipif(
    os.getenv("COW_RUN_INTEGRATION") != "1", reason="set COW_RUN_INTEGRATION=1"
)
ROOT = Path(__file__).parents[2]


@pytest.mark.asyncio
async def test_migrations_are_idempotent_against_clickhouse() -> None:
    store = await ClickHouseStore(ClickHouseConfig.from_env(), ROOT).connect()
    try:
        await store.migrate()
        assert await store.migrate() == []
        assert await store.ping()
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_export_import_resumes_and_quarantines_conflicts(tmp_path: Path) -> None:
    store = await ClickHouseStore(ClickHouseConfig.from_env(), ROOT).connect()
    test_id = uuid.uuid4().hex
    chain = (
        load_config(ROOT / "config" / "chains.yaml")
        .select("sepolia")[0]
        .model_copy(update={"environment": f"import-{test_id}"})
    )
    owner = "0x" + "10" * 20
    valid_to = 1_900_000_000
    uid = (
        "0x"
        + (
            bytes.fromhex(test_id) * 2 + bytes.fromhex(owner[2:]) + valid_to.to_bytes(4, "big")
        ).hex()
    )

    def make_bundle(path: Path, buy_amount: int) -> None:
        orders = path / "orders"
        orders.mkdir(parents=True)
        parquet = orders / "part.parquet"
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "order_uid": uid,
                        "owner": owner,
                        "sell_token": "0x" + "20" * 20,
                        "buy_token": "0x" + "30" * 20,
                        "sell_amount": "100",
                        "buy_amount": str(buy_amount),
                        "valid_to": valid_to,
                        "app_data_hash": "0x" + "40" * 32,
                        "kind": "sell",
                        "partially_fillable": False,
                        "creation_date": datetime.now(UTC),
                    }
                ]
            ),
            parquet,
        )
        digest = hashlib.sha256(parquet.read_bytes()).hexdigest()
        (path / "manifest.json").write_text(
            json.dumps(
                {
                    "format_version": 1,
                    "bundle_id": str(uuid.uuid4()),
                    "source": "integration",
                    "source_schema_version": "test",
                    "environment": chain.environment,
                    "network": chain.key,
                    "chain_id": chain.chain_id,
                    "snapshot_at": datetime.now(UTC).isoformat(),
                    "files": [
                        {
                            "dataset": "orders",
                            "path": "orders/part.parquet",
                            "rows": 1,
                            "sha256": digest,
                        }
                    ],
                }
            )
        )

    try:
        await store.migrate()
        first = tmp_path / "first"
        second = tmp_path / "second"
        make_bundle(first, 90)
        _, stats = await ExportImportService(store, chain).run(first)
        assert stats.accepted == 1
        _, resumed = await ExportImportService(store, chain).run(first)
        assert resumed.model_dump() == {
            "accepted": 0,
            "duplicates": 0,
            "rejected": 0,
            "conflicts": 0,
        }
        make_bundle(second, 80)
        _, conflict = await ExportImportService(store, chain).run(second)
        assert conflict.conflicts == 1
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_checkpoint_queue_and_reconciliation_round_trip() -> None:
    store = await ClickHouseStore(ClickHouseConfig.from_env(), ROOT).connect()
    test_id = uuid.uuid4().hex
    chain = (
        load_config(ROOT / "config" / "chains.yaml")
        .select("sepolia")[0]
        .model_copy(update={"environment": f"integration-{test_id}"})
    )
    owner = "0x" + "11" * 20
    sell_token = "0x" + "22" * 20
    buy_token = "0x" + "33" * 20
    valid_to = 1_900_000_000
    uid = (
        "0x"
        + (
            bytes.fromhex(test_id) * 2 + bytes.fromhex(owner[2:]) + valid_to.to_bytes(4, "big")
        ).hex()
    )
    tx_hash = "0x" + test_id * 2
    block_hash = "0x" + "66" * 32
    now = datetime.now(UTC)
    try:
        await store.migrate()
        block = BlockHeader(
            number=123,
            block_hash=block_hash,
            parent_hash="0x" + "77" * 32,
            timestamp=now,
        )
        await store.store_blocks(chain, [block])
        await store.checkpoint(chain, block)
        assert await store.get_checkpoint(chain) == 123

        await store.enqueue_work(chain, "order_uid", uid)
        leased = await store.lease_work(chain, "integration-worker", 10)
        assert len(leased) == 1
        await store.finish_work(leased[0], True)
        await store.enqueue_work(chain, "order_uid", uid)
        assert await store.lease_work(chain, "integration-worker", 10) == []

        await store.store_orders(
            chain,
            [
                {
                    "uid": uid,
                    "owner": owner,
                    "sellToken": sell_token,
                    "buyToken": buy_token,
                    "sellAmount": "100",
                    "buyAmount": "90",
                    "validTo": valid_to,
                    "appData": "0x" + "88" * 32,
                    "feeAmount": "0",
                    "kind": "sell",
                    "partiallyFillable": False,
                    "creationDate": now.isoformat(),
                    "status": "open",
                    "class": "market",
                }
            ],
            "integration",
        )
        await store.store_event(
            DecodedEvent(
                environment=chain.environment,
                chain_id=chain.chain_id,
                contract_name="GPv2Settlement",
                contract_address="0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                event_name="Trade",
                args={
                    "owner": owner,
                    "sellToken": sell_token,
                    "buyToken": buy_token,
                    "sellAmount": 100,
                    "buyAmount": 90,
                    "feeAmount": 1,
                    "orderUid": uid,
                },
                block_number=123,
                block_hash=block_hash,
                transaction_hash=tx_hash,
                transaction_index=1,
                log_index=2,
                block_timestamp=now,
            )
        )
        await store.store_event(
            DecodedEvent(
                environment=chain.environment,
                chain_id=chain.chain_id,
                contract_name="GPv2Settlement",
                contract_address="0x9008d19f58aabd9ed0d60971565aa8510560ab41",
                event_name="Settlement",
                args={"solver": "0x" + "99" * 20},
                block_number=123,
                block_hash=block_hash,
                transaction_hash=tx_hash,
                transaction_index=1,
                log_index=3,
                block_timestamp=now,
            )
        )
        await store.store_competition(
            chain,
            {"auctionId": 42, "transactionHash": tx_hash, "winner": "0x" + "99" * 20},
            "integration",
        )
        results = await ValidationService(store).validate_chain(chain)
        assert all(result.passed for result in results)
        assert (await store.coverage(chain))["orders"][0]["rows"] == 1
        assert set(await store.known_tokens(chain)) == {sell_token, buy_token}
        assert await store.active_order_uids(chain) == [uid]

        canonical = await store.client.query(
            f"SELECT count() FROM {store.quoted_database}.trades_canonical "
            "WHERE environment={environment:String}",
            parameters={"environment": chain.environment},
        )
        assert canonical.result_rows[0][0] == 1
        await store.store_blocks(
            chain,
            [
                BlockHeader(
                    number=123,
                    block_hash="0x" + "aa" * 32,
                    parent_hash=block.parent_hash,
                    timestamp=now,
                )
            ],
        )
        canonical = await store.client.query(
            f"SELECT count() FROM {store.quoted_database}.trades_canonical "
            "WHERE environment={environment:String}",
            parameters={"environment": chain.environment},
        )
        assert canonical.result_rows[0][0] == 0
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_purge_trims_terminal_work_and_allows_rediscovery() -> None:
    store = await ClickHouseStore(ClickHouseConfig.from_env(), ROOT).connect()
    test_id = uuid.uuid4().hex
    chain = (
        load_config(ROOT / "config" / "chains.yaml")
        .select("sepolia")[0]
        .model_copy(update={"environment": f"purge-{test_id}"})
    )
    done_uid = "0x" + "a1" * 56
    open_uid = "0x" + "b2" * 56
    try:
        await store.migrate()

        # One item taken through pending -> running -> done (separate inserts, so its
        # versions land in separate parts). The pending item is enqueued afterwards so
        # it is not swept up in the same lease.
        await store.enqueue_work(chain, "order_uid", done_uid)
        leased = await store.lease_work(chain, "purge-worker", 10)
        done_item = next(item for item in leased if item.key == done_uid.lower())
        await store.finish_work(done_item, True)
        await store.enqueue_work(chain, "order_uid", open_uid)

        # Cutoff in the future so the just-written terminal row qualifies as "aged".
        cutoff = datetime.now(UTC) + timedelta(minutes=5)
        purged = await store.purge_finished_work(chain, cutoff, batch=50_000)
        assert purged == 1  # only the done item; the pending one is untouched

        # Every version of the done work_id is gone...
        remaining = await store.client.query(
            f"SELECT count() FROM {store.quoted_database}.work_items "
            "WHERE environment={environment:String} AND chain_id={chain_id:UInt64} "
            "AND key={key:String}",
            parameters={
                "environment": chain.environment,
                "chain_id": chain.chain_id,
                "key": done_uid.lower(),
            },
        )
        assert remaining.result_rows[0][0] == 0

        # ...the still-open item survives and remains leaseable...
        released = await store.lease_work(chain, "purge-worker", 10)
        assert [item.key for item in released] == [open_uid.lower()]

        # ...and re-discovering the purged uid re-creates a fresh, leaseable item
        # (finite-window dedup, not permanent completion).
        await store.enqueue_work(chain, "order_uid", done_uid)
        rediscovered = await store.lease_work(chain, "purge-worker", 10)
        assert done_uid.lower() in {item.key for item in rediscovered}
    finally:
        await store.close()
