from __future__ import annotations

import asyncio
import json
from collections.abc import Iterable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import clickhouse_connect
import structlog
from clickhouse_connect.driver import httputil

from cow_indexer.config import ChainConfig, ClickHouseConfig
from cow_indexer.models import BlockHeader, DecodedEvent, ImportStats, RpcLog, WorkItem
from cow_indexer.observability import ROWS_WRITTEN
from cow_indexer.storage.migrations import migration_files, quote_database, split_sql
from cow_indexer.utils import (
    canonical_json,
    normalize_address,
    normalize_auction_order,
    normalize_hash,
    normalize_order_uid,
    parse_datetime,
    sha256_json,
    utcnow,
    validate_order_uid,
)

log = structlog.get_logger()
MAX_REVISION = 2**64 - 1
# Terminal work status for competitions the public API does not serve; sits just
# below `done`/`dead` so it wins the ReplacingMergeTree merge and lease_work skips it.
UNAVAILABLE_REVISION = MAX_REVISION - 2

# The three terminal work statuses. A terminal row always carries one of the top
# revisions above, so a retry (small revision) can never write over one; the presence
# of a terminal-status row therefore means the work_id is terminal. Retention deletes
# every version of such work_ids (see purge_finished_work).
TERMINAL_WORK_STATUSES = ("done", "dead", "unavailable_from_public_api")

# Per-query memory ceiling for continuous-path FINAL reads. Well under the instance
# limit so a single oversized FINAL fails in isolation (and the loop backs off)
# instead of tripping the OvercommitTracker and cascading to ingestion. max_memory_usage
# is per-query/per-server, not an aggregate limit, so it is paired with a process-wide
# semaphore (ClickHouseStore._final_gate) capping concurrent FINAL reads.
FINAL_QUERY_MEMORY = 128 * 1024 * 1024  # 128 MiB
MAX_CONCURRENT_FINAL = 2

# Settings for the retention DELETE: bound the IN-set memory and force a synchronous
# lightweight delete so a batch is fully applied (and its rows masked from the next
# SELECT) before the next batch is chosen — which is what makes the drain terminate.
PURGE_SETTINGS = {
    "max_memory_usage": FINAL_QUERY_MEMORY,
    "max_rows_in_set": 50_000,
    "max_bytes_in_set": 64 * 1024 * 1024,
    "max_threads": 2,
    "lightweight_delete_mode": "lightweight_update_force",
    "lightweight_deletes_sync": 2,
}


def _winning_solution(solutions: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pick the winning solver-competition solution: prefer the explicit isWinner
    flag, then ranking==1, else the last entry (historical worst-to-best order)."""
    if not solutions:
        return None
    for solution in solutions:
        if solution.get("isWinner"):
            return solution
    for solution in solutions:
        if str(solution.get("ranking", "")) == "1":
            return solution
    return solutions[-1]


def _competition_tx_hashes(payload: dict[str, Any]) -> list[str]:
    """Extract and normalize all settlement tx hashes for an auction, tolerating the
    v2 `transactionHashes` array, the legacy singular field, and export snake_case."""
    raw = payload.get("transactionHashes") or payload.get("transaction_hashes") or []
    if isinstance(raw, str):
        raw = [raw]
    if not raw:
        singular = payload.get("transactionHash") or payload.get("transaction_hash")
        raw = [singular] if singular else []
    hashes: list[str] = []
    for value in raw:
        try:
            normalized = normalize_hash(value)
        except (ValueError, TypeError):
            continue
        if normalized not in hashes:
            hashes.append(normalized)
    return hashes


class ClickHouseStore:
    def __init__(self, config: ClickHouseConfig, project_root: Path) -> None:
        self.config = config
        self.project_root = project_root
        self.database = config.database
        self.quoted_database = quote_database(config.database)
        self.client: Any = None
        # Process-wide gate limiting how many continuous-path FINAL reads run at once.
        # Created lazily inside the running loop so it binds to the correct event loop.
        self._final_semaphore: asyncio.Semaphore | None = None

    async def connect(self) -> ClickHouseStore:
        if self.client is None:
            # Enlarge the HTTP connection pool: one shared async client serves all
            # chains, and the default pool of 8 is saturated by concurrent scanners.
            pool_mgr = httputil.get_pool_manager(maxsize=self.config.pool_size, num_pools=4)
            self.client = await clickhouse_connect.get_async_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                database="default",
                secure=self.config.secure,
                pool_mgr=pool_mgr,
            )
        return self

    async def close(self) -> None:
        if self.client is not None:
            await self.client.close()
            self.client = None

    async def ping(self) -> bool:
        try:
            await self._ensure()
            result = await self.client.query("SELECT 1")
            return result.result_rows == [(1,)]
        except Exception:
            return False

    async def migrate(self) -> list[str]:
        await self._ensure()
        table_exists = await self.client.query(
            "SELECT count() FROM system.tables WHERE database = {db:String} AND name = 'schema_migrations'",
            parameters={"db": self.database},
        )
        applied: set[str] = set()
        if table_exists.result_rows[0][0]:
            result = await self.client.query(
                f"SELECT version FROM {self.quoted_database}.schema_migrations FINAL"
            )
            applied = {row[0] for row in result.result_rows}

        completed: list[str] = []
        for path in migration_files(self.project_root / "migrations"):
            version = path.stem
            if version in applied:
                continue
            source = path.read_text().replace("__DATABASE__", self.quoted_database)
            for statement in split_sql(source):
                await self.client.command(statement)
            await self._insert(
                "schema_migrations",
                [{"version": version, "applied_at": utcnow()}],
            )
            completed.append(version)
            log.info("migration_applied", version=version)
        return completed

    async def _ensure(self) -> None:
        if self.client is None:
            await self.connect()

    def _final_gate(self) -> asyncio.Semaphore:
        """Process-wide limiter for concurrent FINAL reads on the continuous path.
        Created on first use so it binds to the active event loop."""
        if self._final_semaphore is None:
            self._final_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FINAL)
        return self._final_semaphore

    async def _insert(self, table: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        await self._ensure()
        columns = list(rows[0])
        data = [[row[column] for column in columns] for row in rows]
        await self.client.insert(f"{self.database}.{table}", data, column_names=columns)
        chain = str(rows[0].get("chain_id", "none"))
        ROWS_WRITTEN.labels(chain, table).inc(len(rows))

    async def store_raw_logs(self, chain: ChainConfig, logs: list[RpcLog]) -> None:
        now = utcnow()
        await self._insert(
            "raw_rpc_logs",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "contract_address": item.address,
                    "topics": [topic.lower() for topic in item.topics],
                    "data": item.data.lower(),
                    "block_number": item.block_number,
                    "block_hash": item.block_hash,
                    "transaction_hash": item.transaction_hash,
                    "transaction_index": item.transaction_index,
                    "log_index": item.log_index,
                    "removed": item.removed,
                    "observed_at": now,
                }
                for item in logs
            ],
        )

    async def store_blocks(self, chain: ChainConfig, blocks: Iterable[BlockHeader]) -> None:
        now = utcnow()
        await self._insert(
            "chain_blocks",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "block_number": block.number,
                    "block_hash": block.block_hash,
                    "parent_hash": block.parent_hash,
                    "block_timestamp": block.timestamp,
                    "observed_at": now,
                }
                for block in blocks
            ],
        )

    def _decoded_event_rows(
        self, event: DecodedEvent, now: datetime
    ) -> dict[str, list[dict[str, Any]]]:
        args = event.args
        rows: dict[str, list[dict[str, Any]]] = {
            "decoded_events": [
                {
                    "environment": event.environment,
                    "chain_id": event.chain_id,
                    "contract_name": event.contract_name,
                    "contract_address": event.contract_address,
                    "event_name": event.event_name,
                    "args": canonical_json(event.args),
                    "block_number": event.block_number,
                    "block_hash": event.block_hash,
                    "block_timestamp": event.block_timestamp,
                    "transaction_hash": event.transaction_hash,
                    "transaction_index": event.transaction_index,
                    "log_index": event.log_index,
                    "removed": event.removed,
                    "observed_at": now,
                }
            ]
        }
        if event.event_name == "Trade":
            rows["trades"] = [
                {
                    "environment": event.environment,
                    "chain_id": event.chain_id,
                    "order_uid": normalize_order_uid(args["orderUid"]),
                    "tx_hash": event.transaction_hash,
                    "log_index": event.log_index,
                    "block_number": event.block_number,
                    "block_hash": event.block_hash,
                    "block_timestamp": event.block_timestamp,
                    "owner": normalize_address(args["owner"]),
                    "sell_token": normalize_address(args["sellToken"]),
                    "buy_token": normalize_address(args["buyToken"]),
                    "sell_amount": int(args["sellAmount"]),
                    "buy_amount": int(args["buyAmount"]),
                    "fee_amount": int(args["feeAmount"]),
                    "source": "rpc",
                    "raw_payload": canonical_json(args),
                    "observed_at": now,
                }
            ]
        elif event.event_name == "Settlement":
            rows["settlements"] = [
                {
                    "environment": event.environment,
                    "chain_id": event.chain_id,
                    "tx_hash": event.transaction_hash,
                    "block_number": event.block_number,
                    "block_hash": event.block_hash,
                    "block_timestamp": event.block_timestamp,
                    "solver": normalize_address(args["solver"]),
                    "log_index": event.log_index,
                    "observed_at": now,
                }
            ]
        elif event.event_name == "Interaction":
            rows["interactions"] = [
                {
                    "environment": event.environment,
                    "chain_id": event.chain_id,
                    "tx_hash": event.transaction_hash,
                    "block_number": event.block_number,
                    "block_hash": event.block_hash,
                    "block_timestamp": event.block_timestamp,
                    "log_index": event.log_index,
                    "target": normalize_address(args["target"]),
                    "value": int(args["value"]),
                    "selector": args["selector"],
                    "observed_at": now,
                }
            ]

        if event.event_name in {
            "OrderInvalidated",
            "OrderInvalidation",
            "OrderRefund",
            "PreSignature",
            "OrderPlacement",
            "ConditionalOrderCreated",
            "MerkleRootSet",
            "SwapGuardSet",
        }:
            uid = args.get("orderUid", "")
            owner = args.get("owner") or args.get("sender") or ""
            event_id = sha256_json(
                [
                    event.environment,
                    event.chain_id,
                    event.transaction_hash,
                    event.log_index,
                    event.event_name,
                ]
            )
            rows["order_events"] = [
                {
                    "event_id": event_id,
                    "environment": event.environment,
                    "chain_id": event.chain_id,
                    "order_uid": normalize_order_uid(uid) if uid else "",
                    "owner": normalize_address(owner) if owner else "",
                    "event_type": event.event_name,
                    "source": "rpc",
                    "block_number": event.block_number,
                    "transaction_hash": event.transaction_hash,
                    "log_index": event.log_index,
                    "event_timestamp": event.block_timestamp,
                    "payload": canonical_json(args),
                    "observed_at": now,
                }
            ]
        return rows

    async def store_event(self, event: DecodedEvent) -> None:
        await self.store_events([event])

    async def store_events(self, events: list[DecodedEvent]) -> None:
        """Batch the decoded-event rows for a whole scanned range into one INSERT
        per target table instead of one INSERT per log."""
        if not events:
            return
        now = utcnow()
        batches: dict[str, list[dict[str, Any]]] = {}
        for event in events:
            for table, rows in self._decoded_event_rows(event, now).items():
                batches.setdefault(table, []).extend(rows)
        for table in ("decoded_events", "trades", "settlements", "interactions", "order_events"):
            if batches.get(table):
                await self._insert(table, batches[table])

    async def enqueue_work(
        self, chain: ChainConfig, kind: str, key: str, payload: dict[str, Any] | None = None
    ) -> None:
        await self.enqueue_work_many(chain, [(kind, key, payload)])

    async def enqueue_work_many(
        self,
        chain: ChainConfig,
        items: Iterable[tuple[str, str, dict[str, Any] | None]],
    ) -> None:
        now = utcnow()
        rows: dict[str, dict[str, Any]] = {}
        for kind, key, payload in items:
            normalized_key = key.lower()
            work_id = sha256_json([chain.environment, chain.chain_id, kind, normalized_key])
            # Dedup within the batch; a range often enqueues the same token / owner
            # many times. ReplacingMergeTree keeps the highest revision anyway, but a
            # fresh revision-0 row must not clobber a terminal one, so keep only one
            # revision-0 row per work_id per batch.
            rows[work_id] = {
                "work_id": work_id,
                "environment": chain.environment,
                "chain_id": chain.chain_id,
                "kind": kind,
                "key": normalized_key,
                "payload": canonical_json(payload or {}),
                "status": "pending",
                "attempts": 0,
                "lease_owner": "",
                "lease_until": None,
                "next_attempt_at": now,
                "error": "",
                "revision": 0,
                "observed_at": now,
            }
        await self._insert("work_items", list(rows.values()))

    async def checkpoint(self, chain: ChainConfig, block: BlockHeader) -> None:
        current = await self.get_checkpoint(chain)
        if current is not None and block.number < current:
            return
        await self._insert(
            "indexing_checkpoints",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "source": "rpc",
                    "block_number": block.number,
                    "block_hash": block.block_hash,
                    "updated_at": utcnow(),
                }
            ],
        )

    async def record_range(
        self,
        chain: ChainConfig,
        run_id: str,
        from_block: int,
        to_block: int,
        rows: int,
        status: str,
        started_at: datetime,
        error: str = "",
    ) -> None:
        await self._insert(
            "indexing_ranges",
            [
                {
                    "run_id": run_id,
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "from_block": from_block,
                    "to_block": to_block,
                    "rows": rows,
                    "status": status,
                    "error": error,
                    "started_at": started_at,
                    "finished_at": utcnow(),
                }
            ],
        )

    async def get_checkpoint(self, chain: ChainConfig) -> int | None:
        await self._ensure()
        result = await self.client.query(
            f"SELECT block_number FROM {self.quoted_database}.indexing_checkpoints FINAL "
            "WHERE environment = {environment:String} AND chain_id = {chain_id:UInt64} "
            "AND source = 'rpc' LIMIT 1",
            parameters={"environment": chain.environment, "chain_id": chain.chain_id},
        )
        return int(result.result_rows[0][0]) if result.result_rows else None

    async def store_api_payload(
        self, chain: ChainConfig, endpoint: str, source_key: str, payload: Any
    ) -> None:
        await self._insert(
            "raw_api_payloads",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "endpoint": endpoint,
                    "source_key": source_key.lower(),
                    "payload": canonical_json(payload),
                    "payload_hash": sha256_json(payload),
                    "observed_at": utcnow(),
                }
            ],
        )

    async def store_orders(
        self, chain: ChainConfig, rows: list[dict[str, Any]], source: str
    ) -> None:
        now = utcnow()
        normalized: list[dict[str, Any]] = []
        events: list[dict[str, Any]] = []
        for row in rows:
            owner = normalize_address(row["owner"])
            valid_to = int(row["validTo"])
            uid = validate_order_uid(row["uid"], owner, valid_to)
            app_data = row.get("appDataHash") or row.get("appData") or ""
            app_data_hash = (
                normalize_hash(app_data)
                if isinstance(app_data, str) and len(app_data) == 66
                else ""
            )
            creation_date = parse_datetime(row.get("creationDate")) or now
            immutable = {
                "uid": uid,
                "owner": owner,
                "sellToken": normalize_address(row["sellToken"]),
                "buyToken": normalize_address(row["buyToken"]),
                "receiver": normalize_address(row["receiver"]) if row.get("receiver") else None,
                "sellAmount": str(row["sellAmount"]),
                "buyAmount": str(row["buyAmount"]),
                "validTo": valid_to,
                "appDataHash": app_data_hash,
                "kind": row["kind"],
                "partiallyFillable": bool(row["partiallyFillable"]),
            }
            normalized.append(
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "order_uid": uid,
                    "owner": owner,
                    "sell_token": immutable["sellToken"],
                    "buy_token": immutable["buyToken"],
                    "receiver": immutable["receiver"],
                    "sell_amount": int(row["sellAmount"]),
                    "buy_amount": int(row["buyAmount"]),
                    "valid_to": valid_to,
                    "app_data_hash": app_data_hash,
                    "fee_amount": int(row.get("feeAmount", 0)),
                    "kind": str(row["kind"]),
                    "partially_fillable": bool(row["partiallyFillable"]),
                    "sell_token_balance": str(row.get("sellTokenBalance", "erc20")),
                    "buy_token_balance": str(row.get("buyTokenBalance", "erc20")),
                    "signing_scheme": str(row.get("signingScheme", "")),
                    "signature": str(row.get("signature", "")),
                    "creation_date": creation_date,
                    "status": str(row.get("status", "unknown")),
                    "class": str(row.get("class", "unknown")),
                    "executed_sell_amount": int(row.get("executedSellAmount", 0)),
                    "executed_buy_amount": int(row.get("executedBuyAmount", 0)),
                    "executed_fee_amount": int(row.get("executedFeeAmount", 0)),
                    "immutable_hash": sha256_json(immutable),
                    "source": source,
                    "raw_payload": canonical_json(row),
                    "source_updated_at": parse_datetime(row.get("lastUpdate")) or now,
                    "observed_at": now,
                }
            )
            if status := row.get("status"):
                events.append(
                    {
                        "event_id": sha256_json(
                            [chain.environment, chain.chain_id, uid, "status", status, source]
                        ),
                        "environment": chain.environment,
                        "chain_id": chain.chain_id,
                        "order_uid": uid,
                        "owner": owner,
                        "event_type": f"status:{status}",
                        "source": source,
                        "block_number": None,
                        "transaction_hash": "",
                        "log_index": None,
                        "event_timestamp": now,
                        "payload": canonical_json({"status": status}),
                        "observed_at": now,
                    }
                )
        await self._insert("orders", normalized)
        await self._insert("order_events", events)

    async def store_api_trades(
        self, chain: ChainConfig, rows: list[dict[str, Any]], source: str
    ) -> None:
        now = utcnow()
        trades: list[dict[str, Any]] = []
        fees: list[dict[str, Any]] = []
        for row in rows:
            tx_hash = normalize_hash(row.get("txHash") or row["transactionHash"])
            uid = normalize_order_uid(row["orderUid"])
            log_index = int(row.get("logIndex", 0))
            protocol_fees = row.get("executedProtocolFees") or []
            trades.append(
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "order_uid": uid,
                    "tx_hash": tx_hash,
                    "log_index": log_index,
                    "block_number": int(row.get("blockNumber", 0)),
                    "block_hash": normalize_hash(row["blockHash"]) if row.get("blockHash") else "",
                    "block_timestamp": parse_datetime(row.get("blockTimestamp")),
                    "owner": normalize_address(row["owner"]),
                    "sell_token": normalize_address(row["sellToken"]),
                    "buy_token": normalize_address(row["buyToken"]),
                    "sell_amount": int(row["sellAmount"]),
                    "buy_amount": int(row["buyAmount"]),
                    "fee_amount": sum(int(fee.get("amount", 0)) for fee in protocol_fees),
                    "source": source,
                    "raw_payload": canonical_json(row),
                    "observed_at": now,
                }
            )
            for index, fee in enumerate(protocol_fees):
                fees.append(
                    {
                        "environment": chain.environment,
                        "chain_id": chain.chain_id,
                        "order_uid": uid,
                        "tx_hash": tx_hash,
                        "log_index": log_index,
                        "fee_index": index,
                        "token": normalize_address(fee.get("token") or row["sellToken"]),
                        "amount": int(fee.get("amount", 0)),
                        "policy": str(fee.get("policy", "")),
                        "source": source,
                        "raw_payload": canonical_json(fee),
                        "observed_at": now,
                    }
                )
        await self._insert("trades", trades)
        await self._insert("protocol_fees", fees)

    async def store_competition(
        self, chain: ChainConfig, payload: dict[str, Any], source: str
    ) -> None:
        now = utcnow()
        auction_id = int(payload.get("auctionId", payload.get("auction_id", 0)))
        solutions = (
            payload.get("solutions") or payload.get("competition", {}).get("solutions") or []
        )
        winning = _winning_solution(solutions)
        tx_hashes = _competition_tx_hashes(payload)

        # Winner address comes from the winning solution (v2 has no top-level winner);
        # fall back to the legacy top-level fields.
        winner_raw = ""
        if winning:
            winner_raw = winning.get("solverAddress") or winning.get("solver") or ""
        if not winner_raw:
            winner_raw = payload.get("winner") or payload.get("winnerAddress") or ""
        winner_tx = ""
        if winning and winning.get("txHash"):
            try:
                winner_tx = normalize_hash(winning["txHash"])
            except (ValueError, TypeError):
                winner_tx = ""
        header_tx = winner_tx or (tx_hashes[0] if tx_hashes else "")

        # reference_score: scalar `referenceScore`, or a per-solver `referenceScores` map.
        reference_score = payload.get("referenceScore")
        if reference_score is None and payload.get("referenceScores") is not None:
            reference_score = canonical_json(payload["referenceScores"])
        reference_score = str(reference_score) if reference_score is not None else ""

        auction_block = int(
            payload.get("auctionStartBlock")
            or payload.get("auction_start_block")
            or payload.get("auction", {}).get("block", 0)
            or 0
        )

        await self._insert(
            "solver_competitions",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "auction_id": auction_id,
                    "tx_hash": header_tx,
                    "winner": normalize_address(winner_raw) if winner_raw else "",
                    "reference_score": reference_score,
                    "auction_block": auction_block,
                    "source": source,
                    "raw_payload": canonical_json(payload),
                    "observed_at": now,
                }
            ],
        )
        await self._insert(
            "competition_transactions",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "auction_id": auction_id,
                    "tx_index": index,
                    "tx_hash": tx_hash,
                    "source": source,
                    "observed_at": now,
                }
                for index, tx_hash in enumerate(tx_hashes)
            ],
        )
        solution_rows = []
        for index, solution in enumerate(solutions):
            solver = solution.get("solverAddress") or solution.get("solver") or ""
            solution_tx = solution.get("txHash") or ""
            try:
                solution_tx = normalize_hash(solution_tx) if solution_tx else ""
            except (ValueError, TypeError):
                solution_tx = ""
            solution_rows.append(
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "auction_id": auction_id,
                    "solution_index": index,
                    "solver": normalize_address(solver) if solver else "",
                    "score": str(solution.get("score", "")),
                    "ranking": int(solution.get("ranking", index + 1)),
                    "is_winner": bool(solution.get("isWinner", solution is winning)),
                    "tx_hash": solution_tx,
                    "payload": canonical_json(solution),
                    "observed_at": now,
                }
            )
        await self._insert("competition_solutions", solution_rows)
        auction = payload.get("auction") or {}
        order_rows = []
        for order in auction.get("orders", []):
            normalized = normalize_auction_order(order)
            if normalized:
                uid, order_payload = normalized
                order_rows.append(
                    {
                        "environment": chain.environment,
                        "chain_id": chain.chain_id,
                        "auction_id": auction_id,
                        "order_uid": uid,
                        "payload": canonical_json(order_payload),
                        "observed_at": now,
                    }
                )
        await self._insert("auction_orders", order_rows)
        await self._insert(
            "auction_prices",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "auction_id": auction_id,
                    "token": normalize_address(token),
                    "price": int(price),
                    "observed_at": now,
                }
                for token, price in (auction.get("prices") or {}).items()
            ],
        )

    async def store_app_data(
        self, chain: ChainConfig, app_data_hash: str, payload: dict[str, Any], source: str
    ) -> None:
        await self._insert(
            "app_data",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "app_data_hash": normalize_hash(app_data_hash),
                    "full_app_data": canonical_json(payload),
                    "source": source,
                    "observed_at": utcnow(),
                }
            ],
        )

    async def store_native_price(
        self, chain: ChainConfig, token: str, payload: dict[str, Any], source: str
    ) -> None:
        price = payload.get("price") if isinstance(payload, dict) else payload
        await self._insert(
            "native_prices",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "token": normalize_address(token),
                    "native_price": str(price),
                    "source": source,
                    "observed_at": utcnow(),
                }
            ],
        )

    async def lease_work(self, chain: ChainConfig, worker_id: str, limit: int) -> list[WorkItem]:
        await self._ensure()
        async with self._final_gate():
            result = await self.client.query(
                f"SELECT work_id, kind, key, payload, attempts "
                f"FROM {self.quoted_database}.work_items FINAL "
                "WHERE environment = {environment:String} AND chain_id = {chain_id:UInt64} "
                "AND ((status = 'pending' AND next_attempt_at <= now64(3)) "
                "OR (status = 'running' AND lease_until < now64(3))) "
                "ORDER BY next_attempt_at LIMIT {limit:UInt32}",
                parameters={
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "limit": limit,
                },
                settings={"max_memory_usage": FINAL_QUERY_MEMORY},
            )
        now = utcnow()
        items: list[WorkItem] = []
        versions: list[dict[str, Any]] = []
        for work_id, kind, key, payload, attempts in result.result_rows:
            attempt = int(attempts) + 1
            item = WorkItem(
                work_id=work_id,
                environment=chain.environment,
                chain_id=chain.chain_id,
                kind=kind,
                key=key,
                payload=json.loads(payload),
                attempts=attempt,
            )
            items.append(item)
            versions.append(
                {
                    "work_id": work_id,
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "kind": kind,
                    "key": key,
                    "payload": payload,
                    "status": "running",
                    "attempts": attempt,
                    "lease_owner": worker_id,
                    "lease_until": now + timedelta(minutes=5),
                    "next_attempt_at": now,
                    "error": "",
                    "revision": attempt * 10 + 1,
                    "observed_at": now,
                }
            )
        await self._insert("work_items", versions)
        return items

    async def finish_work(
        self,
        item: WorkItem,
        success: bool,
        error: str | None = None,
        retry_at: datetime | None = None,
        terminal: str | None = None,
    ) -> None:
        now = utcnow()
        if terminal is not None:
            # Non-error terminal outcome (e.g. unavailable_from_public_api): sticky,
            # not retried, not dead-lettered.
            status = terminal
            revision = UNAVAILABLE_REVISION
        elif success:
            status = "done"
            revision = MAX_REVISION
        elif retry_at is not None:
            status = "pending"
            revision = item.attempts * 10 + 2
        else:
            status = "dead"
            revision = MAX_REVISION - 1
        row = {
            "work_id": item.work_id,
            "environment": item.environment,
            "chain_id": item.chain_id,
            "kind": item.kind,
            "key": item.key,
            "payload": canonical_json(item.payload),
            "status": status,
            "attempts": item.attempts,
            "lease_owner": "",
            "lease_until": None,
            "next_attempt_at": retry_at or now,
            "error": error or "",
            "revision": revision,
            "observed_at": now,
        }
        await self._insert("work_items", [row])
        if status == "dead":
            await self._insert(
                "dead_letters",
                [
                    {
                        "work_id": item.work_id,
                        "environment": item.environment,
                        "chain_id": item.chain_id,
                        "kind": item.kind,
                        "key": item.key,
                        "payload": canonical_json(item.payload),
                        "attempts": item.attempts,
                        "error": error or "",
                        "failed_at": now,
                    }
                ],
            )

    async def active_order_uids(self, chain: ChainConfig, limit: int = 1000) -> list[str]:
        await self._ensure()
        async with self._final_gate():
            result = await self.client.query(
                f"SELECT order_uid FROM {self.quoted_database}.orders FINAL "
                "WHERE environment = {environment:String} AND chain_id = {chain_id:UInt64} "
                "AND status IN ('open', 'presignaturePending') LIMIT {limit:UInt32}",
                parameters={
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "limit": limit,
                },
                settings={"max_memory_usage": FINAL_QUERY_MEMORY},
            )
        return [row[0] for row in result.result_rows]

    async def known_tokens(self, chain: ChainConfig, limit: int = 500) -> list[str]:
        await self._ensure()
        async with self._final_gate():
            result = await self.client.query(
                f"SELECT token FROM ("
                f"SELECT sell_token AS token FROM {self.quoted_database}.orders FINAL "
                "WHERE environment={environment:String} AND chain_id={chain_id:UInt64} "
                "UNION DISTINCT "
                f"SELECT buy_token AS token FROM {self.quoted_database}.orders FINAL "
                "WHERE environment={environment:String} AND chain_id={chain_id:UInt64} "
                "UNION DISTINCT "
                f"SELECT sell_token AS token FROM {self.quoted_database}.trades FINAL "
                "WHERE environment={environment:String} AND chain_id={chain_id:UInt64} "
                "UNION DISTINCT "
                f"SELECT buy_token AS token FROM {self.quoted_database}.trades FINAL "
                "WHERE environment={environment:String} AND chain_id={chain_id:UInt64}) "
                "WHERE token != '' LIMIT {limit:UInt32}",
                parameters={
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "limit": limit,
                },
                settings={"max_memory_usage": FINAL_QUERY_MEMORY},
            )
        return [row[0] for row in result.result_rows]

    async def tokens_with_metadata(self, chain: ChainConfig) -> list[str]:
        await self._ensure()
        result = await self.client.query(
            f"SELECT DISTINCT token FROM {self.quoted_database}.token_metadata "
            "WHERE environment={environment:String} AND chain_id={chain_id:UInt64}",
            parameters={"environment": chain.environment, "chain_id": chain.chain_id},
        )
        return [row[0] for row in result.result_rows]

    async def store_token_metadata(
        self, chain: ChainConfig, token: str, metadata: dict[str, Any], source: str
    ) -> None:
        decimals = max(0, min(255, int(metadata.get("decimals", 0) or 0)))
        await self._insert(
            "token_metadata",
            [
                {
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "token": normalize_address(token),
                    "symbol": str(metadata.get("symbol", "")),
                    "name": str(metadata.get("name", "")),
                    "decimals": decimals,
                    "source": source,
                    "observed_at": utcnow(),
                }
            ],
        )

    async def purge_finished_work(
        self, chain: ChainConfig, cutoff: datetime, batch: int = 50_000
    ) -> int:
        """Delete every version of terminal (done/dead/unavailable) work items whose
        latest write predates `cutoff`, keeping `work_items` small so `lease_work FINAL`
        never scans an unbounded table. Bounded and FINAL-free: select at most `batch`
        aged terminal work_ids, then delete all their versions. Returns the number of
        work_ids purged so the caller can decide whether another batch is due.

        This is finite-window deduplication, NOT permanent completion: after a work_id
        is purged, a later deterministic rediscovery (finality rescan, latest
        competition, import, enrichment fanout) re-creates a fresh pending item. That is
        safe because handler writes are idempotent; it only costs API calls, which the
        grace window bounds. Still-open items carry no terminal-status row, so they are
        never selected.
        """
        await self._ensure()
        async with self._final_gate():
            selected = await self.client.query(
                f"SELECT DISTINCT work_id FROM {self.quoted_database}.work_items "
                "WHERE environment = {environment:String} AND chain_id = {chain_id:UInt64} "
                "AND status IN ('done', 'dead', 'unavailable_from_public_api') "
                "AND observed_at < {cutoff:DateTime64(3)} "
                "LIMIT {batch:UInt32}",
                parameters={
                    "environment": chain.environment,
                    "chain_id": chain.chain_id,
                    "cutoff": cutoff,
                    "batch": batch,
                },
                settings={"max_memory_usage": FINAL_QUERY_MEMORY, "max_threads": 2},
            )
        work_ids = [row[0] for row in selected.result_rows]
        if not work_ids:
            return 0
        # work_id is a sha256 hex digest (see enqueue_work_many), so inlining the list
        # is injection-safe and avoids a huge server-side array parameter.
        id_list = "','".join(work_ids)
        await self.client.command(
            f"DELETE FROM {self.quoted_database}.work_items "
            "WHERE environment = {environment:String} AND chain_id = {chain_id:UInt64} "
            f"AND work_id IN ('{id_list}')",
            parameters={"environment": chain.environment, "chain_id": chain.chain_id},
            settings=PURGE_SETTINGS,
        )
        return len(work_ids)

    async def import_rows(
        self, manifest: Any, dataset: str, rows: list[dict[str, Any]], bundle_id: str
    ) -> ImportStats:
        from cow_indexer.sources.exports.adapters import normalize_export_rows

        stats, table_rows, conflicts = await normalize_export_rows(
            self, manifest, dataset, rows, bundle_id
        )
        now = utcnow()
        await self._insert(
            "raw_export_rows",
            [
                {
                    "bundle_id": bundle_id,
                    "environment": manifest.environment,
                    "chain_id": manifest.chain_id,
                    "dataset": dataset,
                    "row_hash": sha256_json(row),
                    "payload": canonical_json(row),
                    "imported_at": now,
                }
                for row in rows
            ],
        )
        for table, normalized_rows in table_rows.items():
            await self._insert(table, normalized_rows)
        await self._insert("import_conflicts", conflicts)
        return stats

    async def order_immutable_hash(
        self, environment: str, chain_id: int, order_uid: str
    ) -> str | None:
        await self._ensure()
        result = await self.client.query(
            f"SELECT immutable_hash FROM {self.quoted_database}.orders FINAL "
            "WHERE environment = {environment:String} AND chain_id = {chain_id:UInt64} "
            "AND order_uid = {uid:String} LIMIT 1",
            parameters={"environment": environment, "chain_id": chain_id, "uid": order_uid},
        )
        return result.result_rows[0][0] if result.result_rows else None

    async def import_file_done(self, bundle_id: str, dataset: str, path: str, sha256: str) -> bool:
        await self._ensure()
        result = await self.client.query(
            f"SELECT status FROM {self.quoted_database}.import_files FINAL "
            "WHERE bundle_id = {bundle_id:UUID} AND dataset = {dataset:String} "
            "AND path = {path:String} AND sha256 = {sha256:String} LIMIT 1",
            parameters={"bundle_id": bundle_id, "dataset": dataset, "path": path, "sha256": sha256},
        )
        return bool(result.result_rows and result.result_rows[0][0] == "complete")

    async def record_import_file(
        self,
        bundle_id: str,
        dataset: str,
        path: str,
        sha256: str,
        rows: int,
        status: str,
        error: str = "",
    ) -> None:
        await self._insert(
            "import_files",
            [
                {
                    "bundle_id": bundle_id,
                    "dataset": dataset,
                    "path": path,
                    "sha256": sha256,
                    "rows": rows,
                    "status": status,
                    "error": error,
                    "updated_at": utcnow(),
                }
            ],
        )

    async def record_import_run(
        self, manifest: Any, status: str, stats: ImportStats, error: str = ""
    ) -> None:
        await self._insert(
            "import_runs",
            [
                {
                    "bundle_id": manifest.bundle_id,
                    "environment": manifest.environment,
                    "chain_id": manifest.chain_id,
                    "source": manifest.source,
                    "snapshot_at": manifest.snapshot_at,
                    "status": status,
                    "accepted": stats.accepted,
                    "duplicates": stats.duplicates,
                    "rejected": stats.rejected,
                    "conflicts": stats.conflicts,
                    "error": error,
                    "updated_at": utcnow(),
                }
            ],
        )

    async def status(self) -> list[dict[str, Any]]:
        await self._ensure()
        result = await self.client.query(
            f"SELECT environment, chain_id, block_number, block_hash, updated_at "
            f"FROM {self.quoted_database}.indexing_checkpoints FINAL ORDER BY chain_id"
        )
        return [dict(zip(result.column_names, row, strict=True)) for row in result.result_rows]

    async def coverage(self, chain: ChainConfig) -> dict[str, Any]:
        await self._ensure()
        result = await self.client.query(
            f"SELECT source, count(), min(creation_date), max(creation_date) "
            f"FROM {self.quoted_database}.orders FINAL "
            "WHERE environment = {environment:String} AND chain_id = {chain_id:UInt64} GROUP BY source",
            parameters={"environment": chain.environment, "chain_id": chain.chain_id},
        )
        return {
            "chain": chain.key,
            "orders": [
                {"source": row[0], "rows": row[1], "earliest": row[2], "latest": row[3]}
                for row in result.result_rows
            ],
        }
