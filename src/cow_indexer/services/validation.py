from __future__ import annotations

from typing import Any

from cow_indexer.config import ChainConfig
from cow_indexer.models import ValidationResult
from cow_indexer.storage.clickhouse import ClickHouseStore


class ValidationService:
    def __init__(self, store: ClickHouseStore) -> None:
        self.store = store

    async def validate_chain(self, chain: ChainConfig) -> list[ValidationResult]:
        checks = [
            (
                "trades_have_orders",
                f"SELECT count() FROM {self.store.quoted_database}.trades_canonical AS t "
                f"LEFT JOIN {self.store.quoted_database}.orders AS o FINAL "
                "ON t.environment=o.environment AND t.chain_id=o.chain_id AND t.order_uid=o.order_uid "
                "WHERE t.environment={environment:String} AND t.chain_id={chain_id:UInt64} AND o.order_uid=''",
            ),
            (
                "settlements_have_competitions",
                f"SELECT count() FROM {self.store.quoted_database}.settlements_canonical AS s "
                f"LEFT JOIN {self.store.quoted_database}.solver_competitions AS c FINAL "
                "ON s.environment=c.environment AND s.chain_id=c.chain_id AND s.tx_hash=c.tx_hash "
                "WHERE s.environment={environment:String} AND s.chain_id={chain_id:UInt64} AND c.tx_hash=''",
            ),
            (
                "no_import_conflicts",
                f"SELECT count() FROM {self.store.quoted_database}.import_conflicts "
                "WHERE environment={environment:String} AND chain_id={chain_id:UInt64}",
            ),
        ]
        results: list[ValidationResult] = []
        await self.store.connect()
        for name, query in checks:
            response = await self.store.client.query(
                query,
                parameters={"environment": chain.environment, "chain_id": chain.chain_id},
            )
            count = int(response.result_rows[0][0])
            results.append(
                ValidationResult(
                    name=name,
                    passed=count == 0,
                    count=count,
                    detail="unmatched records" if count else "",
                )
            )
        return results

    async def coverage(self, chains: list[ChainConfig]) -> list[dict[str, Any]]:
        return [await self.store.coverage(chain) for chain in chains]
