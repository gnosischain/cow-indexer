from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from cow_indexer.utils import normalize_address, normalize_hash


class RpcLog(BaseModel):
    address: str
    topics: list[str]
    data: str
    block_number: int
    block_hash: str
    transaction_hash: str
    transaction_index: int
    log_index: int
    removed: bool = False

    @field_validator("address")
    @classmethod
    def valid_address(cls, value: str) -> str:
        return normalize_address(value)

    @field_validator("block_hash", "transaction_hash")
    @classmethod
    def valid_hash(cls, value: str) -> str:
        return normalize_hash(value)

    @classmethod
    def from_rpc(cls, payload: dict[str, Any]) -> RpcLog:
        return cls(
            address=payload["address"],
            topics=payload["topics"],
            data=payload["data"],
            block_number=int(payload["blockNumber"], 16),
            block_hash=payload["blockHash"],
            transaction_hash=payload["transactionHash"],
            transaction_index=int(payload["transactionIndex"], 16),
            log_index=int(payload["logIndex"], 16),
            removed=payload.get("removed", False),
        )


class BlockHeader(BaseModel):
    number: int
    block_hash: str
    parent_hash: str
    timestamp: datetime

    @classmethod
    def from_rpc(cls, payload: dict[str, Any]) -> BlockHeader:
        return cls(
            number=int(payload["number"], 16),
            block_hash=payload["hash"],
            parent_hash=payload["parentHash"],
            timestamp=datetime.fromtimestamp(int(payload["timestamp"], 16), UTC),
        )


class DecodedEvent(BaseModel):
    environment: str
    chain_id: int
    contract_name: str
    contract_address: str
    event_name: str
    args: dict[str, Any]
    block_number: int
    block_hash: str
    transaction_hash: str
    transaction_index: int
    log_index: int
    removed: bool = False
    block_timestamp: datetime | None = None


class WorkItem(BaseModel):
    work_id: str
    environment: str
    chain_id: int
    kind: Literal["order_uid", "owner", "tx_hash", "app_data", "token"]
    key: str
    payload: dict[str, Any] = Field(default_factory=dict)
    attempts: int = 0


class ValidationResult(BaseModel):
    name: str
    passed: bool
    count: int = 0
    detail: str = ""


class ImportStats(BaseModel):
    accepted: int = 0
    duplicates: int = 0
    rejected: int = 0
    conflicts: int = 0

    def add(self, other: ImportStats) -> None:
        self.accepted += other.accepted
        self.duplicates += other.duplicates
        self.rejected += other.rejected
        self.conflicts += other.conflicts
