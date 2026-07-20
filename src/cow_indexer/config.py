from __future__ import annotations

import json
import os
import socket
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cow_indexer.utils import normalize_address


class ContractDeployment(BaseModel):
    name: str
    address: str
    from_block: int = Field(ge=0)
    abi: Path

    @field_validator("address")
    @classmethod
    def valid_address(cls, value: str) -> str:
        return normalize_address(value)


class Deployment(BaseModel):
    network: str
    chain_id: int = Field(gt=0)
    source: str
    contracts: list[ContractDeployment]

    @property
    def start_block(self) -> int:
        return min(contract.from_block for contract in self.contracts)


class ChainConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    key: str
    chain_id: int = Field(gt=0)
    environment: str
    api_base_url: str
    rpc_url_env: str
    deployment_file: Path
    finality_blocks: int = Field(default=20, ge=1)
    enabled: bool = True
    project_root: Path = Field(exclude=True)
    deployment: Deployment | None = Field(default=None, exclude=True)

    @property
    def rpc_url(self) -> str:
        value = os.getenv(self.rpc_url_env)
        if not value:
            raise RuntimeError(f"{self.rpc_url_env} is required for chain {self.key}")
        return value

    def load_deployment(self) -> Deployment:
        path = self.deployment_file
        if not path.is_absolute():
            path = self.project_root / path
        deployment = Deployment.model_validate_json(path.read_text())
        if deployment.chain_id != self.chain_id:
            raise ValueError(
                f"deployment {path} has chain_id {deployment.chain_id}, expected {self.chain_id}"
            )
        for contract in deployment.contracts:
            if not contract.abi.is_absolute():
                contract.abi = self.project_root / contract.abi
        self.deployment = deployment
        return deployment


class IndexerConfig(BaseModel):
    chains: list[ChainConfig]
    project_root: Path = Field(exclude=True)

    @model_validator(mode="after")
    def unique_chains(self) -> IndexerConfig:
        keys = [chain.key for chain in self.chains]
        ids = [chain.chain_id for chain in self.chains]
        if len(keys) != len(set(keys)):
            raise ValueError("chain keys must be unique")
        if len(ids) != len(set(ids)):
            raise ValueError("chain IDs must be unique")
        return self

    def select(self, selector: str) -> list[ChainConfig]:
        enabled = [chain for chain in self.chains if chain.enabled]
        if selector == "all":
            return enabled
        selected = [chain for chain in enabled if chain.key == selector]
        if not selected:
            choices = ", ".join(chain.key for chain in enabled)
            raise ValueError(f"unknown or disabled chain {selector!r}; choose one of: {choices}")
        return selected


class ClickHouseConfig(BaseModel):
    host: str = "localhost"
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "cow_indexer"
    secure: bool = False
    # HTTP connection-pool size. The default clickhouse-connect pool is 8, which one
    # shared async client saturates when many chains scan concurrently ("Connection
    # pool is full, discarding connection"). Scale it up for multi-chain runs.
    pool_size: int = 32

    @classmethod
    def from_env(cls) -> ClickHouseConfig:
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USERNAME", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            database=os.getenv("CLICKHOUSE_DATABASE", "cow_indexer"),
            secure=os.getenv("CLICKHOUSE_SECURE", "false").lower() in {"1", "true", "yes"},
            pool_size=int(os.getenv("CLICKHOUSE_POOL_SIZE", "32")),
        )


class RuntimeConfig(BaseModel):
    worker_id: str = Field(default_factory=lambda: f"{socket.gethostname()}-{os.getpid()}")
    metrics_host: str = "0.0.0.0"
    metrics_port: int = 9090
    api_interval_seconds: float = 0.1
    api_max_interval_seconds: float = 5.0
    enrich_concurrency: int = 6
    max_attempts: int = 6
    api_key: str | None = None

    @classmethod
    def from_env(cls) -> RuntimeConfig:
        api_key = os.getenv("COW_API_KEY") or None
        # With an X-API-Key the CoW allowance is ~30 RPS; run ~10 RPS to stay under it.
        # Without a key the public edge blocks well below that, so default much slower.
        default_interval = "0.1" if api_key else "0.6"
        return cls(
            worker_id=os.getenv("COW_WORKER_ID") or f"{socket.gethostname()}-{os.getpid()}",
            metrics_host=os.getenv("COW_METRICS_HOST", "0.0.0.0"),
            metrics_port=int(os.getenv("COW_METRICS_PORT", "9090")),
            api_interval_seconds=float(os.getenv("COW_API_INTERVAL_SECONDS", default_interval)),
            api_max_interval_seconds=float(os.getenv("COW_API_MAX_INTERVAL_SECONDS", "5.0")),
            enrich_concurrency=int(os.getenv("COW_ENRICH_CONCURRENCY", "6")),
            max_attempts=int(os.getenv("COW_MAX_ATTEMPTS", "6")),
            api_key=api_key,
        )


def load_config(path: Path) -> IndexerConfig:
    resolved = path.resolve()
    project_root = resolved.parent.parent
    payload = yaml.safe_load(resolved.read_text())
    chains = [ChainConfig(project_root=project_root, **item) for item in payload["chains"]]
    config = IndexerConfig(chains=chains, project_root=project_root)
    for chain in config.chains:
        chain.load_deployment()
    return config


def load_abi(path: Path) -> list[dict]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, list):
        raise ValueError(f"ABI must be an array: {path}")
    return payload
