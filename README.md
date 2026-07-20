# CoW Protocol Indexer

![Cow](img/header-banner.png)

A standalone, multi-chain indexer for CoW Protocol. It reads canonical contract events directly from EVM JSON-RPC, enriches discovered orders and settlements through the public CoW API, and optionally imports authoritative off-chain history bundles. It does not depend on DBT or any existing ingestion pipeline.

## Coverage model

The mandatory RPC and API path provides complete indexed history for configured on-chain contracts and the maximum order-book history publicly discoverable from order UIDs, owners, transactions, and competitions. It cannot discover an off-chain order that never executes when neither its UID nor its owner is otherwise known. The optional Parquet export interface closes that gap when an authorized CoW order-book database export is available.

## Architecture

```text
EVM RPC ── logs/blocks ──┐
                         ├─ decoder ─ canonical ClickHouse tables
CoW API ─ orders/trades ─┤                 │
                         │                 ├─ checkpoints and reorg reconciliation
Export bundle ─ parquet ─┘                 └─ durable API enrichment work
```

Every identity includes `environment` and `chain_id`. Addresses and hashes are stored lowercase; raw RPC, API, and export payloads are retained. Token amounts use ClickHouse `UInt256`.

## Repository layout

```text
config/             chain and endpoint configuration
deployments/        official contract addresses and safe scan starts
abis/               event-only contract ABIs
migrations/         indexer-owned ClickHouse schema
src/cow_indexer/    RPC/API clients, decoding, storage, and services
export-schema/      stable off-chain export contract
scripts/            read-only PostgreSQL bundle exporter
tests/              unit, integration, and opt-in live tests
```

## Quick start

Requirements: Python 3.12+, `uv`, a ClickHouse Cloud service with the configured database already created, and one historical-log-capable EVM JSON-RPC endpoint per enabled chain.

```bash
cp .env.example .env
# Set the ClickHouse Cloud hostname, username and password in .env.
# The standard secure native-HTTP endpoint uses port 8443.
uv sync --all-extras
set -a
source .env
set +a
uv run cow-indexer migrate
```

Configure RPC variables in `.env`. Archive state and traces are not required, but the provider must serve historical `eth_getLogs` calls back to each configured deployment block.

```dotenv
COW_RPC_URL_MAINNET=https://...
COW_RPC_URL_GNOSIS=https://...
COW_RPC_URL_ARBITRUM=https://...
```

Disable a chain in `config/chains.yaml` when no RPC is available. The API base URLs, finality windows, and deployment files are configured independently per chain.

Docker Compose does not run a local ClickHouse server. It runs migrations once against ClickHouse Cloud and starts the indexer only after they succeed:

```bash
# COW_CHAIN defaults to gnosis; set it to all after the first-chain test.
docker compose up -d --build
```

The ClickHouse user needs table, view, insert, and select privileges within `CLICKHOUSE_DATABASE`; it does not need account-wide `CREATE DATABASE` permission.

## Commands

Apply migrations and backfill every enabled network:

```bash
uv run cow-indexer migrate
uv run cow-indexer backfill --chain all
```

Bound a historical or repair scan:

```bash
uv run cow-indexer backfill --chain gnosis --from-block 30000000
uv run cow-indexer repair --chain mainnet --from-block 20000000 --to-block 20100000
```

Run continuous indexing:

```bash
uv run cow-indexer continuous --chain all
```

The continuous service independently runs, retries, and reports failures for each chain. It polls finalized logs, rescans the finality window, refreshes solver competitions and active orders, processes enrichment work, and snapshots token native prices. Health endpoints are exposed on port 9090:

- `/health`: process liveness
- `/ready`: ClickHouse connectivity
- `/metrics`: Prometheus metrics

Inspect progress and reconcile datasets:

```bash
uv run cow-indexer status
uv run cow-indexer coverage --chain all
uv run cow-indexer validate --chain all
```

## Historical scan behavior

The scanner starts with 5,000-block `eth_getLogs` requests. Successful ranges grow to 50,000 blocks; provider range/response-limit errors halve the request down to a 50-block minimum. Raw logs, block headers, decoded events, and the checkpoint are committed in that order.

Continuous ingestion stores canonical block hashes throughout the finality window. A replacement block hash makes logs from the abandoned block non-canonical at query/reconciliation time without destructive mutations. Repair scans never move the durable forward checkpoint backward.

## API enrichment

Event discovery creates deterministic work identities for:

- order UIDs
- owners
- settlement transaction hashes
- app-data hashes
- token addresses

CoW API order lookups are split into the documented maximum of 128 UIDs. Account orders and v2 trades paginate with 1,000-row pages until a short page is received. Retryable HTTP statuses use bounded exponential backoff. `curl_cffi` browser TLS impersonation is used because CoW's edge can distinguish ordinary Python TLS clients.

The ClickHouse work queue is append-only and restart-safe. Terminal revisions prevent a replayed chain range from reviving completed work. Run only one enrichment worker replica for a given `(environment, chain_id)`; ClickHouse does not provide a transactional competing-consumer lease.

## Authoritative off-chain export

The importer intentionally consumes a stable, versioned Parquet bundle instead of connecting the live indexer to CoW's changing PostgreSQL schema.

```text
cow-export-mainnet/
├── manifest.json
├── orders/part-00000.parquet
├── order_events/part-00000.parquet
├── auctions/part-00000.parquet
└── trades/part-00000.parquet
```

The manifest declares the bundle UUID, chain, environment, source schema version, snapshot time, historical coverage, file row counts, and SHA-256 checksums. Supported datasets are `orders`, `order_events`, `auctions`, `auction_orders`, `solver_competitions`, `trades`, `app_data`, and `quotes`.

Inspect and import a bundle:

```bash
uv run cow-indexer inspect-export --bundle /data/cow-export-mainnet
uv run cow-indexer import-export \
  --bundle /data/cow-export-mainnet \
  --verify-checksums \
  --enqueue-enrichment
uv run cow-indexer validate-export --import-id 019abcde-0000-7000-8000-000000000000
```

Each completed file is checkpointed by `(bundle_id, dataset, path, sha256)`. Interrupted imports resume at the next incomplete file. Raw source rows are retained. Existing orders with matching immutable hashes are duplicates; mismatches are quarantined in `import_conflicts` and never overwrite the canonical order.

### Creating a bundle

Database access is optional and must be read-only. Install the exporter dependency:

```bash
uv sync --extra postgres-export
```

Create source-specific SQL files that alias fields to the canonical names in `export-schema/v1`. Then run:

```bash
export COW_EXPORT_PG_DSN='postgresql://readonly:...@host/database'
uv run python scripts/export_orderbook.py \
  --network mainnet \
  --chain-id 1 \
  --source-schema-version services-git-commit \
  --output /data/cow-export-mainnet \
  --dataset orders=/secure/queries/orders.sql \
  --dataset order_events=/secure/queries/order_events.sql \
  --dataset auctions=/secure/queries/auctions.sql \
  --dataset trades=/secure/queries/trades.sql
```

The exporter opens one repeatable-read, read-only PostgreSQL transaction so every dataset belongs to the same logical snapshot. Source queries are deliberately not embedded: they must be reviewed against the exact upstream database revision and may expose only approved protocol data.

## Deployment metadata

Settlement addresses and known start blocks come from `@cowprotocol/contracts`. EthFlow addresses come from `cowprotocol/ethflowcontract`, and ComposableCoW addresses from `cowprotocol/composable-cow`. A `from_block` of zero means the official address is verified but a deployment block was not pinned; it is conservative and cannot skip events, but operators should replace it with a verified receipt block for faster initial scans.

## Testing

```bash
uv run ruff check .
uv run pytest
```

Integration tests require ClickHouse and are marked `integration`. Live tests require the corresponding `COW_RPC_URL_*` environment variable and are opt-in.

## Operational notes

- Run migrations before any ingestion command.
- Use a dedicated ClickHouse database and credentials in production.
- Keep RPC URLs and PostgreSQL DSNs out of logs and committed configuration.
- Back up ClickHouse before schema upgrades.
- Alert on safe-head lag, request failures, dead letters, and import conflicts.
- “Complete off-chain history” may only be claimed when a validated export manifest declares the required historical boundary.
