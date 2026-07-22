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

Set a CoW API key so enrichment is not throttled by CoW's edge. Without one the public edge starts returning `403 Request blocked` under load; the key is sent as `X-API-Key` and raises the allowance to roughly 30 RPS. Enrichment pacing and retry behavior are tunable and are a single global budget shared across all chains:

```dotenv
COW_API_KEY=...
COW_API_INTERVAL_SECONDS=0.1   # global pacing; default 0.1 (~10 RPS) with a key, 0.6 without
COW_API_MAX_INTERVAL_SECONDS=5 # adaptive backoff ceiling on 429/403
COW_ENRICH_CONCURRENCY=6       # bounded concurrent enrichment work items
COW_MAX_ATTEMPTS=6             # attempts before a work item is dead-lettered
```

Before enabling many chains, validate every configured RPC and API endpoint with `preflight` (see below); it is read-only and catches pruned/non-archive nodes before they stall a scan.

Docker Compose does not run a local ClickHouse server. It runs migrations once against ClickHouse Cloud and starts the indexer only after they succeed:

```bash
# COW_CHAIN defaults to gnosis; set it to all after the first-chain test.
docker compose up -d --build
```

The ClickHouse user needs table, view, insert, and select privileges within `CLICKHOUSE_DATABASE`; it does not need account-wide `CREATE DATABASE` permission.

## Commands

Validate every selected chain's RPC (head plus historical `eth_getLogs` at the deployment block) and CoW API reachability, without touching ClickHouse. It prints a per-chain report and exits non-zero if any chain's RPC path fails — run it before enabling new chains:

```bash
uv run cow-indexer preflight --chain all
```

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

## Running modes

Each mode is a subcommand of the same binary; they share the ClickHouse schema and the durable checkpoints, so they interleave safely.

| Mode | Command | Purpose |
| --- | --- | --- |
| Preflight | `preflight --chain <all\|key>` | Read-only readiness check: RPC head, historical `eth_getLogs` at the deployment block, and CoW API reachability per chain. Run before enabling chains. |
| Migrate | `migrate` | Apply `migrations/*.sql` (idempotent `CREATE ... IF NOT EXISTS` / `CREATE OR REPLACE VIEW`). Run before any ingestion. |
| Backfill | `backfill --chain <sel> [--from-block N] [--to-block M]` | One-shot forward scan from the checkpoint (or a bound) up to the safe head, then exit. |
| Repair | `repair --chain <key> --from-block N --to-block M` | Rescan a bounded range and reconcile reorgs/gaps **without moving the forward checkpoint backward**. |
| Continuous | `continuous --chain <all\|key>` | Long-running service: forward scan + finality-window rescan + competitions + active orders + enrichment + token prices, per chain. Exposes `:9090`. |
| Inspect / status | `status`, `coverage --chain <sel>`, `validate --chain <sel>` | Report per-chain checkpoints, historical coverage, and reconciliation checks. |
| Export import | `inspect-export`, `import-export`, `validate-export` | Optional authoritative off-chain bundle import (see below). |

Typical lifecycle: `preflight` → `migrate` → `continuous` (which backfills from each chain's pinned deployment block up to the tip and then tracks the head). `backfill` and `repair` are for bounded/one-shot work. In `continuous` mode every chain runs independently — a failing chain retries in isolation and does not stop the others. A fresh chain with no checkpoint starts at the earliest pinned `from_block` in its `deployments/*.json`; pin those blocks (not `0`) to avoid scanning from genesis.

## Historical scan behavior

The scanner starts with 5,000-block `eth_getLogs` requests. Successful ranges grow to 50,000 blocks; provider range/response-limit errors halve the request down to a 50-block minimum. Range/limit errors are recognized by JSON-RPC code and message even when the provider returns them with a non-200 HTTP status (some return `503` for "too many logs"), so they trigger adaptive halving (logged as `rpc_range_reduced`) rather than aborting the scan. Each RPC call has a hard request timeout, so a provider that accepts a connection but never responds raises a retryable error instead of parking the scan. Raw logs and block headers are batched per range; decoded events and enrichment work items are flushed in one insert per table; raw logs, block headers, decoded events, and the checkpoint are committed in that order.

Continuous ingestion stores canonical block hashes throughout the finality window. A replacement block hash makes logs from the abandoned block non-canonical at query/reconciliation time without destructive mutations. The reorg-aware `*_canonical` views are additionally bounded to the committed checkpoint, so they never expose partially-processed or not-yet-committed rows. Repair scans never move the durable forward checkpoint backward.

## API enrichment

Event discovery creates deterministic work identities for:

- order UIDs
- owners
- settlement transaction hashes
- app-data hashes
- token addresses

CoW API order lookups are split into the documented maximum of 128 UIDs; leased `order_uid` work items share one batched fetch. Account orders and v2 trades paginate with 1,000-row pages until a short page is received. `curl_cffi` browser TLS impersonation is used because CoW's edge distinguishes (and blocks) ordinary Python TLS clients — this is required even with an API key, which is sent as `X-API-Key`. A single rate limiter is shared across all chains (they hit the same host and key); it uses bounded exponential backoff on retryable statuses and additionally backs the global rate off toward `COW_API_MAX_INTERVAL_SECONDS` on `429`/`403`, recovering as requests succeed.

The ClickHouse work queue is append-only and restart-safe. Terminal revisions (`done`/`dead`/`unavailable_from_public_api`) dominate the ReplacingMergeTree merge, so a replayed chain range cannot revive completed work *while those rows exist*. To keep the queue bounded (an unbounded `work_items` makes `lease_work`'s `FINAL` an OOM risk), a scheduled maintenance task deletes every version of terminal work items older than a grace window (`COW_PURGE_GRACE_HOURS`, default 24h). This is finite-window deduplication, not permanent completion: after a terminal work item is purged, a later deterministic rediscovery (finality rescan, latest competition, import, enrichment fanout) re-creates a fresh pending item and re-enriches it — safe because handler writes are idempotent, at the cost of some API calls. Purging is serialized process-wide and can be disabled (`COW_PURGE_ENABLED=false`) for a one-time bulk cleanup via `purge-work`. A competition the public API no longer serves is marked terminal `unavailable_from_public_api` (recorded, not retried, not dead-lettered). Run only one enrichment worker replica for a given `(environment, chain_id)`; ClickHouse does not provide a transactional competing-consumer lease.

## Observability

The continuous service serves three endpoints on `COW_METRICS_PORT` (default 9090):

- `/health`: process liveness
- `/ready`: ClickHouse connectivity
- `/metrics`: Prometheus metrics

Exported metrics (labeled by chain):

- `cow_chain_lag_blocks{chain}` — safe head minus the committed scan position
- `cow_rows_written_total{chain,table}` — rows written per table
- `cow_rpc_requests_total{chain,method,status}` and `cow_api_requests_total{chain,route,status}` — request counts (the API metric is labeled by templated route, not the per-UID path, to bound series cardinality)
- `cow_request_seconds{source,chain}` — RPC/API latency histogram

There is deliberately no exact pending-work gauge: an exact latest-state count on the append-only `work_items` requires a full-table `FINAL`, which is the OOM this design removes. Track backlog via `cow_rows_written_total` progress and the `purge_sweep` log line instead.

Logs are structured JSON on stdout. During a backfill, watch `cow_rows_written_total` increasing and the `range_indexed` log line advancing; `cow_chain_lag_blocks` is legitimately large until a chain catches up, so alert on "no rows written" and pod health rather than on lag thresholds during the initial backfill.

## Recovering from failures

Checkpoints are durable in ClickHouse per `(environment, chain_id, source='rpc')`, and all writes are idempotent (ReplacingMergeTree), so recovery from almost any interruption is to restart `continuous` — each chain resumes from its last committed checkpoint with no data loss.

- **Process crash / restart / redeploy.** Restart `continuous`; it resumes per chain from the checkpoint. No manual step.
- **A chain stops advancing (silent stall).** RPC calls are timeout-guarded, so a hung provider raises and the per-chain loop retries instead of parking. Confirm progress via `status` (checkpoint advancing) and the `cow_rows_written_total` / `range_indexed` signals. If one provider is unhealthy, swap its `COW_RPC_URL_*` and restart.
- **RPC range/response limit** (`-32005`, "too many logs", "block range" …). Handled automatically — the scan halves the window down to 50 blocks and continues, logging `rpc_range_reduced` (not an error). No action needed.
- **Pruned / non-archive RPC** (`-32701 History has been pruned`, "Archive requests require a token"). The node cannot serve historical logs at the deployment block. Detect with `preflight`; point `COW_RPC_URL_*` at a history-serving node (archive *state* is not required, but historical `eth_getLogs` back to the deployment block is) and restart.
- **CoW API `403 Request blocked` / `429`.** The edge is rate-limiting. Set `COW_API_KEY`; the client self-throttles (global adaptive backoff) and retries. If it persists, raise `COW_API_INTERVAL_SECONDS` (e.g. `0.2`) or lower `COW_ENRICH_CONCURRENCY`. Transient blocks clear on their own and work items are retried, not lost.
- **Enrichment `Code 241 / MEMORY_LIMIT_EXCEEDED` on `work_items`.** The enrichment queue grew large enough that the lease `FINAL` exceeds its per-query cap. It fails **in isolation** and the loop backs off — RPC ingestion keeps running (the cap + concurrency gate prevent the old instance-wide cascade). It self-heals as the scheduled purge trims finished items. To unblock immediately: `OPTIMIZE TABLE <db>.work_items FINAL;` (collapses the many parts the FINAL must merge — the usual cause) and/or raise `CLICKHOUSE_FINAL_MEMORY_MB`. Lower `COW_ENRICH_BATCH` if a single lease is too heavy. A genuinely huge distinct backlog is expected during a full-history multi-chain backfill and drains at the API rate.
- **Dead letters.** Items that exhaust `COW_MAX_ATTEMPTS` land in `dead_letters`; inspect them for the cause. To retry, re-enqueue the originating range with `repair` (idempotent). Historical competitions the API no longer serves are terminal `unavailable_from_public_api`, not failures.
- **Reorg.** Handled automatically — canonical block hashes are re-observed within the finality window and the `*_canonical` views drop orphaned-hash rows. Force re-observation of an older range with `repair` (it re-fetches every block header in the range).
- **A gap or a newly added contract.** `repair --chain X --from-block A --to-block B` rescans a bounded range without moving the forward checkpoint backward. Example: after adding a contract with an earlier `from_block`, repair `[new_from_block, current_checkpoint]` — a lowered start block only auto-applies to a chain that has no checkpoint yet.
- **Start over / wipe.** `TRUNCATE` the data tables (keep `schema_migrations`), then run `continuous`; with no checkpoint each chain restarts at its pinned deployment block. `TRUNCATE` needs only table privileges, not `DROP DATABASE`.
- **Bringing up many chains.** Run `preflight --chain all` first; pin each chain's `from_block` in `deployments/*.json` (a `0` start scans from genesis); ensure every `COW_RPC_URL_*` is set. The CoW API budget is global, so more chains means slower per-chain enrichment.

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
- Run `preflight` before enabling new chains, and pin each chain's `from_block` so it does not scan from genesis.
- Set `COW_API_KEY`; keep RPC URLs, the API key, and PostgreSQL DSNs out of logs and committed configuration.
- Use a dedicated ClickHouse database and credentials in production.
- Back up ClickHouse before schema upgrades.
- Alert on pod health and on "no rows written in an hour" (a stall), plus RPC/API error ratios, dead letters, and import conflicts. Treat `cow_chain_lag_blocks` thresholds as steady-state signals to enable only after the initial backfill catches up — it is legitimately large during it.
- “Complete off-chain history” may only be claimed when a validated export manifest declares the required historical boundary.
