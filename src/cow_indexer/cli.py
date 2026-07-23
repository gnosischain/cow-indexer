from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any

import typer

from cow_indexer import __version__
from cow_indexer.config import ClickHouseConfig, RuntimeConfig, load_config
from cow_indexer.observability import configure_logging
from cow_indexer.services.backfill_orderbook import UID_BATCH_SIZE, BackfillOrderbookService
from cow_indexer.services.continuous import run_continuous
from cow_indexer.services.export_import import ExportImportService, inspect_bundle
from cow_indexer.services.historical import HistoricalIndexer
from cow_indexer.services.preflight import run_preflight
from cow_indexer.services.repair import repair_range
from cow_indexer.services.validation import ValidationService
from cow_indexer.sources.exports.manifest import load_manifest
from cow_indexer.sources.rpc import RpcClient
from cow_indexer.storage.clickhouse import ClickHouseStore
from cow_indexer.utils import utcnow

app = typer.Typer(
    no_args_is_help=True,
    invoke_without_command=True,
    pretty_exceptions_enable=False,
)
DEFAULT_CONFIG = Path("config/chains.yaml")
ConfigOption = Annotated[Path, typer.Option("--config", exists=True, dir_okay=False)]


def _print(value: Any) -> None:
    typer.echo(json.dumps(value, indent=2, default=str))


async def _store(config_path: Path) -> ClickHouseStore:
    config = load_config(config_path)
    return await ClickHouseStore(ClickHouseConfig.from_env(), config.project_root).connect()


@app.callback()
def callback(
    version: Annotated[bool, typer.Option("--version", is_eager=True)] = False,
) -> None:
    """Index CoW Protocol directly from EVM RPC and the public order-book API."""
    configure_logging()
    if version:
        typer.echo(__version__)
        raise typer.Exit()


@app.command()
def migrate(config: ConfigOption = DEFAULT_CONFIG) -> None:
    """Apply indexer-owned ClickHouse migrations."""

    async def run() -> None:
        store = await _store(config)
        try:
            applied = await store.migrate()
            _print({"applied": applied})
        finally:
            await store.close()

    asyncio.run(run())


@app.command()
def backfill(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    from_block: Annotated[int | None, typer.Option("--from-block", min=0)] = None,
    to_block: Annotated[int | None, typer.Option("--to-block", min=0)] = None,
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Backfill finalized on-chain events and enqueue API enrichment."""

    async def run() -> None:
        indexer_config = load_config(config)
        store = await ClickHouseStore(
            ClickHouseConfig.from_env(), indexer_config.project_root
        ).connect()
        output: dict[str, int] = {}
        try:
            for selected in indexer_config.select(chain):
                rpc = RpcClient(selected.rpc_url, selected.key)
                try:
                    output[selected.key] = await HistoricalIndexer(selected, rpc, store).scan(
                        from_block, to_block
                    )
                finally:
                    await rpc.close()
            _print(output)
        finally:
            await store.close()

    asyncio.run(run())


@app.command()
def continuous(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Continuously ingest every selected chain."""

    async def run() -> None:
        indexer_config = load_config(config)
        store = await ClickHouseStore(
            ClickHouseConfig.from_env(), indexer_config.project_root
        ).connect()
        try:
            await run_continuous(indexer_config.select(chain), store, RuntimeConfig.from_env())
        finally:
            await store.close()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


@app.command("purge-work")
def purge_work(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    grace_hours: Annotated[float, typer.Option("--grace-hours", min=0)] = 24.0,
    batch: Annotated[int, typer.Option("--batch", min=1)] = 50000,
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Delete aged terminal work_items in bounded, serial batches (one-time backlog
    cleanup). Run with the scheduled purge disabled (COW_PURGE_ENABLED=false) and the
    continuous process paused so the two do not race on the shared table."""

    async def run() -> None:
        indexer_config = load_config(config)
        store = await ClickHouseStore(
            ClickHouseConfig.from_env(), indexer_config.project_root
        ).connect()
        cutoff = utcnow() - timedelta(hours=grace_hours)
        output: dict[str, int] = {}
        try:
            for selected in indexer_config.select(chain):
                total = 0
                while True:
                    purged = await store.purge_finished_work(selected, cutoff, batch)
                    total += purged
                    _print({"chain": selected.key, "purged_batch": purged, "purged_total": total})
                    # A short batch means no aged terminal work_ids remain for this chain.
                    if purged < batch:
                        break
                output[selected.key] = total
            _print({"purged": output})
        finally:
            await store.close()

    asyncio.run(run())


@app.command()
def repair(
    chain: Annotated[str, typer.Option("--chain")],
    from_block: Annotated[int, typer.Option("--from-block", min=0)],
    to_block: Annotated[int, typer.Option("--to-block", min=0)],
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Rescan and reconcile a bounded block range without moving checkpoints backward."""
    if to_block < from_block:
        raise typer.BadParameter("--to-block must be >= --from-block")

    async def run() -> None:
        indexer_config = load_config(config)
        selected = indexer_config.select(chain)
        if len(selected) != 1:
            raise typer.BadParameter("repair requires exactly one chain")
        current = selected[0]
        store = await ClickHouseStore(
            ClickHouseConfig.from_env(), indexer_config.project_root
        ).connect()
        rpc = RpcClient(current.rpc_url, current.key)
        try:
            count = await repair_range(HistoricalIndexer(current, rpc, store), from_block, to_block)
            _print({"chain": current.key, "logs": count})
        finally:
            await rpc.close()
            await store.close()

    asyncio.run(run())


@app.command("inspect-export")
def inspect_export(
    bundle: Annotated[Path, typer.Option("--bundle", exists=True, file_okay=False)],
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Validate an export manifest, checksums, and Parquet row counts."""
    root = load_config(config).project_root
    manifest, inspections = inspect_bundle(bundle, root / "export-schema" / "manifest.schema.json")
    _print(
        {
            "manifest": manifest.model_dump(mode="json"),
            "files": [
                {
                    "dataset": item.dataset,
                    "path": item.path,
                    "expected_rows": item.expected_rows,
                    "actual_rows": item.actual_rows,
                    "checksum_ok": item.checksum_ok,
                    "valid": item.valid,
                }
                for item in inspections
            ],
            "valid": all(item.valid for item in inspections),
        }
    )


@app.command("import-export")
def import_export(
    bundle: Annotated[Path, typer.Option("--bundle", exists=True, file_okay=False)],
    verify_checksums: Annotated[
        bool, typer.Option("--verify-checksums/--no-verify-checksums")
    ] = True,
    enqueue_enrichment: Annotated[bool, typer.Option("--enqueue-enrichment")] = False,
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Idempotently import a canonical off-chain history bundle."""

    async def run() -> None:
        indexer_config = load_config(config)
        manifest = load_manifest(
            bundle, indexer_config.project_root / "export-schema" / "manifest.schema.json"
        )
        matches = [
            chain
            for chain in indexer_config.chains
            if chain.chain_id == manifest.chain_id and chain.environment == manifest.environment
        ]
        if len(matches) != 1:
            raise ValueError("bundle chain/environment is not uniquely configured")
        store = await ClickHouseStore(
            ClickHouseConfig.from_env(), indexer_config.project_root
        ).connect()
        try:
            _, stats = await ExportImportService(store, matches[0]).run(
                bundle,
                schema_path=indexer_config.project_root / "export-schema" / "manifest.schema.json",
                verify_checksums=verify_checksums,
                enqueue_enrichment=enqueue_enrichment,
            )
            _print({"bundle_id": str(manifest.bundle_id), **stats.model_dump()})
        finally:
            await store.close()

    asyncio.run(run())


@app.command("validate-export")
def validate_export(
    import_id: Annotated[str, typer.Option("--import-id")],
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Show final state and conflicts for an imported bundle."""

    async def run() -> None:
        store = await _store(config)
        try:
            result = await store.client.query(
                f"SELECT status, accepted, duplicates, rejected, conflicts, error "
                f"FROM {store.quoted_database}.import_runs FINAL WHERE bundle_id={{id:UUID}} LIMIT 1",
                parameters={"id": import_id},
            )
            if not result.result_rows:
                raise ValueError(f"unknown import ID: {import_id}")
            _print(dict(zip(result.column_names, result.result_rows[0], strict=True)))
        finally:
            await store.close()

    asyncio.run(run())


@app.command()
def validate(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Reconcile orders, trades, settlements, competitions, and imports."""

    async def run() -> bool:
        indexer_config = load_config(config)
        store = await ClickHouseStore(
            ClickHouseConfig.from_env(), indexer_config.project_root
        ).connect()
        try:
            service = ValidationService(store)
            output = {}
            passed = True
            for selected in indexer_config.select(chain):
                results = await service.validate_chain(selected)
                output[selected.key] = [result.model_dump() for result in results]
                passed = passed and all(result.passed for result in results)
            _print(output)
            return passed
        finally:
            await store.close()

    if not asyncio.run(run()):
        raise typer.Exit(1)


@app.command()
def coverage(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Report API/RPC/export historical coverage by chain."""

    async def run() -> None:
        indexer_config = load_config(config)
        store = await ClickHouseStore(
            ClickHouseConfig.from_env(), indexer_config.project_root
        ).connect()
        try:
            _print(await ValidationService(store).coverage(indexer_config.select(chain)))
        finally:
            await store.close()

    asyncio.run(run())


@app.command()
def preflight(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Check each chain's RPC (head + historical getLogs) and CoW API without touching
    ClickHouse. Exits non-zero if any selected chain's RPC path fails."""

    async def run() -> bool:
        indexer_config = load_config(config)
        results = await run_preflight(
            indexer_config.select(chain), RuntimeConfig.from_env().api_key
        )
        _print(results)
        return all(result["ok"] for result in results)

    if not asyncio.run(run()):
        raise typer.Exit(1)


@app.command()
def status(config: ConfigOption = DEFAULT_CONFIG) -> None:
    """Show current per-chain RPC checkpoints."""

    async def run() -> None:
        store = await _store(config)
        try:
            _print(await store.status())
        finally:
            await store.close()

    asyncio.run(run())


backfill_orderbook_app = typer.Typer(
    no_args_is_help=True,
    help="Historical orderbook backfill: replay off-chain order history via the "
    "public CoW API (probe -> seed-orders -> drain -> seed-owners -> drain).",
)
app.add_typer(backfill_orderbook_app, name="backfill-orderbook")


async def _backfill_service(config_path: Path) -> tuple[Any, ClickHouseStore, BackfillOrderbookService]:
    indexer_config = load_config(config_path)
    store = await ClickHouseStore(
        ClickHouseConfig.from_env(), indexer_config.project_root
    ).connect()
    return indexer_config, store, BackfillOrderbookService(store, RuntimeConfig.from_env())


@backfill_orderbook_app.command("probe")
def backfill_orderbook_probe(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    per_chain: Annotated[int, typer.Option("--per-chain", min=1, max=UID_BATCH_SIZE)] = 5,
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Feasibility gate: fetch each chain's oldest-traded uids via by_uids and report
    the hit rate (repeatable; misses mark the epoch the public API no longer serves)."""

    async def run() -> None:
        indexer_config, store, service = await _backfill_service(config)
        try:
            _print(await service.probe(indexer_config.select(chain), per_chain))
        finally:
            await store.close()

    asyncio.run(run())


@backfill_orderbook_app.command("seed-orders")
def backfill_orderbook_seed_orders(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    limit: Annotated[int | None, typer.Option("--limit", min=1)] = None,
    batch_size: Annotated[
        int, typer.Option("--batch-size", min=1, max=UID_BATCH_SIZE)
    ] = UID_BATCH_SIZE,
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Enqueue order_uids_batch items for traded uids missing from `orders`
    (newest embedded validTo first; anti-joined SQL-side; idempotent re-seed)."""

    async def run() -> None:
        indexer_config, store, service = await _backfill_service(config)
        try:
            output = []
            for selected in indexer_config.select(chain):
                output.append(await service.seed_orders(selected, limit, batch_size))
            _print(output)
        finally:
            await store.close()

    asyncio.run(run())


@backfill_orderbook_app.command("seed-owners")
def backfill_orderbook_seed_owners(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    limit: Annotated[int | None, typer.Option("--limit", min=1)] = None,
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Enqueue owner_orders_backfill items for every distinct trader (terminal work
    states make re-seeding a no-op)."""

    async def run() -> None:
        indexer_config, store, service = await _backfill_service(config)
        try:
            output = []
            for selected in indexer_config.select(chain):
                output.append(await service.seed_owners(selected, limit))
            _print(output)
        finally:
            await store.close()

    asyncio.run(run())


@backfill_orderbook_app.command("drain")
def backfill_orderbook_drain(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    run_seconds: Annotated[float | None, typer.Option("--run-seconds", min=1)] = None,
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Process ONLY the backfill work kinds with a dedicated rate limiter
    (COW_BACKFILL_* knobs). Safe to run beside `continuous`; Ctrl-C safe (leases
    expire and items are re-leased)."""

    async def run() -> None:
        indexer_config, store, service = await _backfill_service(config)
        try:
            _print(await service.drain(indexer_config.select(chain), run_seconds))
        finally:
            await store.close()

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


@backfill_orderbook_app.command("status")
def backfill_orderbook_status(
    chain: Annotated[str, typer.Option("--chain")] = "all",
    config: ConfigOption = DEFAULT_CONFIG,
) -> None:
    """Per chain: backfill work-item counts by kind/status plus orders coverage per
    source with min/max(creation_date)."""

    async def run() -> None:
        indexer_config, store, service = await _backfill_service(config)
        try:
            _print(await service.status(indexer_config.select(chain)))
        finally:
            await store.close()

    asyncio.run(run())


if __name__ == "__main__":
    app()
