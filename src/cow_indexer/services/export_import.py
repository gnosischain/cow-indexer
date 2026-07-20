from __future__ import annotations

from pathlib import Path

import structlog

from cow_indexer.config import ChainConfig
from cow_indexer.models import ImportStats
from cow_indexer.sources.exports.manifest import ExportManifest, load_manifest
from cow_indexer.sources.exports.reader import FileInspection, inspect_file, iter_rows
from cow_indexer.storage.clickhouse import ClickHouseStore

log = structlog.get_logger()


def inspect_bundle(
    bundle: Path, schema_path: Path | None = None
) -> tuple[ExportManifest, list[FileInspection]]:
    manifest = load_manifest(bundle, schema_path)
    inspections = [inspect_file(bundle, manifest, item) for item in manifest.files]
    return manifest, inspections


class ExportImportService:
    def __init__(self, store: ClickHouseStore, chain: ChainConfig) -> None:
        self.store = store
        self.chain = chain

    async def run(
        self,
        bundle: Path,
        *,
        schema_path: Path | None = None,
        verify_checksums: bool = True,
        enqueue_enrichment: bool = False,
        batch_size: int = 10_000,
    ) -> tuple[ExportManifest, ImportStats]:
        manifest = load_manifest(bundle, schema_path)
        if (
            manifest.chain_id != self.chain.chain_id
            or manifest.environment != self.chain.environment
        ):
            raise ValueError(
                f"bundle targets {manifest.environment}/{manifest.chain_id}, "
                f"but selected chain is {self.chain.environment}/{self.chain.chain_id}"
            )
        stats = ImportStats()
        await self.store.record_import_run(manifest, "running", stats)
        try:
            for item in manifest.files:
                if await self.store.import_file_done(
                    str(manifest.bundle_id), item.dataset, str(item.path), item.sha256
                ):
                    continue
                if verify_checksums:
                    inspection = inspect_file(bundle, manifest, item)
                    if not inspection.valid:
                        raise ValueError(
                            f"invalid export file {item.path}: rows "
                            f"{inspection.actual_rows}/{inspection.expected_rows}, "
                            f"checksum_ok={inspection.checksum_ok}"
                        )
                imported_rows = 0
                try:
                    for rows in iter_rows(bundle, manifest, item, batch_size):
                        batch_stats = await self.store.import_rows(
                            manifest, item.dataset, rows, str(manifest.bundle_id)
                        )
                        stats.add(batch_stats)
                        imported_rows += len(rows)
                        if enqueue_enrichment and item.dataset == "orders":
                            for row in rows:
                                uid = row.get("order_uid") or row.get("uid")
                                if uid:
                                    await self.store.enqueue_work(self.chain, "order_uid", uid)
                    await self.store.record_import_file(
                        str(manifest.bundle_id),
                        item.dataset,
                        str(item.path),
                        item.sha256,
                        imported_rows,
                        "complete",
                    )
                except Exception as exc:
                    await self.store.record_import_file(
                        str(manifest.bundle_id),
                        item.dataset,
                        str(item.path),
                        item.sha256,
                        imported_rows,
                        "failed",
                        f"{type(exc).__name__}: {exc}",
                    )
                    raise
            await self.store.record_import_run(manifest, "complete", stats)
            return manifest, stats
        except Exception as exc:
            await self.store.record_import_run(
                manifest, "failed", stats, f"{type(exc).__name__}: {exc}"
            )
            raise
