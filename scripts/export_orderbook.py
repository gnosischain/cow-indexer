#!/usr/bin/env python3
"""Export source-specific PostgreSQL queries into the stable CoW indexer bundle format.

Every query must alias its columns to the canonical names documented in
export-schema/v1. Keeping the queries outside this script prevents a CoW
services migration from silently changing the import contract.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def parse_dataset(value: str) -> tuple[str, Path]:
    try:
        dataset, path = value.split("=", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected DATASET=QUERY.sql") from exc
    if not dataset or not path:
        raise argparse.ArgumentTypeError("expected DATASET=QUERY.sql")
    return dataset, Path(path)


def json_value(value: Any) -> Any:
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value
    if isinstance(value, (dict, list, str, int, float, bool, bytes)) or value is None:
        return value
    return str(value)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dump_manifest(path: Path, payload: dict[str, Any]) -> None:
    def default(value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat().replace("+00:00", "Z")
        raise TypeError(type(value).__name__)

    path.write_text(json.dumps(payload, indent=2, default=default) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn-env", default="COW_EXPORT_PG_DSN")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--network", required=True)
    parser.add_argument("--chain-id", type=int, required=True)
    parser.add_argument("--environment", default="production")
    parser.add_argument("--source", default="cow-orderbook-postgres")
    parser.add_argument("--source-schema-version", required=True)
    parser.add_argument("--dataset", action="append", type=parse_dataset, required=True)
    parser.add_argument("--fetch-size", type=int, default=10_000)
    args = parser.parse_args()

    dsn = os.getenv(args.dsn_env)
    if not dsn:
        parser.error(f"environment variable {args.dsn_env} is required")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error(f"output directory is not empty: {args.output}")
    args.output.mkdir(parents=True, exist_ok=True)

    try:
        import psycopg
    except ImportError as exc:
        raise SystemExit(
            "install the postgres-export extra: uv sync --extra postgres-export"
        ) from exc

    files: list[dict[str, Any]] = []
    snapshot_at = datetime.now(UTC)
    with psycopg.connect(dsn) as connection:
        with connection.transaction():
            connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            for dataset, query_path in args.dataset:
                dataset_dir = args.output / dataset
                dataset_dir.mkdir(parents=True, exist_ok=True)
                relative = Path(dataset) / "part-00000.parquet"
                output_path = args.output / relative
                row_count = 0
                writer: pq.ParquetWriter | None = None
                with connection.cursor(name=f"cow_export_{dataset}") as cursor:
                    cursor.execute(query_path.read_text())
                    columns = [description.name for description in cursor.description]
                    while batch := cursor.fetchmany(args.fetch_size):
                        rows = [
                            {
                                column: json_value(value)
                                for column, value in zip(columns, row, strict=True)
                            }
                            for row in batch
                        ]
                        table = pa.Table.from_pylist(rows)
                        if writer is None:
                            writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
                        writer.write_table(table)
                        row_count += len(rows)
                if writer is None:
                    empty = pa.table({})
                    pq.write_table(empty, output_path, compression="zstd")
                else:
                    writer.close()
                files.append(
                    {
                        "dataset": dataset,
                        "path": str(relative),
                        "rows": row_count,
                        "sha256": sha256_file(output_path),
                    }
                )

    manifest = {
        "format_version": 1,
        "bundle_id": str(uuid.uuid4()),
        "source": args.source,
        "source_schema_version": args.source_schema_version,
        "environment": args.environment,
        "network": args.network,
        "chain_id": args.chain_id,
        "snapshot_at": snapshot_at,
        "coverage": {},
        "files": files,
    }
    dump_manifest(args.output / "manifest.json", manifest)


if __name__ == "__main__":
    main()
