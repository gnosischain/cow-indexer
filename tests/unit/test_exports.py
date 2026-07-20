import hashlib
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from cow_indexer.services.export_import import inspect_bundle
from cow_indexer.sources.exports.manifest import ExportManifest


def test_bundle_inspection_verifies_rows_and_checksum(tmp_path: Path) -> None:
    data_dir = tmp_path / "orders"
    data_dir.mkdir()
    parquet = data_dir / "part.parquet"
    pq.write_table(pa.table({"order_uid": ["a", "b"]}), parquet)
    digest = hashlib.sha256(parquet.read_bytes()).hexdigest()
    manifest = {
        "format_version": 1,
        "bundle_id": str(uuid.uuid4()),
        "source": "test",
        "source_schema_version": "abc",
        "environment": "production",
        "network": "mainnet",
        "chain_id": 1,
        "snapshot_at": datetime.now(UTC).isoformat(),
        "files": [
            {"dataset": "orders", "path": "orders/part.parquet", "rows": 2, "sha256": digest}
        ],
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    _, inspections = inspect_bundle(tmp_path)
    assert inspections[0].valid


def test_manifest_rejects_path_traversal() -> None:
    with pytest.raises(ValueError):
        ExportManifest.model_validate(
            {
                "format_version": 1,
                "bundle_id": str(uuid.uuid4()),
                "source": "test",
                "source_schema_version": "abc",
                "environment": "production",
                "network": "mainnet",
                "chain_id": 1,
                "snapshot_at": datetime.now(UTC),
                "files": [
                    {
                        "dataset": "orders",
                        "path": "../orders.parquet",
                        "rows": 0,
                        "sha256": "0" * 64,
                    }
                ],
            }
        )
