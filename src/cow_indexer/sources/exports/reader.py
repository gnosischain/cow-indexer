from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from cow_indexer.sources.exports.manifest import ExportFile, ExportManifest
from cow_indexer.utils import sha256_file


@dataclass(slots=True)
class FileInspection:
    dataset: str
    path: str
    expected_rows: int
    actual_rows: int
    checksum_ok: bool

    @property
    def valid(self) -> bool:
        return self.expected_rows == self.actual_rows and self.checksum_ok


def inspect_file(bundle: Path, manifest: ExportManifest, item: ExportFile) -> FileInspection:
    path = manifest.resolve_file(bundle, item)
    if not path.is_file():
        raise FileNotFoundError(path)
    metadata = pq.read_metadata(path)
    return FileInspection(
        dataset=item.dataset,
        path=str(item.path),
        expected_rows=item.rows,
        actual_rows=metadata.num_rows,
        checksum_ok=sha256_file(path) == item.sha256,
    )


def iter_rows(
    bundle: Path,
    manifest: ExportManifest,
    item: ExportFile,
    batch_size: int = 10_000,
) -> Iterator[list[dict]]:
    path = manifest.resolve_file(bundle, item)
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=batch_size):
        yield pa.Table.from_batches([batch]).to_pylist()
