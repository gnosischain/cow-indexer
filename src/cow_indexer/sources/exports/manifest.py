from __future__ import annotations

import json
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

from jsonschema import Draft202012Validator
from pydantic import BaseModel, Field, field_validator, model_validator

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class Coverage(BaseModel):
    orders_created_from: datetime | None = None
    orders_created_to: datetime | None = None


class ExportFile(BaseModel):
    dataset: str
    path: Path
    rows: int = Field(ge=0)
    sha256: str

    @field_validator("path")
    @classmethod
    def relative_safe_path(cls, value: Path) -> Path:
        if value.is_absolute() or ".." in value.parts:
            raise ValueError("export file paths must be relative and cannot contain '..'")
        return value

    @field_validator("sha256")
    @classmethod
    def valid_sha256(cls, value: str) -> str:
        lowered = value.lower()
        if not SHA256_RE.fullmatch(lowered):
            raise ValueError("sha256 must contain 64 hexadecimal characters")
        return lowered


class ExportManifest(BaseModel):
    format_version: Literal[1]
    bundle_id: uuid.UUID
    source: str
    source_schema_version: str
    environment: str
    network: str
    chain_id: int = Field(gt=0)
    snapshot_at: datetime
    coverage: Coverage = Field(default_factory=Coverage)
    files: list[ExportFile]

    @model_validator(mode="after")
    def unique_files(self) -> ExportManifest:
        identities = [(item.dataset, item.path) for item in self.files]
        if len(identities) != len(set(identities)):
            raise ValueError("manifest contains duplicate dataset/path entries")
        return self

    def resolve_file(self, bundle: Path, item: ExportFile) -> Path:
        root = bundle.resolve()
        resolved = (root / item.path).resolve()
        if not resolved.is_relative_to(root):
            raise ValueError(f"file escapes bundle root: {item.path}")
        return resolved


def load_manifest(bundle: Path, schema_path: Path | None = None) -> ExportManifest:
    manifest_path = bundle.resolve() / "manifest.json"
    payload = json.loads(manifest_path.read_text())
    if schema_path is not None:
        schema = json.loads(schema_path.read_text())
        Draft202012Validator(schema).validate(payload)
    return ExportManifest.model_validate(payload)
