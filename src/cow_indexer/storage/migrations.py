from __future__ import annotations

import re
from pathlib import Path

DATABASE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def quote_database(name: str) -> str:
    if not DATABASE_RE.fullmatch(name):
        raise ValueError(f"invalid ClickHouse database name: {name!r}")
    return f"`{name}`"


def split_sql(source: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    quote: str | None = None
    escaped = False
    for character in source:
        if escaped:
            buffer.append(character)
            escaped = False
            continue
        if character == "\\" and quote:
            buffer.append(character)
            escaped = True
            continue
        if character in {"'", '"', "`"}:
            if quote == character:
                quote = None
            elif quote is None:
                quote = character
        if character == ";" and quote is None:
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
        else:
            buffer.append(character)
    trailing = "".join(buffer).strip()
    if trailing:
        statements.append(trailing)
    return statements


def migration_files(directory: Path) -> list[Path]:
    return sorted(path for path in directory.glob("*.sql") if path.name[:3].isdigit())
