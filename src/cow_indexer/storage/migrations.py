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
    index = 0
    length = len(source)
    while index < length:
        character = source[index]
        if escaped:
            buffer.append(character)
            escaped = False
            index += 1
            continue
        if character == "\\" and quote:
            buffer.append(character)
            escaped = True
            index += 1
            continue
        # Skip `-- ...` line comments when outside a string so their contents
        # (semicolons, apostrophes) cannot be mistaken for SQL tokens.
        if quote is None and character == "-" and index + 1 < length and source[index + 1] == "-":
            while index < length and source[index] != "\n":
                index += 1
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
        index += 1
    trailing = "".join(buffer).strip()
    if trailing:
        statements.append(trailing)
    return statements


def migration_files(directory: Path) -> list[Path]:
    return sorted(path for path in directory.glob("*.sql") if path.name[:3].isdigit())
