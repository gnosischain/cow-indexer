from pathlib import Path

from cow_indexer.storage.migrations import migration_files, quote_database, split_sql

ROOT = Path(__file__).parents[2]


def test_migrations_are_contiguous_and_split() -> None:
    files = migration_files(ROOT / "migrations")
    assert [path.name[:3] for path in files] == [f"{index:03}" for index in range(9)]
    for path in files:
        assert split_sql(path.read_text())


def test_split_sql_ignores_line_comments() -> None:
    source = (
        "-- a comment with a semicolon; and an apostrophe in ClickHouse's analyzer\n"
        "CREATE VIEW v AS SELECT 1 WHERE source = 'rpc';\n"
        "-- trailing note; still ignored\n"
        "CREATE VIEW w AS SELECT 2;\n"
    )
    statements = split_sql(source)
    assert len(statements) == 2
    assert statements[0].startswith("CREATE VIEW v")
    assert statements[1].startswith("CREATE VIEW w")


def test_database_name_is_strictly_quoted() -> None:
    assert quote_database("cow_indexer") == "`cow_indexer`"
    try:
        quote_database("cow-indexer; DROP DATABASE default")
    except ValueError:
        pass
    else:
        raise AssertionError("unsafe database name was accepted")
