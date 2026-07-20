.PHONY: install test lint migrate backfill continuous

install:
	uv sync --all-extras

test:
	uv run pytest

lint:
	uv run ruff check .

migrate:
	uv run cow-indexer migrate

backfill:
	uv run cow-indexer backfill --chain all

continuous:
	uv run cow-indexer continuous --chain all

