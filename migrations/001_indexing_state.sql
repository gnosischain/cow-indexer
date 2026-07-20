CREATE TABLE IF NOT EXISTS __DATABASE__.chain_blocks
(
    environment LowCardinality(String),
    chain_id UInt64,
    block_number UInt64,
    block_hash String,
    parent_hash String,
    block_timestamp DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, block_number);

CREATE TABLE IF NOT EXISTS __DATABASE__.indexing_checkpoints
(
    environment LowCardinality(String),
    chain_id UInt64,
    source LowCardinality(String),
    block_number UInt64,
    block_hash String,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (environment, chain_id, source);

CREATE TABLE IF NOT EXISTS __DATABASE__.indexing_ranges
(
    run_id UUID,
    environment LowCardinality(String),
    chain_id UInt64,
    from_block UInt64,
    to_block UInt64,
    rows UInt64,
    status LowCardinality(String),
    error String,
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree
ORDER BY (environment, chain_id, started_at, from_block);

CREATE TABLE IF NOT EXISTS __DATABASE__.work_items
(
    work_id String,
    environment LowCardinality(String),
    chain_id UInt64,
    kind LowCardinality(String),
    key String,
    payload String,
    status LowCardinality(String),
    attempts UInt16,
    lease_owner String,
    lease_until Nullable(DateTime64(3, 'UTC')),
    next_attempt_at DateTime64(3, 'UTC'),
    error String,
    revision UInt64,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(revision)
ORDER BY (environment, chain_id, work_id);

CREATE TABLE IF NOT EXISTS __DATABASE__.dead_letters
(
    work_id String,
    environment LowCardinality(String),
    chain_id UInt64,
    kind LowCardinality(String),
    key String,
    payload String,
    attempts UInt16,
    error String,
    failed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (environment, chain_id, failed_at, work_id);

CREATE TABLE IF NOT EXISTS __DATABASE__.runs
(
    run_id UUID,
    command LowCardinality(String),
    environment LowCardinality(String),
    chain_id UInt64,
    status LowCardinality(String),
    detail String,
    started_at DateTime64(3, 'UTC'),
    finished_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree
ORDER BY (started_at, run_id);

