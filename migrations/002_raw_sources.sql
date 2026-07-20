CREATE TABLE IF NOT EXISTS __DATABASE__.raw_rpc_logs
(
    environment LowCardinality(String),
    chain_id UInt64,
    contract_address String,
    topics Array(String),
    data String,
    block_number UInt64,
    block_hash String,
    transaction_hash String,
    transaction_index UInt32,
    log_index UInt32,
    removed Bool,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, block_number, transaction_hash, log_index, block_hash);

CREATE TABLE IF NOT EXISTS __DATABASE__.decoded_events
(
    environment LowCardinality(String),
    chain_id UInt64,
    contract_name LowCardinality(String),
    contract_address String,
    event_name LowCardinality(String),
    args String,
    block_number UInt64,
    block_hash String,
    block_timestamp Nullable(DateTime64(3, 'UTC')),
    transaction_hash String,
    transaction_index UInt32,
    log_index UInt32,
    removed Bool,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, transaction_hash, log_index, block_hash);

CREATE TABLE IF NOT EXISTS __DATABASE__.raw_api_payloads
(
    environment LowCardinality(String),
    chain_id UInt64,
    endpoint LowCardinality(String),
    source_key String,
    payload String,
    payload_hash String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, endpoint, source_key, payload_hash);

CREATE TABLE IF NOT EXISTS __DATABASE__.raw_export_rows
(
    bundle_id UUID,
    environment LowCardinality(String),
    chain_id UInt64,
    dataset LowCardinality(String),
    row_hash String,
    payload String,
    imported_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(imported_at)
ORDER BY (bundle_id, dataset, row_hash);

CREATE TABLE IF NOT EXISTS __DATABASE__.import_runs
(
    bundle_id UUID,
    environment LowCardinality(String),
    chain_id UInt64,
    source String,
    snapshot_at DateTime64(3, 'UTC'),
    status LowCardinality(String),
    accepted UInt64,
    duplicates UInt64,
    rejected UInt64,
    conflicts UInt64,
    error String,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY bundle_id;

CREATE TABLE IF NOT EXISTS __DATABASE__.import_files
(
    bundle_id UUID,
    dataset LowCardinality(String),
    path String,
    sha256 String,
    rows UInt64,
    status LowCardinality(String),
    error String,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (bundle_id, dataset, path, sha256);

CREATE TABLE IF NOT EXISTS __DATABASE__.import_conflicts
(
    bundle_id UUID,
    environment LowCardinality(String),
    chain_id UInt64,
    dataset LowCardinality(String),
    record_key String,
    conflict_type LowCardinality(String),
    existing_payload String,
    incoming_payload String,
    detected_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (bundle_id, dataset, detected_at, record_key);

