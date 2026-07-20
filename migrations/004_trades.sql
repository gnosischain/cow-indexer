CREATE TABLE IF NOT EXISTS __DATABASE__.trades
(
    environment LowCardinality(String),
    chain_id UInt64,
    order_uid String,
    tx_hash String,
    log_index UInt32,
    block_number UInt64,
    block_hash String,
    block_timestamp Nullable(DateTime64(3, 'UTC')),
    owner String,
    sell_token String,
    buy_token String,
    sell_amount UInt256,
    buy_amount UInt256,
    fee_amount UInt256,
    source LowCardinality(String),
    raw_payload String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, tx_hash, log_index, order_uid);

CREATE TABLE IF NOT EXISTS __DATABASE__.protocol_fees
(
    environment LowCardinality(String),
    chain_id UInt64,
    order_uid String,
    tx_hash String,
    log_index UInt32,
    fee_index UInt16,
    token String,
    amount UInt256,
    policy String,
    source LowCardinality(String),
    raw_payload String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, tx_hash, log_index, order_uid, fee_index);

CREATE TABLE IF NOT EXISTS __DATABASE__.settlements
(
    environment LowCardinality(String),
    chain_id UInt64,
    tx_hash String,
    block_number UInt64,
    block_hash String,
    block_timestamp Nullable(DateTime64(3, 'UTC')),
    solver String,
    log_index UInt32,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, tx_hash, log_index);

CREATE TABLE IF NOT EXISTS __DATABASE__.interactions
(
    environment LowCardinality(String),
    chain_id UInt64,
    tx_hash String,
    block_number UInt64,
    block_hash String,
    block_timestamp Nullable(DateTime64(3, 'UTC')),
    log_index UInt32,
    target String,
    value UInt256,
    selector String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, tx_hash, log_index);
