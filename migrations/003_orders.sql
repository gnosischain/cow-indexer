CREATE TABLE IF NOT EXISTS __DATABASE__.orders
(
    environment LowCardinality(String),
    chain_id UInt64,
    order_uid String,
    owner String,
    sell_token String,
    buy_token String,
    receiver Nullable(String),
    sell_amount UInt256,
    buy_amount UInt256,
    valid_to UInt32,
    app_data_hash String,
    fee_amount UInt256,
    kind LowCardinality(String),
    partially_fillable Bool,
    sell_token_balance LowCardinality(String),
    buy_token_balance LowCardinality(String),
    signing_scheme LowCardinality(String),
    signature String,
    creation_date DateTime64(3, 'UTC'),
    status LowCardinality(String),
    class LowCardinality(String),
    executed_sell_amount UInt256,
    executed_buy_amount UInt256,
    executed_fee_amount UInt256,
    immutable_hash String,
    source LowCardinality(String),
    raw_payload String,
    source_updated_at DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, order_uid);

CREATE TABLE IF NOT EXISTS __DATABASE__.order_events
(
    event_id String,
    environment LowCardinality(String),
    chain_id UInt64,
    order_uid String,
    owner String,
    event_type LowCardinality(String),
    source LowCardinality(String),
    block_number Nullable(UInt64),
    transaction_hash String,
    log_index Nullable(UInt32),
    event_timestamp Nullable(DateTime64(3, 'UTC')),
    payload String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, event_id);

