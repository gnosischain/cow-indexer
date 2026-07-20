CREATE TABLE IF NOT EXISTS __DATABASE__.app_data
(
    environment LowCardinality(String),
    chain_id UInt64,
    app_data_hash String,
    full_app_data String,
    source LowCardinality(String),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, app_data_hash);

CREATE TABLE IF NOT EXISTS __DATABASE__.token_metadata
(
    environment LowCardinality(String),
    chain_id UInt64,
    token String,
    symbol String,
    name String,
    decimals UInt8,
    source LowCardinality(String),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, token);

CREATE TABLE IF NOT EXISTS __DATABASE__.native_prices
(
    environment LowCardinality(String),
    chain_id UInt64,
    token String,
    native_price String,
    source LowCardinality(String),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, token, observed_at);

CREATE TABLE IF NOT EXISTS __DATABASE__.quotes
(
    environment LowCardinality(String),
    chain_id UInt64,
    quote_id String,
    owner String,
    sell_token String,
    buy_token String,
    sell_amount UInt256,
    buy_amount UInt256,
    fee_amount UInt256,
    payload String,
    source LowCardinality(String),
    created_at DateTime64(3, 'UTC'),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, quote_id);

CREATE VIEW IF NOT EXISTS __DATABASE__.raw_rpc_logs_canonical AS
SELECT l.*
FROM __DATABASE__.raw_rpc_logs AS l FINAL
INNER JOIN __DATABASE__.chain_blocks AS b FINAL
    ON l.environment = b.environment
    AND l.chain_id = b.chain_id
    AND l.block_number = b.block_number
    AND l.block_hash = b.block_hash
WHERE NOT l.removed;

CREATE VIEW IF NOT EXISTS __DATABASE__.decoded_events_canonical AS
SELECT e.*
FROM __DATABASE__.decoded_events AS e FINAL
INNER JOIN __DATABASE__.chain_blocks AS b FINAL
    ON e.environment = b.environment
    AND e.chain_id = b.chain_id
    AND e.block_number = b.block_number
    AND e.block_hash = b.block_hash
WHERE NOT e.removed;

CREATE VIEW IF NOT EXISTS __DATABASE__.trades_canonical AS
SELECT t.*
FROM __DATABASE__.trades AS t FINAL
LEFT JOIN __DATABASE__.chain_blocks AS b FINAL
    ON t.environment = b.environment
    AND t.chain_id = b.chain_id
    AND t.block_number = b.block_number
WHERE t.source != 'rpc' OR t.block_hash = b.block_hash;

CREATE VIEW IF NOT EXISTS __DATABASE__.settlements_canonical AS
SELECT s.*
FROM __DATABASE__.settlements AS s FINAL
INNER JOIN __DATABASE__.chain_blocks AS b FINAL
    ON s.environment = b.environment
    AND s.chain_id = b.chain_id
    AND s.block_number = b.block_number
    AND s.block_hash = b.block_hash;

CREATE VIEW IF NOT EXISTS __DATABASE__.interactions_canonical AS
SELECT i.*
FROM __DATABASE__.interactions AS i FINAL
INNER JOIN __DATABASE__.chain_blocks AS b FINAL
    ON i.environment = b.environment
    AND i.chain_id = b.chain_id
    AND i.block_number = b.block_number
    AND i.block_hash = b.block_hash;
