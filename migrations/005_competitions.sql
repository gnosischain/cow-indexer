CREATE TABLE IF NOT EXISTS __DATABASE__.auctions
(
    environment LowCardinality(String),
    chain_id UInt64,
    auction_id UInt64,
    block_number UInt64,
    deadline Nullable(DateTime64(3, 'UTC')),
    source LowCardinality(String),
    payload String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, auction_id);

CREATE TABLE IF NOT EXISTS __DATABASE__.solver_competitions
(
    environment LowCardinality(String),
    chain_id UInt64,
    auction_id UInt64,
    tx_hash String,
    winner String,
    reference_score String,
    auction_block UInt64,
    source LowCardinality(String),
    raw_payload String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, auction_id);

CREATE TABLE IF NOT EXISTS __DATABASE__.competition_solutions
(
    environment LowCardinality(String),
    chain_id UInt64,
    auction_id UInt64,
    solution_index UInt32,
    solver String,
    score String,
    ranking UInt32,
    is_winner Bool,
    payload String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, auction_id, solution_index);

CREATE TABLE IF NOT EXISTS __DATABASE__.auction_orders
(
    environment LowCardinality(String),
    chain_id UInt64,
    auction_id UInt64,
    order_uid String,
    payload String,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, auction_id, order_uid);

CREATE TABLE IF NOT EXISTS __DATABASE__.auction_prices
(
    environment LowCardinality(String),
    chain_id UInt64,
    auction_id UInt64,
    token String,
    price UInt256,
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, auction_id, token);
