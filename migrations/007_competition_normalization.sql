-- One-to-many mapping of a solver competition (auction) to its settlement
-- transaction hashes. Batch auctions settle across multiple transactions, which
-- the single tx_hash column on solver_competitions cannot represent.
CREATE TABLE IF NOT EXISTS __DATABASE__.competition_transactions
(
    environment LowCardinality(String),
    chain_id UInt64,
    auction_id UInt64,
    tx_index UInt32,
    tx_hash String,
    source LowCardinality(String),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, auction_id, tx_hash);

-- Per-solution settlement transaction hash (winning solution carries the
-- executed tx). Empty for solutions that were not executed.
ALTER TABLE __DATABASE__.competition_solutions ADD COLUMN IF NOT EXISTS tx_hash String;
