-- Redefine the reorg-aware canonical views so they never expose rows beyond the
-- durable committed RPC checkpoint. A scan inserts all raw logs for a range,
-- decodes them individually, and only then advances the checkpoint. The finality
-- rescan and repair runs also write blocks/events ahead of the checkpoint with
-- update_checkpoint=False. Consumers must not see those partially-processed or
-- not-yet-committed rows. The bound uses argMax(block_number, updated_at), which
-- reproduces ReplacingMergeTree(updated_at) semantics without FINAL. A plain
-- max() would be unsafe because checkpoint monotonicity is only enforced in the
-- application layer. Chains with no committed checkpoint expose no canonical rows.
--
-- The reorg join is nested in a subquery so the outer scope has only two tables
-- (the derived table plus the checkpoint aggregate). The ClickHouse analyzer
-- would otherwise prefix the SELECT star output columns as table.col in a
-- 3-table join, breaking downstream unqualified column references.

CREATE OR REPLACE VIEW __DATABASE__.raw_rpc_logs_canonical AS
SELECT c.*
FROM (
    SELECT l.*
    FROM __DATABASE__.raw_rpc_logs AS l FINAL
    INNER JOIN __DATABASE__.chain_blocks AS b FINAL
        ON l.environment = b.environment
        AND l.chain_id = b.chain_id
        AND l.block_number = b.block_number
        AND l.block_hash = b.block_hash
    WHERE NOT l.removed
) AS c
INNER JOIN (
    SELECT environment, chain_id, argMax(block_number, updated_at) AS cp
    FROM __DATABASE__.indexing_checkpoints
    WHERE source = 'rpc'
    GROUP BY environment, chain_id
) AS k ON c.environment = k.environment AND c.chain_id = k.chain_id
WHERE c.block_number <= k.cp;

CREATE OR REPLACE VIEW __DATABASE__.decoded_events_canonical AS
SELECT c.*
FROM (
    SELECT e.*
    FROM __DATABASE__.decoded_events AS e FINAL
    INNER JOIN __DATABASE__.chain_blocks AS b FINAL
        ON e.environment = b.environment
        AND e.chain_id = b.chain_id
        AND e.block_number = b.block_number
        AND e.block_hash = b.block_hash
    WHERE NOT e.removed
) AS c
INNER JOIN (
    SELECT environment, chain_id, argMax(block_number, updated_at) AS cp
    FROM __DATABASE__.indexing_checkpoints
    WHERE source = 'rpc'
    GROUP BY environment, chain_id
) AS k ON c.environment = k.environment AND c.chain_id = k.chain_id
WHERE c.block_number <= k.cp;

-- Trades keep API-sourced rows unconditionally (they may carry block_number 0
-- and an empty block_hash). Only rpc-sourced trades are hash-matched and
-- checkpoint-bounded.
CREATE OR REPLACE VIEW __DATABASE__.trades_canonical AS
SELECT c.* EXCEPT (_canonical_hash)
FROM (
    SELECT t.*, (t.source != 'rpc' OR t.block_hash = b.block_hash) AS _canonical_hash
    FROM __DATABASE__.trades AS t FINAL
    LEFT JOIN __DATABASE__.chain_blocks AS b FINAL
        ON t.environment = b.environment
        AND t.chain_id = b.chain_id
        AND t.block_number = b.block_number
) AS c
LEFT JOIN (
    SELECT environment, chain_id, argMax(block_number, updated_at) AS cp
    FROM __DATABASE__.indexing_checkpoints
    WHERE source = 'rpc'
    GROUP BY environment, chain_id
) AS k ON c.environment = k.environment AND c.chain_id = k.chain_id
WHERE c._canonical_hash AND (c.source != 'rpc' OR c.block_number <= k.cp);

CREATE OR REPLACE VIEW __DATABASE__.settlements_canonical AS
SELECT c.*
FROM (
    SELECT s.*
    FROM __DATABASE__.settlements AS s FINAL
    INNER JOIN __DATABASE__.chain_blocks AS b FINAL
        ON s.environment = b.environment
        AND s.chain_id = b.chain_id
        AND s.block_number = b.block_number
        AND s.block_hash = b.block_hash
) AS c
INNER JOIN (
    SELECT environment, chain_id, argMax(block_number, updated_at) AS cp
    FROM __DATABASE__.indexing_checkpoints
    WHERE source = 'rpc'
    GROUP BY environment, chain_id
) AS k ON c.environment = k.environment AND c.chain_id = k.chain_id
WHERE c.block_number <= k.cp;

CREATE OR REPLACE VIEW __DATABASE__.interactions_canonical AS
SELECT c.*
FROM (
    SELECT i.*
    FROM __DATABASE__.interactions AS i FINAL
    INNER JOIN __DATABASE__.chain_blocks AS b FINAL
        ON i.environment = b.environment
        AND i.chain_id = b.chain_id
        AND i.block_number = b.block_number
        AND i.block_hash = b.block_hash
) AS c
INNER JOIN (
    SELECT environment, chain_id, argMax(block_number, updated_at) AS cp
    FROM __DATABASE__.indexing_checkpoints
    WHERE source = 'rpc'
    GROUP BY environment, chain_id
) AS k ON c.environment = k.environment AND c.chain_id = k.chain_id
WHERE c.block_number <= k.cp;
