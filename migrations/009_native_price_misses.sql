-- Tokens the native-price endpoint has no price for.
--
-- tokens_with_fresh_price reads native_prices, which store_native_price only writes on
-- SUCCESS. A token that 404s therefore never becomes "fresh" and the sweep re-asks it on
-- every pass -- every price_interval_seconds (900s), not every price_refresh_seconds.
-- Measured 2026-09-16 on GCP: /api/v1/token/{address}/native_price was 42-46% of ALL CoW
-- API traffic at a ~65-72% 404 rate, i.e. roughly a third of the request budget spent
-- re-asking questions already answered "no". Those misses also keep regenerating `token`
-- work items, which crowd out live order enrichment.
--
-- Deliberately a separate table rather than a sentinel row in native_prices: cerebro-mcp
-- reads that table (token_native_prices in cow_explorer) and an empty-price row would
-- change its shape.
--
-- ORDER BY omits observed_at (unlike native_prices, which is append-only history) so the
-- ReplacingMergeTree keeps ONE row per token -- the latest attempt. This is a cache, not
-- a log.
CREATE TABLE IF NOT EXISTS __DATABASE__.native_price_misses
(
    environment LowCardinality(String),
    chain_id UInt64,
    token String,
    reason LowCardinality(String),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
ORDER BY (environment, chain_id, token);
