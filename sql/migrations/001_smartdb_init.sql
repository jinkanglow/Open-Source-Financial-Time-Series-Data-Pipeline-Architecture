-- Flyway Migration Script: 001_smartdb_init.sql
-- Version: 1.0
-- Description: Initialize Smart-DB schema with all 11 continuous aggregates
-- Based on: TimescaleDB Continuous Aggregates documentation

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
CREATE EXTENSION IF NOT EXISTS vector CASCADE;

-- ============================================================================
-- Raw Data Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS market_data_raw (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 2) NOT NULL,
    trade_id VARCHAR(50) NOT NULL,
    side VARCHAR(4) CHECK (side IN ('buy', 'sell')),
    bid NUMERIC(20, 8),
    ask NUMERIC(20, 8),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, symbol, trade_id)
);

-- Convert to hypertable
SELECT create_hypertable('market_data_raw', 'time', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_market_data_symbol_time 
    ON market_data_raw (symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_market_data_trade_id 
    ON market_data_raw (trade_id);

-- ============================================================================
-- Continuous Aggregates (11 Core Features)
-- ============================================================================

-- 1. ohlc_1m: 1-minute OHLC aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS ohlc_1m_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS minute,
    symbol,
    first(price, time) AS open,
    max(price) AS high,
    min(price) AS low,
    last(price, time) AS close,
    sum(volume) AS volume,
    count(*) AS trade_count
FROM market_data_raw
GROUP BY minute, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ohlc_1m_agg',
    start_offset => INTERVAL '1 hour',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

-- 2. sma_20: 20-period Simple Moving Average
CREATE MATERIALIZED VIEW IF NOT EXISTS sma_20_calc AS
SELECT
    time_bucket('1 minute', time) AS minute,
    symbol,
    avg(price) AS sma_20
FROM (
    SELECT 
        time,
        symbol,
        price,
        row_number() OVER (PARTITION BY symbol ORDER BY time DESC) as rn
    FROM market_data_raw
) ranked
WHERE rn <= 20
GROUP BY minute, symbol
WITH NO DATA;

-- 3. volatility_1h: 1-hour volatility
CREATE MATERIALIZED VIEW IF NOT EXISTS volatility_1h_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    symbol,
    stddev_samp(
        ln(price / LAG(price) OVER (PARTITION BY symbol ORDER BY time))
    ) AS volatility_1h,
    count(*) AS sample_count
FROM market_data_raw
GROUP BY hour, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('volatility_1h_agg',
    start_offset => INTERVAL '2 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '2 minutes',
    if_not_exists => TRUE
);

-- 4. vwap_5m: 5-minute VWAP
CREATE MATERIALIZED VIEW IF NOT EXISTS vwap_5m_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS five_min,
    symbol,
    sum(price * volume) / NULLIF(sum(volume), 0) AS vwap_5m,
    sum(volume) AS total_volume
FROM market_data_raw
GROUP BY five_min, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('vwap_5m_agg',
    start_offset => INTERVAL '30 minutes',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE
);

-- 5. trade_imbalance_5m: 5-minute Trade Imbalance
CREATE MATERIALIZED VIEW IF NOT EXISTS trade_imbalance_5m_agg
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS five_min,
    symbol,
    sum(CASE WHEN side = 'buy' THEN volume ELSE 0 END) AS buy_volume,
    sum(CASE WHEN side = 'sell' THEN volume ELSE 0 END) AS sell_volume,
    (sum(CASE WHEN side = 'buy' THEN volume ELSE 0 END) - 
     sum(CASE WHEN side = 'sell' THEN volume ELSE 0 END)) / 
    NULLIF(sum(volume), 0) AS trade_imbalance_5m
FROM market_data_raw
WHERE side IS NOT NULL
GROUP BY five_min, symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('trade_imbalance_5m_agg',
    start_offset => INTERVAL '30 minutes',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE
);

-- 6. large_trade_flags table (populated by Flink CEP)
CREATE TABLE IF NOT EXISTS large_trade_flags (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    trade_id VARCHAR(50) NOT NULL,
    large_trade_flag BOOLEAN DEFAULT TRUE,
    volume NUMERIC(20, 2),
    percentile_95 NUMERIC(20, 2),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, symbol, trade_id)
);

SELECT create_hypertable('large_trade_flags', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 7. bidask_spreads table (populated by Flink)
CREATE TABLE IF NOT EXISTS bidask_spreads (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    bid NUMERIC(20, 8),
    ask NUMERIC(20, 8),
    bidask_spread NUMERIC(20, 8) GENERATED ALWAYS AS (ask - bid) STORED,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('bidask_spreads', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_bidask_symbol_time 
    ON bidask_spreads (symbol, time DESC);

-- 8. regime_tags table
CREATE TABLE IF NOT EXISTS regime_tags (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price NUMERIC(20, 8),
    sma_20 NUMERIC(20, 8),
    regime_tag VARCHAR(10) GENERATED ALWAYS AS (
        CASE 
            WHEN price > sma_20 THEN 'up'
            WHEN price < sma_20 THEN 'down'
            ELSE 'neutral'
        END
    ) STORED,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, symbol)
);

SELECT create_hypertable('regime_tags', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_regime_symbol_time 
    ON regime_tags (symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_regime_tag 
    ON regime_tags (regime_tag);

-- 9. news_sentiment table
CREATE TABLE IF NOT EXISTS news_sentiment (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    news_id VARCHAR(100),
    news_sentiment_embedding vector(1536),
    sentiment_score NUMERIC(5, 4),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time, symbol, news_id)
);

SELECT create_hypertable('news_sentiment', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_news_sentiment_symbol_time 
    ON news_sentiment (symbol, time DESC);

-- 10. EWM 12 function
CREATE OR REPLACE FUNCTION calculate_ewm_12(
    p_symbol VARCHAR,
    p_time TIMESTAMPTZ
) RETURNS NUMERIC AS $$
DECLARE
    v_ewm NUMERIC;
    v_alpha NUMERIC := 0.15;
BEGIN
    WITH recent_prices AS (
        SELECT price, time
        FROM market_data_raw
        WHERE symbol = p_symbol
          AND time <= p_time
        ORDER BY time DESC
        LIMIT 12
    ),
    ewm_calc AS (
        SELECT 
            price,
            LAG(price) OVER (ORDER BY time DESC) as prev_price,
            row_number() OVER (ORDER BY time DESC) as rn
        FROM recent_prices
    )
    SELECT 
        CASE 
            WHEN rn = 1 THEN price
            ELSE v_alpha * price + (1 - v_alpha) * prev_price
        END
    INTO v_ewm
    FROM ewm_calc
    ORDER BY rn DESC
    LIMIT 1;
    
    RETURN COALESCE(v_ewm, 0);
END;
$$ LANGUAGE plpgsql;

-- 11. feature_pit_snapshot function
CREATE OR REPLACE FUNCTION feature_pit_snapshot(
    p_symbol VARCHAR,
    p_as_of_ts TIMESTAMPTZ
)
RETURNS TABLE (
    ohlc_1m_open NUMERIC,
    ohlc_1m_high NUMERIC,
    ohlc_1m_low NUMERIC,
    ohlc_1m_close NUMERIC,
    sma_20 NUMERIC,
    ewm_12 NUMERIC,
    volatility_1h NUMERIC,
    vwap_5m NUMERIC,
    large_trade_flag BOOLEAN,
    bidask_spread NUMERIC,
    trade_imbalance_5m NUMERIC,
    regime_tag VARCHAR,
    news_sentiment_embedding vector(1536),
    snapshot_time TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    WITH latest_ohlc AS (
        SELECT *
        FROM ohlc_1m_agg
        WHERE symbol = p_symbol
          AND minute <= p_as_of_ts
        ORDER BY minute DESC
        LIMIT 1
    ),
    latest_sma AS (
        SELECT sma_20
        FROM sma_20_calc
        WHERE symbol = p_symbol
          AND minute <= p_as_of_ts
        ORDER BY minute DESC
        LIMIT 1
    ),
    latest_vol AS (
        SELECT volatility_1h
        FROM volatility_1h_agg
        WHERE symbol = p_symbol
          AND hour <= p_as_of_ts
        ORDER BY hour DESC
        LIMIT 1
    ),
    latest_vwap AS (
        SELECT vwap_5m
        FROM vwap_5m_agg
        WHERE symbol = p_symbol
          AND five_min <= p_as_of_ts
        ORDER BY five_min DESC
        LIMIT 1
    ),
    latest_imbalance AS (
        SELECT trade_imbalance_5m
        FROM trade_imbalance_5m_agg
        WHERE symbol = p_symbol
          AND five_min <= p_as_of_ts
        ORDER BY five_min DESC
        LIMIT 1
    ),
    latest_regime AS (
        SELECT regime_tag
        FROM regime_tags
        WHERE symbol = p_symbol
          AND time <= p_as_of_ts
        ORDER BY time DESC
        LIMIT 1
    ),
    latest_bidask AS (
        SELECT bidask_spread
        FROM bidask_spreads
        WHERE symbol = p_symbol
          AND time <= p_as_of_ts
        ORDER BY time DESC
        LIMIT 1
    ),
    latest_large_trade AS (
        SELECT large_trade_flag
        FROM large_trade_flags
        WHERE symbol = p_symbol
          AND time <= p_as_of_ts
        ORDER BY time DESC
        LIMIT 1
    ),
    latest_news AS (
        SELECT news_sentiment_embedding
        FROM news_sentiment
        WHERE symbol = p_symbol
          AND time <= p_as_of_ts
        ORDER BY time DESC
        LIMIT 1
    )
    SELECT 
        o.open,
        o.high,
        o.low,
        o.close,
        s.sma_20,
        calculate_ewm_12(p_symbol, p_as_of_ts) AS ewm_12,
        v.volatility_1h,
        vw.vwap_5m,
        lt.large_trade_flag,
        ba.bidask_spread,
        ti.trade_imbalance_5m,
        r.regime_tag,
        n.news_sentiment_embedding,
        p_as_of_ts AS snapshot_time
    FROM latest_ohlc o
    LEFT JOIN latest_sma s ON TRUE
    LEFT JOIN latest_vol v ON TRUE
    LEFT JOIN latest_vwap vw ON TRUE
    LEFT JOIN latest_imbalance ti ON TRUE
    LEFT JOIN latest_regime r ON TRUE
    LEFT JOIN latest_bidask ba ON TRUE
    LEFT JOIN latest_large_trade lt ON TRUE
    LEFT JOIN latest_news n ON TRUE;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- Row Level Security (RLS)
-- ============================================================================

ALTER TABLE market_data_raw ENABLE ROW LEVEL SECURITY;
ALTER TABLE large_trade_flags ENABLE ROW LEVEL SECURITY;
ALTER TABLE bidask_spreads ENABLE ROW LEVEL SECURITY;
ALTER TABLE regime_tags ENABLE ROW LEVEL SECURITY;
ALTER TABLE news_sentiment ENABLE ROW LEVEL SECURITY;

-- ============================================================================
-- Comments
-- ============================================================================

COMMENT ON TABLE market_data_raw IS 'Raw market data hypertable - single source of truth';
COMMENT ON MATERIALIZED VIEW ohlc_1m_agg IS '1-minute OHLC continuous aggregate - SLA: ≤30s';
COMMENT ON MATERIALIZED VIEW sma_20_calc IS '20-period Simple Moving Average - SLA: ≤2min';
COMMENT ON MATERIALIZED VIEW volatility_1h_agg IS '1-hour volatility - SLA: ≤2min';
COMMENT ON MATERIALIZED VIEW vwap_5m_agg IS '5-minute VWAP - SLA: ≤30s';
COMMENT ON MATERIALIZED VIEW trade_imbalance_5m_agg IS '5-minute trade imbalance - SLA: ≤30s';
COMMENT ON FUNCTION feature_pit_snapshot IS 'Point-in-time feature snapshot - Core Feast interface - SLA: instant';
