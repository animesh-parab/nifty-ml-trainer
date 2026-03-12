-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create main 1-min OHLCV table
CREATE TABLE IF NOT EXISTS nifty_1min (
    time        TIMESTAMPTZ       NOT NULL,
    open        DOUBLE PRECISION  NOT NULL,
    high        DOUBLE PRECISION  NOT NULL,
    low         DOUBLE PRECISION  NOT NULL,
    close       DOUBLE PRECISION  NOT NULL
);

-- Convert to hypertable (TimescaleDB magic)
SELECT create_hypertable('nifty_1min', 'time', if_not_exists => TRUE);

-- Index for fast time-range queries
CREATE INDEX IF NOT EXISTS idx_nifty_1min_time ON nifty_1min (time DESC);

-- 5-min continuous aggregate
CREATE MATERIALIZED VIEW nifty_5min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('5 minutes', time) AS time,
    FIRST(open, time)              AS open,
    MAX(high)                      AS high,
    MIN(low)                       AS low,
    LAST(close, time)              AS close
FROM nifty_1min
GROUP BY time_bucket('5 minutes', time);

-- 15-min continuous aggregate
CREATE MATERIALIZED VIEW nifty_15min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('15 minutes', time) AS time,
    FIRST(open, time)               AS open,
    MAX(high)                       AS high,
    MIN(low)                        AS low,
    LAST(close, time)               AS close
FROM nifty_1min
GROUP BY time_bucket('15 minutes', time);

-- 1-hour continuous aggregate
CREATE MATERIALIZED VIEW nifty_1hour
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS time,
    FIRST(open, time)           AS open,
    MAX(high)                   AS high,
    MIN(low)                    AS low,
    LAST(close, time)           AS close
FROM nifty_1min
GROUP BY time_bucket('1 hour', time);
