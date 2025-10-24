-- enable timescaledb
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- create bitcoin prices table
CREATE TABLE IF NOT EXISTS bitcoin_prices (
    timestamp TIMESTAMPTZ PRIMARY KEY,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume_btc NUMERIC,
    volume_usd NUMERIC,
    weighted_price NUMERIC
);

-- hypertable with day-long chucnks
SELECT create_hypertable('bitcoin_prices', 'timestamp', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');
