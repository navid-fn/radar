-- +goose Up
CREATE TABLE IF NOT EXISTS ohlc (
    id String,
    source LowCardinality(String),
    symbol LowCardinality(String),
    interval LowCardinality(String),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    usdt_price Float64,
    open_time DateTime,
    inserted_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (source, symbol, interval, open_time);

-- +goose Down
DROP TABLE IF EXISTS ohlc;

