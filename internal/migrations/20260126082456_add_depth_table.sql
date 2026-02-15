-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS radar.orderbook (
    snapshot_id String,
    source LowCardinality(String),
    symbol LowCardinality(String),
    side LowCardinality(String),
    price Float64 CODEC(ZSTD(1)),
    volume Float64 CODEC(ZSTD(1)),
    last_update DateTime('Asia/Tehran'),
) ENGINE = MergeTree()
ORDER BY (source, symbol, snapshot_id, side)
SETTINGS merge_with_ttl_timeout = 3600,
min_rows_for_wide_part = 0,
min_bytes_for_wide_part = 0;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS radar.orderbook;
-- +goose StatementEnd
