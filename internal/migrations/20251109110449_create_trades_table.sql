-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS radar.trade
(
    trade_id    String,
    source      String,
    symbol      String,
    side        String,
    price       Float64,
    base_amount Float64,
    quote_amount Float64,
    event_time  DateTime('Asia/Tehran'),
    inserted_at DateTime('Asia/Tehran') DEFAULT now()
) ENGINE = ReplacingMergeTree(event_time)
ORDER BY (source, trade_id)
SETTINGS 
    merge_with_ttl_timeout = 3600,
    min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS radar.trade;
-- +goose StatementEnd
