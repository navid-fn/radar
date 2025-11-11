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
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (source, trade_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS radar.trade;
-- +goose StatementEnd
