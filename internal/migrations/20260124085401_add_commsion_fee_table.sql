-- +goose Up
-- +goose StatementBegin
CREATE TABLE IF NOT EXISTS radar.commission
(
    source      String,
    symbol      String,
    taker       Float64,
    maker       Float64,
    inserted_at DateTime('Asia/Tehran') DEFAULT now()
) ENGINE = MergeTree 
ORDER BY (source, symbol, inserted_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS radar.commission;
-- +goose StatementEnd
