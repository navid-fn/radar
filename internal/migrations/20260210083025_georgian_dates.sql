-- +goose Up
-- +goose StatementBegin
CREATE TABLE radar.dim_calendar (
    gregorian_date Date,
    jalali_date String,
    jalali_year UInt16,
    jalali_month UInt8,
    jalali_month_name LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY gregorian_date;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS radar.dim_calendar;
-- +goose StatementEnd
