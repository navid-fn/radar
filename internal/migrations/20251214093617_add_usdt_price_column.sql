-- +goose Up
-- +goose StatementBegin
ALTER TABLE radar.trade 
ADD COLUMN IF NOT EXISTS usdt_price Float64;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE radar.trade 
DROP COLUMN IF EXISTS usdt_price;
-- +goose StatementEnd
