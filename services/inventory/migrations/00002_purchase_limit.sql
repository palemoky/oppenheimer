-- +goose Up

-- 每人限购数量，0 表示不限。库存闸门（Redis）装载 SKU 时读取它，限购在闸门里执行，
-- MySQL 这一侧不检查：预占时已经不知道是哪个用户、此前买过多少。
ALTER TABLE sku_stock
    ADD COLUMN purchase_limit INT NOT NULL DEFAULT 0 COMMENT '每人限购数量，0 表示不限' AFTER sold,
    ADD CONSTRAINT chk_sku_stock_purchase_limit CHECK (purchase_limit >= 0);

-- +goose Down
ALTER TABLE sku_stock
    DROP CHECK chk_sku_stock_purchase_limit,
    DROP COLUMN purchase_limit;
