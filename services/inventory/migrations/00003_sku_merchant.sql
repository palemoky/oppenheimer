-- +goose Up

-- SKU 所属的商家（ADR-0014）。定下来之后不改：订单按它进商家维度的索引表，改了索引就对不上。
-- 默认值 1 一是给存量行用，二是本项目没有创建 SKU 的接口，SKU 都由脚本和测试直接插入，
-- 不关心商家的地方可以不写这一列。
-- 本文件只有一条 DDL，它自己是原子的，中途被杀后可以整版重跑。
ALTER TABLE sku_stock
    ADD COLUMN merchant_id BIGINT NOT NULL DEFAULT 1 COMMENT '所属商家，写入后不改' AFTER sku_id;

-- +goose Down
ALTER TABLE sku_stock DROP COLUMN merchant_id;
