-- +goose Up
-- 入库总量，和它的守恒式（设计文档 3e 的 4.2、ADR-0039）。
--
-- sku_stock 一直有一条写在注释里的恒等式：available + reserved + sold = 入库总量。等号右边那个数以前没有存，
-- 所以总量被改坏了（手工改了一个数、应用增量的代码出了错）没有任何地方会报。这里把它存下来并加上 CHECK：
-- 守恒由数据库在每次写入时保证，不用事后对账。
--
-- 默认值是表达式（MySQL 8.0.13 起），两个用处：
--   - 存量行：加列时按行求值，每一行的 stocked 就是它现在三个数之和。迁移时已经存在的偏差因此被当成事实
--     固定下来，这条约束只管今后。
--   - 以后的插入：本项目没有创建 SKU 的接口，SKU 都由脚本和测试直接插入，不写这一列时它等于三个数之和，
--     那些地方不用改。
-- 之后任何让三个数之和变化的写入都必须同时改 stocked（补货：available 和 stocked 一起加），否则被拒绝。
-- 预占、确认、释放不受影响：它们的增量三个数之和为 0（stock_pending 的 CHECK）。
--
-- 在 MySQL 9.7.4 上验证过：加列时存量行按表达式求值；不写这一列的插入通过；只改 available 的更新被拒绝。
-- 本文件只有一条 DDL，它自己是原子的，中途被杀后可以整版重跑。
ALTER TABLE sku_stock
    ADD COLUMN stocked BIGINT NOT NULL DEFAULT (available + reserved + sold) COMMENT '入库总量' AFTER sold,
    ADD CONSTRAINT chk_sku_stock_total CHECK (available + reserved + sold = stocked);

-- +goose Down
ALTER TABLE sku_stock
    DROP CONSTRAINT chk_sku_stock_total,
    DROP COLUMN stocked;
