-- +goose Up
-- 还没有应用到 sku_stock 上的库存增量（设计文档 1d 第 3 节、ADR-0018）。
--
-- 预占、确认、释放不再直接改 sku_stock：买同一个 SKU 的所有订单改的是同一行，行锁要拿到事务提交，
-- 落库吞吐被它卡住。它们改为在各自的事务里往这张表插入一行“三个数各要变多少”，
-- 由后台的应用者按 SKU 求和后一次更新 sku_stock，同一个事务里删掉这批行。
--
-- 所以这是一张队列表，应用之后就删，不是流水；流水是 reservation 和 reservation_item。
-- 任何要拿库存数做判断的读取都必须加上这张表里没应用的增量（store.GetStock）。
--
-- 每一行自身满足 d_available + d_reserved + d_sold = 0：增量只在三个数之间挪动，不改变总量。
--   预占  d_available = -n, d_reserved = +n
--   释放  d_available = +n, d_reserved = -n
--   确认                    d_reserved = -n, d_sold = +n
-- 本文件只有一条 DDL，它自己是原子的，中途被杀后可以整版重跑。
CREATE TABLE IF NOT EXISTS stock_pending (
    id           BIGINT      NOT NULL AUTO_INCREMENT,
    sku_id       BIGINT      NOT NULL,
    d_available  BIGINT      NOT NULL,
    d_reserved   BIGINT      NOT NULL,
    d_sold       BIGINT      NOT NULL,
    order_id     BIGINT      NOT NULL COMMENT '产生这条增量的订单，排查用',
    created_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    -- 读“真实的数”时按 SKU 求和。
    KEY idx_stock_pending_sku (sku_id),
    CONSTRAINT chk_stock_pending_balanced CHECK (d_available + d_reserved + d_sold = 0)
) ENGINE = InnoDB;

-- +goose Down
DROP TABLE IF EXISTS stock_pending;
