-- +goose Up

-- SKU 的价格与库存。本项目没有商品服务，价格暂放在这里。
-- 恒等式：available + reserved + sold = 入库总量，三者都不为负。
CREATE TABLE sku_stock (
    sku_id      BIGINT       NOT NULL,
    name        VARCHAR(128) NOT NULL,
    unit_price  BIGINT       NOT NULL COMMENT '单价，最小货币单位（分）',
    currency    CHAR(3)      NOT NULL DEFAULT 'CNY',
    available   BIGINT       NOT NULL COMMENT '可售',
    reserved    BIGINT       NOT NULL DEFAULT 0 COMMENT '已被订单预占、尚未支付',
    sold        BIGINT       NOT NULL DEFAULT 0 COMMENT '已售',
    created_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (sku_id),
    -- 应用层用条件更新防超卖，CHECK 是最后一道防线。
    CONSTRAINT chk_sku_stock_available CHECK (available >= 0),
    CONSTRAINT chk_sku_stock_reserved  CHECK (reserved >= 0),
    CONSTRAINT chk_sku_stock_sold      CHECK (sold >= 0),
    CONSTRAINT chk_sku_stock_price     CHECK (unit_price >= 0)
) ENGINE = InnoDB;

-- 一笔订单的预占，同时是库存操作的幂等记录和流水。
-- 每个订单一行头记录，状态只能 RESERVED -> CONFIRMED 或 RESERVED -> RELEASED。
-- Release 先于 Reserve 到达时，直接插入一行 RELEASED，挡住迟到的 Reserve。
CREATE TABLE reservation (
    order_id    BIGINT      NOT NULL,
    status      TINYINT     NOT NULL COMMENT '1 RESERVED, 2 CONFIRMED, 3 RELEASED',
    created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_id)
) ENGINE = InnoDB;

CREATE TABLE reservation_item (
    order_id    BIGINT      NOT NULL,
    sku_id      BIGINT      NOT NULL,
    quantity    INT         NOT NULL,
    unit_price  BIGINT      NOT NULL COMMENT '预占时的单价快照',
    currency    CHAR(3)     NOT NULL,
    created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_id, sku_id),
    KEY idx_reservation_item_sku (sku_id),
    CONSTRAINT chk_reservation_item_quantity CHECK (quantity > 0)
) ENGINE = InnoDB;

-- +goose Down
DROP TABLE reservation_item;
DROP TABLE reservation;
DROP TABLE sku_stock;
