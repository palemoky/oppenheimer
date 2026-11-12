-- +goose Up

-- 本地消息表（见 pkg/outbox）。订单状态变更和要发布的 OrderEvent 在同一个事务里写入，
-- 由 outbox 中继发到 Kafka。建表语句与 outbox.Schema 一致，由 migrations_test.go 校验。
CREATE TABLE outbox (
    id            BIGINT          NOT NULL AUTO_INCREMENT,
    topic         VARCHAR(128)    NOT NULL,
    msg_key       VARBINARY(128)  NOT NULL,
    payload       MEDIUMBLOB      NOT NULL,
    created_at    DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at  DATETIME(6)     NULL,
    PRIMARY KEY (id),
    KEY idx_outbox_pending (published_at, id)
) ENGINE = InnoDB;

-- 库存结清改由 order.events 驱动：事件与状态变更同事务写入，一定会被发出，
-- 不再需要“未结清”标记和补结清扫描。
ALTER TABLE orders
    DROP INDEX idx_orders_settle,
    DROP COLUMN stock_settled;

-- +goose Down
-- 已有的订单按“已结清”恢复：它们的库存由事件处理过了，回退后不应被补结清再扫一遍。
ALTER TABLE orders
    ADD COLUMN stock_settled TINYINT NOT NULL DEFAULT 1 AFTER fail_reason,
    ADD KEY idx_orders_settle (stock_settled, status, updated_at);
ALTER TABLE orders ALTER COLUMN stock_settled SET DEFAULT 0;
DROP TABLE outbox;
