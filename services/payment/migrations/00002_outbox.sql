-- +goose Up

-- 本地消息表，SQL 与 pkg/outbox.Schema 一致。支付出结果与 PaymentEvent 在同一个事务里写入。
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

-- 不再轮询回调订单服务：notify_attempts 只用于回调的退避，删除。
-- notified 保留，含义从“回调已成功”变为“订单服务已答复”。
--
-- 原索引 (status, notified, updated_at) 是为“已出结果、未回调、到期重试”的扫描建的。
-- 现在后台只剩“创建超过模拟延迟的 PENDING 支付单”这一种扫描，按 (status, created_at) 建索引，
-- 让 WHERE status = PENDING AND created_at <= ? ORDER BY created_at LIMIT ? 直接走索引有序读取。
ALTER TABLE payments
    DROP COLUMN notify_attempts,
    DROP INDEX idx_payments_pending,
    ADD KEY idx_payments_pending (status, created_at),
    MODIFY COLUMN notified TINYINT NOT NULL DEFAULT 0 COMMENT '订单服务已答复（order.events 的 payment_result_handled）';

-- +goose Down

ALTER TABLE payments
    DROP INDEX idx_payments_pending,
    ADD KEY idx_payments_pending (status, notified, updated_at),
    ADD COLUMN notify_attempts INT NOT NULL DEFAULT 0 AFTER notified,
    MODIFY COLUMN notified TINYINT NOT NULL DEFAULT 0;

DROP TABLE outbox;
