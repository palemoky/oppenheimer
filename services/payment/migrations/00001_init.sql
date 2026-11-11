-- +goose Up

CREATE TABLE payments (
    payment_id   BIGINT      NOT NULL COMMENT '由 idgen 生成',
    order_id     BIGINT      NOT NULL,
    user_id      BIGINT      NOT NULL,
    status       TINYINT     NOT NULL COMMENT '与 proto PaymentStatus 的数值一致：1 PENDING, 2 SUCCEEDED, 3 FAILED, 4 REFUNDED',
    amount       BIGINT      NOT NULL COMMENT '最小货币单位（分）',
    currency     CHAR(3)     NOT NULL,
    outcome      TINYINT     NOT NULL COMMENT '模拟的支付结果：1 成功, 2 失败',
    -- 支付结果需要回调订单服务。0 表示订单服务尚未确认收到，由后台任务重试。
    notified     TINYINT     NOT NULL DEFAULT 0,
    notify_attempts INT      NOT NULL DEFAULT 0,
    created_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (payment_id),
    -- 一个订单只有一笔支付单，这是 CreatePayment 幂等的依据。
    UNIQUE KEY uk_payments_order (order_id),
    -- 后台任务扫描：待出结果的支付、待回调的支付。
    KEY idx_payments_pending (status, notified, updated_at),
    CONSTRAINT chk_payments_amount CHECK (amount >= 0)
) ENGINE = InnoDB;

-- +goose Down
DROP TABLE payments;
