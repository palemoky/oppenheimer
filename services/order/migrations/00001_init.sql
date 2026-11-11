-- +goose Up

CREATE TABLE orders (
    order_id         BIGINT      NOT NULL COMMENT '由 idgen 生成，低 6 位是 user_id 的分片基因',
    user_id          BIGINT      NOT NULL,
    status           TINYINT     NOT NULL COMMENT '与 proto OrderStatus 的数值一致：1 CREATING, 2 PENDING_PAYMENT, 3 PAID, 4 CANCELLED, 5 CLOSED, 6 FAILED',
    total_amount     BIGINT      NOT NULL DEFAULT 0 COMMENT '最小货币单位（分），预占成功后写入',
    currency         CHAR(3)     NOT NULL DEFAULT 'CNY',
    idempotency_key  VARCHAR(64) NOT NULL,
    request_hash     BINARY(32)  NOT NULL COMMENT '请求内容（商品与数量）的 SHA-256，用于识别幂等键被复用到不同请求',
    payment_id       BIGINT      NULL COMMENT '付清本订单的支付单',
    fail_reason      VARCHAR(64) NULL COMMENT '创建失败的原因码',
    -- 进入终态后还需要通知库存服务（确认或释放）。0 表示尚未完成，由后台任务重试直到成功。
    stock_settled    TINYINT     NOT NULL DEFAULT 0,
    expire_at        DATETIME(6) NOT NULL COMMENT '支付截止时间',
    paid_at          DATETIME(6) NULL,
    created_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_id),
    UNIQUE KEY uk_orders_user_idem (user_id, idempotency_key),
    KEY idx_orders_user_created (user_id, created_at),
    -- 后台任务按状态扫描：超时的待支付订单、卡在 CREATING 的订单、未结清库存的终态订单。
    KEY idx_orders_status_expire (status, expire_at),
    KEY idx_orders_settle (stock_settled, status, updated_at),
    CONSTRAINT chk_orders_total CHECK (total_amount >= 0)
) ENGINE = InnoDB;

CREATE TABLE order_items (
    order_id    BIGINT      NOT NULL,
    sku_id      BIGINT      NOT NULL,
    quantity    INT         NOT NULL,
    unit_price  BIGINT      NOT NULL DEFAULT 0 COMMENT '下单时的单价快照，预占成功后写入',
    created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_id, sku_id),
    CONSTRAINT chk_order_items_quantity CHECK (quantity > 0)
) ENGINE = InnoDB;

-- 已处理过的支付结果，以 payment_id 幂等，并记下当时给支付服务的答复。
CREATE TABLE payment_results (
    payment_id   BIGINT      NOT NULL,
    order_id     BIGINT      NOT NULL,
    succeeded    TINYINT     NOT NULL,
    amount       BIGINT      NOT NULL,
    disposition  TINYINT     NOT NULL COMMENT '与 proto PaymentDisposition 的数值一致：1 ACCEPTED, 2 REFUND_REQUIRED',
    created_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (payment_id),
    KEY idx_payment_results_order (order_id)
) ENGINE = InnoDB;

-- +goose Down
DROP TABLE payment_results;
DROP TABLE order_items;
DROP TABLE orders;
