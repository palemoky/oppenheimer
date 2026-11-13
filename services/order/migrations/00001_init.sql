-- +goose Up

-- 本文件是 text/template（见 mysqlx.MigrateTemplate），模板数据是 {Slots []int}：本库上的逻辑分片。
-- 每个 slot 一组表，表名带全局 slot 号（ADR-0012），由 pkg/sharding.Table 生成同样的名字。
-- CHECK 约束名在库内必须唯一，所以也带 slot 号；索引名只需在表内唯一，不用带。
--
-- 渲染后一个版本里有十几条 DDL，而 MySQL 的 DDL 逐条隐式提交：进程在中间被杀会留下部分表、
-- 版本号却没记。所以每条都写成可重入的（IF NOT EXISTS / IF EXISTS），下次启动整版重跑能成功。
{{range .Slots}}
CREATE TABLE IF NOT EXISTS orders_{{.}} (
    order_id         BIGINT      NOT NULL COMMENT '由 idgen 生成，低 6 位是 user_id 的分片基因',
    user_id          BIGINT      NOT NULL,
    status           TINYINT     NOT NULL COMMENT '与 proto OrderStatus 的数值一致：1 CREATING, 2 PENDING_PAYMENT, 3 PAID, 4 CANCELLED, 5 CLOSED, 6 FAILED',
    total_amount     BIGINT      NOT NULL DEFAULT 0 COMMENT '最小货币单位（分），预占成功后写入',
    currency         CHAR(3)     NOT NULL DEFAULT 'CNY',
    idempotency_key  VARCHAR(64) NOT NULL,
    request_hash     BINARY(32)  NOT NULL COMMENT '请求内容（商品与数量）的 SHA-256，用于识别幂等键被复用到不同请求',
    payment_id       BIGINT      NULL COMMENT '付清本订单的支付单',
    fail_reason      VARCHAR(64) NULL COMMENT '创建失败的原因码',
    expire_at        DATETIME(6) NOT NULL COMMENT '支付截止时间',
    paid_at          DATETIME(6) NULL,
    created_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_id),
    UNIQUE KEY uk_orders_user_idem (user_id, idempotency_key),
    KEY idx_orders_user_created (user_id, created_at),
    -- 后台任务按状态扫描：超时的待支付订单、卡在 CREATING 的订单。
    KEY idx_orders_status_expire (status, expire_at),
    CONSTRAINT chk_orders_{{.}}_total CHECK (total_amount >= 0)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS order_items_{{.}} (
    order_id    BIGINT      NOT NULL,
    sku_id      BIGINT      NOT NULL,
    quantity    INT         NOT NULL,
    unit_price  BIGINT      NOT NULL DEFAULT 0 COMMENT '下单时的单价快照，预占成功后写入',
    created_at  DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_id, sku_id),
    CONSTRAINT chk_order_items_{{.}}_quantity CHECK (quantity > 0)
) ENGINE = InnoDB;

-- 已处理过的支付结果，以 payment_id 幂等，并记下当时给支付服务的答复。
-- 只在锁住订单行的事务里读写，所以和订单在同一个 slot。
CREATE TABLE IF NOT EXISTS payment_results_{{.}} (
    payment_id   BIGINT      NOT NULL,
    order_id     BIGINT      NOT NULL,
    succeeded    TINYINT     NOT NULL,
    amount       BIGINT      NOT NULL,
    disposition  TINYINT     NOT NULL COMMENT '与 proto PaymentDisposition 的数值一致：1 ACCEPTED, 2 REFUND_REQUIRED',
    created_at   DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (payment_id),
    KEY idx_payment_results_order (order_id)
) ENGINE = InnoDB;
{{end}}

-- 本地消息表（见 pkg/outbox），每个库一张，不按 slot 拆。订单状态变更和要发布的 OrderEvent
-- 在同一个事务里写入，由本库的 outbox 中继发到 Kafka。建表语句除了 IF NOT EXISTS 之外与 outbox.Schema 一致，由 migrations_test.go 校验。
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGINT          NOT NULL AUTO_INCREMENT,
    topic         VARCHAR(128)    NOT NULL,
    msg_key       VARBINARY(128)  NOT NULL,
    payload       MEDIUMBLOB      NOT NULL,
    created_at    DATETIME(6)     NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at  DATETIME(6)     NULL,
    PRIMARY KEY (id),
    KEY idx_outbox_pending (published_at, id)
) ENGINE = InnoDB;

-- +goose Down
DROP TABLE IF EXISTS outbox;
{{range .Slots}}
DROP TABLE IF EXISTS payment_results_{{.}};
DROP TABLE IF EXISTS order_items_{{.}};
DROP TABLE IF EXISTS orders_{{.}};
{{end}}
