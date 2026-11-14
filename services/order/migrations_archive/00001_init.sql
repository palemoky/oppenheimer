-- +goose Up

-- 冷库（归档库）的表，设计文档 1c 第 6.5 节、ADR-0016。
-- 本文件是 text/template（见 mysqlx.MigrateTemplate），模板数据是 order.ArchiveMigrationData：
-- Partitions 是按年的分区，Below 是下一年 1 月 1 日对应的订单号下界（idgen.LowerBound）。
--
-- 列与热库的 orders_N、order_items_N、payment_results_N 一一对应，另加 archived_at。和热表的区别：
--   - 不按用户分片，一个库里一套表。冷数据没有写入压力。
--   - 按 order_id 做 RANGE 分区。订单号的高位是时间，所以这就是按时间分区，分区键就是主键。
--     过了保存期限的整年数据可以 DROP PARTITION。
--   - 压缩行格式。
--   - 没有 (user_id, idempotency_key) 唯一键（分区表的唯一键必须包含分区键；归档后幂等键不再去重），
--     没有 (status, expire_at)（后台任务只扫非终态，冷库里没有）。
--   - updated_at 没有 ON UPDATE：这里的行是热库里最终映像的原样拷贝。
--   - payment_results_archive 的主键是 (order_id, payment_id)：分区键必须在主键里。
--   - 没有 CHECK 约束：数据已经在热库里检查过，归档任务逐批比对摘要。
CREATE TABLE IF NOT EXISTS orders_archive (
    order_id         BIGINT      NOT NULL,
    user_id          BIGINT      NOT NULL,
    merchant_id      BIGINT      NULL,
    version          BIGINT      NOT NULL,
    status           TINYINT     NOT NULL COMMENT '只会是终态：3 PAID, 4 CANCELLED, 5 CLOSED, 6 FAILED',
    total_amount     BIGINT      NOT NULL,
    currency         CHAR(3)     NOT NULL,
    idempotency_key  VARCHAR(64) NOT NULL,
    request_hash     BINARY(32)  NOT NULL,
    payment_id       BIGINT      NULL,
    fail_reason      VARCHAR(64) NULL,
    expire_at        DATETIME(6) NOT NULL,
    paid_at          DATETIME(6) NULL,
    created_at       DATETIME(6) NOT NULL,
    updated_at       DATETIME(6) NOT NULL,
    archived_at      DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (order_id),
    -- 与热表的同名索引同构，按用户列出订单的语句（FORCE INDEX）在冷热两边通用。
    KEY idx_orders_user (user_id, order_id)
) ENGINE = InnoDB ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8
PARTITION BY RANGE (order_id) (
{{- range .Partitions}}
    PARTITION {{.Name}} VALUES LESS THAN ({{.Below}}),
{{- end}}
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS order_items_archive (
    order_id    BIGINT      NOT NULL,
    sku_id      BIGINT      NOT NULL,
    quantity    INT         NOT NULL,
    unit_price  BIGINT      NOT NULL,
    created_at  DATETIME(6) NOT NULL,
    PRIMARY KEY (order_id, sku_id)
) ENGINE = InnoDB ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8
PARTITION BY RANGE (order_id) (
{{- range .Partitions}}
    PARTITION {{.Name}} VALUES LESS THAN ({{.Below}}),
{{- end}}
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS payment_results_archive (
    order_id     BIGINT      NOT NULL,
    payment_id   BIGINT      NOT NULL,
    succeeded    TINYINT     NOT NULL,
    amount       BIGINT      NOT NULL,
    disposition  TINYINT     NOT NULL,
    created_at   DATETIME(6) NOT NULL,
    PRIMARY KEY (order_id, payment_id)
) ENGINE = InnoDB ROW_FORMAT = COMPRESSED KEY_BLOCK_SIZE = 8
PARTITION BY RANGE (order_id) (
{{- range .Partitions}}
    PARTITION {{.Name}} VALUES LESS THAN ({{.Below}}),
{{- end}}
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

-- +goose Down
DROP TABLE IF EXISTS payment_results_archive;
DROP TABLE IF EXISTS order_items_archive;
DROP TABLE IF EXISTS orders_archive;
