-- +goose Up

-- 本文件是 text/template（见 mysqlx.MigrateTemplate），模板数据是 {Slots []int}：本库上的逻辑分片。
-- 布局是 sharding.MerchantIndexLayout：按 merchant_id 分片，表名带全局 slot 号，由 pkg/sharding.Table 生成同样的名字。
-- 每条 DDL 都写成可重入的（IF NOT EXISTS / IF EXISTS）：DDL 逐条隐式提交，中途被杀后整版重跑要能成功。
--
-- 每一行是订单库里一个订单（orders_N 的一行）在某个版本上的映像，只带列表页要显示的列（设计文档 1c 4.9）。
-- 只有 merchant_id 非空的订单才进来；merchant_id 写入后不变，所以一行不会在 slot 之间搬家。
{{range .Slots}}
CREATE TABLE IF NOT EXISTS merchant_orders_{{.}} (
    merchant_id   BIGINT      NOT NULL,
    order_id      BIGINT      NOT NULL,
    user_id       BIGINT      NOT NULL,
    status        TINYINT     NOT NULL COMMENT '与 proto OrderStatus 的数值一致',
    total_amount  BIGINT      NOT NULL COMMENT '最小货币单位（分）',
    currency      CHAR(3)     NOT NULL,
    created_at    DATETIME(6) NOT NULL COMMENT '订单的创建时间（UTC）',
    paid_at       DATETIME(6) NULL,
    src_version   BIGINT      NOT NULL COMMENT '源行的 version 列，只接受更大的版本',
    synced_at     DATETIME(6) NOT NULL COMMENT '本行最后一次被同步写入的时间（UTC）',
    PRIMARY KEY (merchant_id, order_id),
    KEY idx_merchant_status (merchant_id, status, order_id)
) ENGINE = InnoDB;
{{end}}

-- +goose Down
{{range .Slots}}
DROP TABLE IF EXISTS merchant_orders_{{.}};
{{end}}
