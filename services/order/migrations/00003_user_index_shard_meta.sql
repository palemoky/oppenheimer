-- +goose Up

-- 本文件是 text/template，模板数据同 00001。
--
-- 一、按用户列出订单用订单号做游标（设计文档 1c 的 5.2）：
--   WHERE user_id = ? AND order_id < ? ORDER BY order_id DESC LIMIT n
-- 原来的 idx_orders_user_created (user_id, created_at) 配不上：InnoDB 的二级索引末尾隐含主键，
-- 它实际是 (user_id, created_at, order_id)，按 order_id 排序用不上。换成 (user_id, order_id)。
-- 旧索引没有任何查询在用，同一条 ALTER 里删掉。一条 ALTER 是原子的，所以只需要检查新索引在不在；
-- 和 00002 一样先查 information_schema，整版重跑时跳过已经改过的表。
{{range .Slots}}
SET @ddl = IF(
    (SELECT COUNT(*) FROM information_schema.statistics
      WHERE table_schema = DATABASE() AND table_name = 'orders_{{.}}' AND index_name = 'idx_orders_user') = 0,
    'ALTER TABLE orders_{{.}}
        ADD KEY idx_orders_user (user_id, order_id),
        DROP KEY idx_orders_user_created',
    'DO 0');
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
{{end}}

-- 二、本库在分片布局里的身份（ADR-0015）。只有一行，由 order 启动时写入（空表）并核对
-- （见 store.EnsureShardMeta），扩容工具在切换时改写。库数由部署配置决定之后，靠它挡住
-- “DSN 顺序写反”“拿旧配置连上扩容后的库”这类错误：不符就拒绝启动，而不是把订单写到找不到的地方。
CREATE TABLE IF NOT EXISTS shard_meta (
    id          TINYINT      NOT NULL COMMENT '恒为 1，保证只有一行',
    db_index    INT          NOT NULL COMMENT '本库的序号，从 0 开始',
    db_count    INT          NOT NULL COMMENT '总库数',
    slot_count  INT          NOT NULL COMMENT '总 slot 数',
    slots       VARCHAR(255) NOT NULL COMMENT '本库持有的 slot，升序，逗号分隔',
    updated_at  DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    CONSTRAINT chk_shard_meta_single CHECK (id = 1)
) ENGINE = InnoDB;

-- +goose Down
DROP TABLE IF EXISTS shard_meta;
{{range .Slots}}
ALTER TABLE orders_{{.}}
    ADD KEY idx_orders_user_created (user_id, created_at),
    DROP KEY idx_orders_user;
{{end}}
