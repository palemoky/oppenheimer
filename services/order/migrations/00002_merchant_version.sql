-- +goose Up

-- 本文件是 text/template，模板数据同 00001。
--
-- merchant_id：订单所属的商家，预占成功（CREATING -> PENDING_PAYMENT）时写入，之后不改。
--   还在 CREATING、或预占前就失败的订单为 NULL。商家维度的索引表按它分片（ADR-0014）。
-- version：行版本号，每次更新这一行加一。CDC 的下游只接受版本更高的行映像，
--   靠它去重和定序。更新 orders 的语句都必须带 version = version + 1。
--
-- 每张表一条 ALTER 同时加两列，单条 DDL 是原子的。但一个版本里有多张表，进程在中间被杀会留下
-- 前几张表改了、后几张没改、版本号没记的状态，而 MySQL 没有 ADD COLUMN IF NOT EXISTS。
-- 所以每条 ALTER 前先查 information_schema，列已经在就跳过，整版重跑能成功。
-- 用户变量和预处理语句是会话级的：goose 在同一个连接上执行一个版本里的所有语句，所以可用。
{{range .Slots}}
SET @ddl = IF(
    (SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'orders_{{.}}' AND column_name = 'version') = 0,
    'ALTER TABLE orders_{{.}}
        ADD COLUMN merchant_id BIGINT NULL COMMENT ''所属商家，预占成功时写入，之后不改'' AFTER user_id,
        ADD COLUMN version BIGINT NOT NULL DEFAULT 0 COMMENT ''行版本号，每次更新加一''',
    'DO 0');
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
{{end}}

-- +goose Down
{{range .Slots}}
ALTER TABLE orders_{{.}} DROP COLUMN merchant_id, DROP COLUMN version;
{{end}}
