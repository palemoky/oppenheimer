-- +goose Up

-- 本文件是 text/template，模板数据同 00001（这里用不到）。
--
-- outbox 加一列，存写入这一行时的链路上下文（W3C traceparent，55 个字符；NULL 表示当时没有链路）。
-- 中继是后台循环，它发消息时的 ctx 和写这一行的请求没有关系，上下文只能跟着数据走：pkg/outbox 的 Add
-- 在业务事务里写入，中继发布时放进消息的 header。见设计文档 1d 的 8.6、ADR-0023。
--
-- 加在最后一列，跑完之后表结构与 outbox.Schema 一致（migrations_test.go 拿实际的表结构比）。
-- MySQL 没有 ADD COLUMN IF NOT EXISTS，和 00002、00003 一样先查 information_schema，整版重跑时跳过。
-- 加一个可以为 NULL 的列是 INSTANT 的，不重建表。
SET @ddl = IF(
    (SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema = DATABASE() AND table_name = 'outbox' AND column_name = 'trace_context') = 0,
    'ALTER TABLE outbox ADD COLUMN trace_context VARCHAR(64) NULL',
    'DO 0');
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- +goose Down
ALTER TABLE outbox DROP COLUMN trace_context;
