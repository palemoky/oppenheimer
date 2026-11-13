-- +goose Up

-- 预占时订单所属的商家，重复的 Reserve 据此返回同样的结果。
-- 可空：先于 Reserve 到达的 Release 留下的“已释放”记录没有商家，加这一列之前的预占也没有。
-- 和 00003 分成两个版本，是为了每个版本只有一条 DDL，中途被杀后可以重跑。
ALTER TABLE reservation
    ADD COLUMN merchant_id BIGINT NULL COMMENT '预占时订单所属的商家' AFTER status;

-- +goose Down
ALTER TABLE reservation DROP COLUMN merchant_id;
