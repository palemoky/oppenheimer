-- +goose Up

-- 本地消息表，与 pkg/outbox.Schema 保持一致（改动时两处一起改）。
-- 步骤二里库存服务只消费、不发布事件，这张表暂时是空的；按“每个服务都有 outbox”
-- 的约定先建好，之后库存服务要发布事件（例如步骤三的异步预占结果）时不必再加迁移。
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

-- +goose Down
DROP TABLE outbox;
