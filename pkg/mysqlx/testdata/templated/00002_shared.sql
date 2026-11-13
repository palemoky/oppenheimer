-- +goose Up
CREATE TABLE {{.Shared}} (
    id BIGINT NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB;

-- +goose Down
DROP TABLE {{.Shared}};
