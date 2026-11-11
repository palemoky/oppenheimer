-- +goose Up
CREATE TABLE counter (
    id    BIGINT NOT NULL,
    value BIGINT NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT chk_counter_value CHECK (value >= 0)
) ENGINE = InnoDB;

-- +goose Down
DROP TABLE counter;
