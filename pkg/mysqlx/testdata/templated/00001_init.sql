-- +goose Up
{{range .Slots}}
CREATE TABLE item_{{.}} (
    id BIGINT NOT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB;
{{end}}

-- +goose Down
{{range .Slots}}
DROP TABLE item_{{.}};
{{end}}
