SHELL := /bin/bash
.DEFAULT_GOAL := help

ROOT := $(CURDIR)
BIN  := $(ROOT)/bin
export PATH := $(BIN):$(PATH)

# 工具版本登记在 docs/infra/versions.md，升级时两处一起改。
BUF_VERSION                := v1.75.0
PROTOC_GEN_GO_VERSION      := v1.36.12
PROTOC_GEN_GO_GRPC_VERSION := v1.6.2
OAPI_CODEGEN_VERSION       := v2.8.2

# go.work 里的所有模块目录
MODULES  := $(shell go list -m -f '{{.Dir}}')
SERVICES := $(notdir $(wildcard services/*))
COMPOSE  := docker compose -f deploy/compose/docker-compose.yml

# 集成测试连接的 MySQL，默认是 make up 启动的那个。每个测试会自建并删除临时库。
MYSQL_ROOT_PASSWORD ?= oppenheimer
MYSQL_PORT          ?= 3306
export OPPENHEIMER_TEST_MYSQL_DSN ?= root:$(MYSQL_ROOT_PASSWORD)@tcp(127.0.0.1:$(MYSQL_PORT))/

# 集成测试连接的 Redis 和 Kafka，同样是 make up 启动的。测试各自用随机前缀和临时 topic 隔离。
REDIS_PORT ?= 6379
KAFKA_PORT ?= 9092
export OPPENHEIMER_TEST_REDIS_ADDR    ?= 127.0.0.1:$(REDIS_PORT)
export OPPENHEIMER_TEST_KAFKA_BROKERS ?= 127.0.0.1:$(KAFKA_PORT)

.PHONY: help
help: ## 显示可用目标
	@grep -hE '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

## ---------- 工具 ----------

.PHONY: tools
tools: proto-plugins openapi-tools ## 安装 buf、proto 插件和 oapi-codegen 到 ./bin
	GOWORK=off GOBIN=$(BIN) go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

.PHONY: openapi-tools
openapi-tools: ## 只安装 oapi-codegen
	GOWORK=off GOBIN=$(BIN) go install github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION)

.PHONY: proto-plugins
proto-plugins: ## 只安装 proto 的 Go 插件（CI 里 buf 由 action 提供）
	GOWORK=off GOBIN=$(BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOWORK=off GOBIN=$(BIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

## ---------- 代码 ----------

.PHONY: proto
proto: ## 由 api/proto 生成 Go 代码到 api/gen
	cd api && buf generate

.PHONY: openapi
openapi: ## 由 api/openapi/gateway.yaml 生成 gateway 的类型和服务端接口
	$(BIN)/oapi-codegen -config api/openapi/oapi-codegen.yaml api/openapi/gateway.yaml

.PHONY: proto-lint
proto-lint: ## 检查 proto 的格式与规范
	cd api && buf format --diff --exit-code && buf lint

.PHONY: fmt
fmt: ## 格式化 Go 和 proto
	@for m in $(MODULES); do (cd $$m && golangci-lint fmt) || exit 1; done
	cd api && buf format -w

.PHONY: lint
lint: ## 对所有模块运行 golangci-lint
	@for m in $(MODULES); do echo "==> lint $${m#$(ROOT)/}"; (cd $$m && golangci-lint run ./...) || exit 1; done

.PHONY: test
test: ## 运行全部测试（带竞态检测）。集成测试需要先 make up
	@for m in $(MODULES); do echo "==> test $${m#$(ROOT)/}"; (cd $$m && go test -race ./...) || exit 1; done

.PHONY: test-unit
test-unit: ## 只运行不依赖 MySQL、Redis、Kafka 的测试
	@for m in $(MODULES); do echo "==> test $${m#$(ROOT)/}"; (cd $$m && OPPENHEIMER_TEST_MYSQL_DSN= OPPENHEIMER_TEST_REDIS_ADDR= OPPENHEIMER_TEST_KAFKA_BROKERS= go test -race ./...) || exit 1; done

.PHONY: build
build: ## 编译所有模块，并把各服务的二进制输出到 ./bin
	@for m in $(MODULES); do (cd $$m && go build -o /dev/null ./...) || exit 1; done
	go build -o $(BIN)/ $(foreach s,$(SERVICES),./services/$(s)/cmd/...)

.PHONY: tidy
tidy: ## 对所有模块执行 go mod tidy，并同步 workspace
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done
	go work sync

## ---------- 运行 ----------

.PHONY: run
run: build ## 在本机启动四个服务（需要先 make up），Ctrl-C 停止
	scripts/run-local.sh

.PHONY: e2e
e2e: build ## 端到端测试：在独立的 e2e_* 库上启动四个服务并对网关跑测试（需要先 make up）
	scripts/e2e.sh

.PHONY: chaos
chaos: build ## 杀进程对账测试：压测期间随机 kill -9 服务，静止后对账（需要先 make up，CHAOS_* 变量见脚本）
	scripts/chaos.sh

.PHONY: load
load: ## 压测网关（需要服务已启动），参数用 LOAD_ARGS 传，例如 make load LOAD_ARGS="-c 100 -duration 30s"
	go run ./test/load run $(LOAD_ARGS)

.PHONY: seed
seed: ## 向本地 inventory_db 写入示例 SKU（服务至少启动过一次、表已建好之后）
	@$(COMPOSE) exec -T mysql mysql -uroot -p$(MYSQL_ROOT_PASSWORD) inventory_db -e \
		"INSERT INTO sku_stock (sku_id, name, unit_price, available) VALUES \
		 (1001, 'Mechanical Keyboard', 39900, 100), (1002, 'Wireless Mouse', 12900, 500), (1003, 'Limited Figure', 99900, 10) \
		 ON DUPLICATE KEY UPDATE name = VALUES(name);" 2>/dev/null
	@echo "seeded sku 1001, 1002, 1003"

## ---------- 本地依赖 ----------

.PHONY: up
up: ## 启动 MySQL、Redis（含缓存实例）、Kafka 并等待就绪
	$(COMPOSE) up -d --wait
	@$(COMPOSE) exec -T mysql mysql -uroot -p$(MYSQL_ROOT_PASSWORD) -e \
		"CREATE DATABASE IF NOT EXISTS inventory_db; CREATE DATABASE IF NOT EXISTS order_db; CREATE DATABASE IF NOT EXISTS payment_db;" 2>/dev/null
	@echo "databases ready: inventory_db order_db payment_db"

.PHONY: down
down: ## 停止本地依赖，保留数据
	$(COMPOSE) down

.PHONY: down-volumes
down-volumes: ## 停止本地依赖并删除全部数据
	$(COMPOSE) down --volumes

.PHONY: ps
ps: ## 查看本地依赖的状态
	$(COMPOSE) ps

.PHONY: logs
logs: ## 跟踪本地依赖的日志
	$(COMPOSE) logs -f --tail=100
