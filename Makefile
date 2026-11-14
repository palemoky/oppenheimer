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
run: build ## 在本机启动全部服务（需要先 make up），Ctrl-C 停止
	scripts/run-local.sh

.PHONY: e2e
e2e: build ## 端到端测试：在独立的 e2e_* 库上启动全部服务并对网关跑测试（需要先 make up）
	scripts/e2e.sh

.PHONY: chaos
chaos: build ## 杀进程对账测试：压测期间随机 kill -9 服务，静止后对账（需要先 make up，CHAOS_* 变量见脚本）
	scripts/chaos.sh

.PHONY: bench
bench: build ## 下单吞吐压测：在独立的 bench_* 库上逐档启动服务并压测（需要先 make up，BENCH_* 变量见脚本）
	scripts/bench.sh

.PHONY: load
load: ## 压测网关（需要服务已启动），参数用 LOAD_ARGS 传，例如 make load LOAD_ARGS="-c 100 -duration 30s"
	go run ./test/load run $(LOAD_ARGS)

.PHONY: seed
seed: ## 向本地 inventory_db 写入示例 SKU（服务至少启动过一次、表已建好之后）
	@$(COMPOSE) exec -T mysql mysql -uroot -p$(MYSQL_ROOT_PASSWORD) inventory_db -e \
		"INSERT INTO sku_stock (sku_id, merchant_id, name, unit_price, available) VALUES \
		 (1001, 1, 'Mechanical Keyboard', 39900, 100), (1002, 1, 'Wireless Mouse', 12900, 500), (1003, 1, 'Limited Figure', 99900, 10), \
		 (2001, 2, 'Desk Lamp', 15900, 200) \
		 ON DUPLICATE KEY UPDATE name = VALUES(name);" 2>/dev/null
	@echo "seeded sku 1001, 1002, 1003 (merchant 1), 2001 (merchant 2)"

# 造数（设计 1c 的 5.3）。DSN 与 scripts/run-local.sh 一致；SEED_ARGS 里的同名参数会覆盖这里的默认值。
.PHONY: seed-orders
seed-orders: ## 直接向订单主库灌历史订单（order 至少启动过一次），可中断续跑，参数用 SEED_ARGS 传，例如 SEED_ARGS="-orders 100000"
	go run ./test/load seed \
		-order-dsns "root:$(MYSQL_ROOT_PASSWORD)@tcp(127.0.0.1:3311)/order_db_0,root:$(MYSQL_ROOT_PASSWORD)@tcp(127.0.0.1:3321)/order_db_1" \
		$(SEED_ARGS)

# 归档（ADR-0016）：把受理超过 -older-than 的终态订单从热库搬到冷库。DSN 的生成见 scripts/archive.sh（ORDER_DBS 同 run-local.sh）。
.PHONY: archive
archive: ## 把旧的终态订单从订单热库归档到冷库 order_archive_db，参数用 ARCHIVE_ARGS 传，例如 ARCHIVE_ARGS="-older-than 744h -rate 200"；ORDER_DBS=4 表示订单库已扩成 4 个
	scripts/archive.sh $(ARCHIVE_ARGS)

.PHONY: archive-dry-run
archive-dry-run: ## 归档的试运行：只统计会归档多少订单，不写冷库也不删热库
	scripts/archive.sh -dry-run $(ARCHIVE_ARGS)

.PHONY: archive-reclaim
archive-reclaim: ## 归档删行之后，回收热表的表空间（只做回收，不搬订单）
	scripts/archive.sh -reclaim $(ARCHIVE_ARGS)

.PHONY: seed-report
seed-report: ## 灌数之后量分片均匀度、查询延迟、偏移量对游标（只读），参数用 SEED_ARGS 传，例如 SEED_ARGS="-analyze -json tmp/seed-report.json"
	go run ./test/load seed-report \
		-order-dsns "root:$(MYSQL_ROOT_PASSWORD)@tcp(127.0.0.1:3311)/order_db_0,root:$(MYSQL_ROOT_PASSWORD)@tcp(127.0.0.1:3321)/order_db_1" \
		-merchant-index-dsns "root:$(MYSQL_ROOT_PASSWORD)@tcp(127.0.0.1:$(MYSQL_PORT))/merchant_index_db_0,root:$(MYSQL_ROOT_PASSWORD)@tcp(127.0.0.1:$(MYSQL_PORT))/merchant_index_db_1" \
		$(SEED_ARGS)

## ---------- 本地依赖 ----------

# up 和 up-core 共用：启动核心依赖并建 inventory_db、payment_db。
# order 的 4 个 MySQL 容器不在 CORE_SERVICES 里。
CORE_SERVICES := mysql redis redis-cache kafka

.PHONY: up-core
up-core: ## 只启动 MySQL、Redis（含缓存实例）、Kafka 并等待就绪，不含 order 主从；给不需要 order 主从的场景用（如按模块的测试）
	$(COMPOSE) up -d --wait $(CORE_SERVICES)
	@$(COMPOSE) exec -T mysql mysql -uroot -p$(MYSQL_ROOT_PASSWORD) -e \
		"CREATE DATABASE IF NOT EXISTS inventory_db; CREATE DATABASE IF NOT EXISTS payment_db; \
		 CREATE DATABASE IF NOT EXISTS merchant_index_db_0; CREATE DATABASE IF NOT EXISTS merchant_index_db_1; \
		 CREATE DATABASE IF NOT EXISTS order_archive_db;" 2>/dev/null
	@echo "databases ready: inventory_db payment_db merchant_index_db_0 merchant_index_db_1 order_archive_db"

.PHONY: up
up: up-core ## 启动 MySQL（含 order 两个分片的主从）、Redis（含缓存实例）、Kafka 并等待就绪
	$(COMPOSE) up -d --wait
	@# order_db_0、order_db_1 在各自的分片主库上由复制脚本创建，并确认已复制到从库。
	@MYSQL_ROOT_PASSWORD=$(MYSQL_ROOT_PASSWORD) scripts/mysql-replication.sh

.PHONY: down
down: ## 停止本地依赖（含 kafbat-ui），保留数据
	$(COMPOSE) --profile ui down

.PHONY: down-volumes
down-volumes: ## 停止本地依赖并删除全部数据（含扩容演练的 4 个实例和 kafbat-ui）
	$(COMPOSE) --profile expand --profile ui down --volumes

.PHONY: expand-up
expand-up: ## 启动扩容演练用的 4 个 MySQL 容器（profile expand）并等待就绪；之后用 scripts/expand.sh 分阶段演练
	$(COMPOSE) --profile expand up -d --wait mysql-order-2 mysql-order-2-replica mysql-order-3 mysql-order-3-replica

.PHONY: expand-down
expand-down: ## 停止并删除扩容演练的 4 个容器及其数据卷（不影响其他实例）
	$(COMPOSE) --profile expand rm -f -s -v mysql-order-2 mysql-order-2-replica mysql-order-3 mysql-order-3-replica
	@docker volume rm -f oppenheimer_mysql-order-2-data oppenheimer_mysql-order-2-replica-data oppenheimer_mysql-order-3-data oppenheimer_mysql-order-3-replica-data

KAFKA_UI_PORT ?= 8090
export KAFKA_UI_PORT

.PHONY: kafka-ui
kafka-ui: ## 启动 Kafka 的 Web 界面 kafbat-ui（profile ui，只读）并等待就绪，打印访问地址
	$(COMPOSE) --profile ui up -d --wait kafka-ui
	@echo "kafbat-ui: http://127.0.0.1:$(KAFKA_UI_PORT)"

.PHONY: kafka-ui-down
kafka-ui-down: ## 停止并删除 kafbat-ui 容器（不影响其他实例）
	$(COMPOSE) --profile ui rm -f -s -v kafka-ui

.PHONY: ps
ps: ## 查看本地依赖的状态
	$(COMPOSE) ps

.PHONY: logs
logs: ## 跟踪本地依赖的日志
	$(COMPOSE) logs -f --tail=100
