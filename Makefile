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
# promtool 跟集群上的 Prometheus 走（scripts/cluster/cluster.env 的 PROMETHEUS_VERSION）
PROMTOOL_VERSION           := 3.13.5
YQ_VERSION                 := v4.54.3
export PROMTOOL_VERSION YQ_VERSION

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

# Redis 的 Sentinel 和 Cluster 两种模式（profile redis-modes，make redis-modes-up 启动）。可选：
# 这两个地址连不上时对应的测试跳过。地址是 compose 里给容器固定的 IP，OrbStack 下宿主机可以直接访问。
export OPPENHEIMER_TEST_REDIS_SENTINEL_ADDR ?= sentinel://gate@172.30.0.21:26379,172.30.0.22:26379,172.30.0.23:26379
export OPPENHEIMER_TEST_REDIS_CLUSTER_ADDR  ?= cluster://172.30.0.31:6379,172.30.0.32:6379,172.30.0.33:6379

.PHONY: help
help: ## 显示可用目标
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

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

.PHONY: promtool
promtool: ## 只安装 promtool（版本同集群的 Prometheus，校验 sha256）到 ./bin
	scripts/install-bin.sh promtool

.PHONY: yq
yq: ## 只安装 yq（从规则清单里抽出 .spec.groups 用，校验 sha256）到 ./bin
	scripts/install-bin.sh yq

## ---------- 代码 ----------

.PHONY: proto
proto: ## 由 api/proto 生成 Go 代码到 api/gen
	cd api && buf generate

.PHONY: openapi
openapi: ## 由 api/openapi/gateway.yaml 生成 gateway 的类型和服务端接口
	$(BIN)/oapi-codegen -config api/openapi/oapi-codegen.yaml api/openapi/gateway.yaml

.PHONY: rules-test
rules-test: promtool yq ## 检查并单元测试 Prometheus 的规则（deploy/cluster/monitoring/rules/，promtool check rules + test rules）
	scripts/rules-test.sh

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
	@for m in $(MODULES); do echo "==> test $${m#$(ROOT)/}"; (cd $$m && OPPENHEIMER_TEST_MYSQL_DSN= OPPENHEIMER_TEST_REDIS_ADDR= OPPENHEIMER_TEST_REDIS_SENTINEL_ADDR= OPPENHEIMER_TEST_REDIS_CLUSTER_ADDR= OPPENHEIMER_TEST_KAFKA_BROKERS= go test -race ./...) || exit 1; done

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

.PHONY: fault
fault: build ## 故障注入：挂住、杀掉 inventory，并发打满，验证边车的熔断和并发隔离（需要先 make up，变量见 scripts/fault.sh）
	scripts/fault.sh

.PHONY: bench
bench: build ## 下单吞吐压测：在独立的 bench_* 库上逐档启动服务并压测（需要先 make up，BENCH_* 变量见脚本；BENCH_OPEN=1 加开环档位，BENCH_POLL=1 加轮询压力对比）
	scripts/bench.sh

.PHONY: load
load: ## 压测网关（需要服务已启动），参数用 LOAD_ARGS 传，例如 make load LOAD_ARGS="-c 100 -duration 30s"（开环：-mode open -rate 1000 -duration 30s）
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

# 对账（ADR-0022）：比对主从、订单对商家索引、订单对支付单、订单对预占。DSN 的生成见 scripts/recon.sh（ORDER_DBS 同 run-local.sh）。
.PHONY: recon
recon: ## 对账：在系统运行中比对几份应当一致的数据并定位到行，参数用 RECON_ARGS 传，例如 RECON_ARGS="-pairs index -since 26h"（只比最近 26 小时，不带 -since 是全量）；ORDER_DBS=4 表示订单库已扩成 4 个
	scripts/recon.sh $(RECON_ARGS)

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
up: up-core ## 启动 MySQL（含 order 两个分片的主从）、Redis（含缓存实例）、Kafka、五个 Envoy 边车并等待就绪
	$(COMPOSE) up -d --wait
	@# order_db_0、order_db_1 在各自的分片主库上由复制脚本创建，并确认已复制到从库。
	@MYSQL_ROOT_PASSWORD=$(MYSQL_ROOT_PASSWORD) scripts/mysql-replication.sh

.PHONY: down
down: ## 停止本地依赖（含 kafbat-ui、otel-lgtm），保留数据
	$(COMPOSE) --profile ui --profile obs down

.PHONY: down-volumes
down-volumes: ## 停止本地依赖并删除全部数据（含扩容演练的 4 个实例、kafbat-ui 和 otel-lgtm）
	$(COMPOSE) --profile expand --profile ui --profile obs down --volumes

REDIS_MODES_SERVICES := redis-s-master redis-s-replica redis-sentinel-1 redis-sentinel-2 redis-sentinel-3 \
	redis-c1 redis-c2 redis-c3 redis-c4 redis-c5 redis-c6

.PHONY: redis-modes-up
redis-modes-up: ## 启动 Redis 的 Sentinel（1 主 1 从 3 哨兵）和 Cluster（3 主 3 从）测试环境（profile redis-modes）并等待就绪，打印两个地址
	$(COMPOSE) --profile redis-modes up -d --wait $(REDIS_MODES_SERVICES)
	@scripts/redis-modes-wait.sh
	@echo "OPPENHEIMER_TEST_REDIS_SENTINEL_ADDR=$(OPPENHEIMER_TEST_REDIS_SENTINEL_ADDR)"
	@echo "OPPENHEIMER_TEST_REDIS_CLUSTER_ADDR=$(OPPENHEIMER_TEST_REDIS_CLUSTER_ADDR)"

.PHONY: redis-modes-down
redis-modes-down: ## 删除 Redis 的 Sentinel 和 Cluster 测试容器（不持久化，没有数据要保留）
	$(COMPOSE) --profile redis-modes rm -sf $(REDIS_MODES_SERVICES)

.PHONY: envoy-validate
envoy-validate: ## 用 Envoy 自己的校验模式检查 deploy/envoy/ 下每个边车的配置（需要 docker，不需要 make up）
	@for f in deploy/envoy/*.yaml; do \
		out=$$(docker run --rm -v "$(ROOT)/$$f:/etc/envoy/envoy.yaml:ro" envoyproxy/envoy:v1.40.2 \
			--mode validate -c /etc/envoy/envoy.yaml 2>&1); code=$$?; \
		if [ $$code = 0 ] && printf '%s\n' "$$out" | grep -q "' OK$$"; then \
			printf '%s: %s\n' "$$f" "$$(printf '%s\n' "$$out" | grep "' OK$$")"; \
		else \
			printf '%s: validation failed (docker exit %s), full output:\n%s\n' "$$f" "$$code" "$$out"; exit 1; \
		fi; \
	done

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

GRAFANA_PORT ?= 3000
export GRAFANA_PORT

.PHONY: obs-up
obs-up: ## 启动本机可观测后端 otel-lgtm（profile obs，Grafana+Tempo+Prometheus+Loki）并等待就绪，打印地址；之后 make run 会自动导出（OBS=off 关闭）
	$(COMPOSE) --profile obs up -d --wait otel-lgtm
	@echo "Grafana: http://127.0.0.1:$(GRAFANA_PORT)  (login: admin / admin, image default)"
	@echo "OTLP gRPC: 127.0.0.1:4317, OTLP HTTP: 127.0.0.1:4318"

.PHONY: obs-down
obs-down: ## 停止并删除 otel-lgtm 容器（数据不持久化，一并丢失；不影响其他实例）
	$(COMPOSE) --profile obs rm -f -s -v otel-lgtm

.PHONY: ps
ps: ## 查看本地依赖的状态
	$(COMPOSE) ps

.PHONY: logs
logs: ## 跟踪本地依赖的日志
	$(COMPOSE) logs -f --tail=100

# 集群（阶段 2，设计文档 docs/design/2a-cluster.md）。地址和版本在 scripts/cluster/cluster.env。
# 删除虚拟机不做成目标：要显式运行 scripts/cluster/on-host.sh vm.sh destroy <名字>。
.PHONY: cluster-up
cluster-up: ## 从空的宿主机建出可用的集群：建虚拟机、装 k3s、取 kubeconfig、装 MetalLB、仓库、Envoy Gateway；每一步可重复执行
	scripts/cluster/on-host.sh vm.sh create
	scripts/cluster/k3s.sh prepare
	scripts/cluster/k3s.sh install
	scripts/cluster/k3s.sh kubeconfig
	scripts/cluster/addons.sh all
	scripts/cluster/k3s.sh etcd-backup install

.PHONY: middleware-up
middleware-up: ## 在集群上部署中间件：Redis（主从加 Sentinel、Cluster）、Kafka（Strimzi）、MySQL（两个分片加共享实例）；可重复执行
	scripts/cluster/middleware.sh all

.PHONY: mesh-up
mesh-up: ## 装 Istio 的控制面 istiod 和全网格的指标配置（Telemetry）；要在 make middleware-up 之后、make deploy 之前；可重复执行
	scripts/cluster/mesh.sh up

.PHONY: mesh-status
mesh-status: ## 查看 Istio 控制面的状态（Pod、Helm release、istioctl 版本）
	scripts/cluster/mesh.sh status

.PHONY: mesh-check
mesh-check: ## 检查边车注入在这批节点上能用：临时起一对 Pod，确认请求经过两边的边车并且是 mTLS，跑完清掉
	scripts/cluster/mesh.sh check

.PHONY: monitoring-up
monitoring-up: ## 装可观测的后端到命名空间 monitoring（kube-prometheus-stack、Loki、Tempo、OpenTelemetry Collector、MySQL 和 Redis 的 exporter、Istio 边车和 istiod 的抓取对象），再装 KEDA 到命名空间 keda；要在 make middleware-up 之后、make deploy 之前（exporter 要连 MySQL，gateway 和 order 的 chart 里有 KEDA 的 ScaledObject）；可重复执行
	scripts/cluster/monitoring.sh up

.PHONY: monitoring-status
monitoring-status: ## 看可观测后端的状态：Pod 在哪个节点、卷、不健康的抓取目标、连不上的数据库（mysql_up、redis_up 为 0）
	scripts/cluster/monitoring.sh status

.PHONY: images
images: ## 构建七个业务服务的 linux/amd64 镜像并推送到集群仓库，tag 是提交号（有未提交改动时带 -dirty-时间）；make images IMAGE_SERVICES="order gateway" 只做部分
	scripts/cluster/images.sh all $(IMAGE_SERVICES)

.PHONY: deploy
deploy: ## 构建并推送镜像，再用同一个 tag 把六个业务服务部署到集群（chart 在 deploy/charts/service）；服务之间走 Istio 的边车，要先 make mesh-up；gateway、order 按请求速率伸缩，要先 make monitoring-up（装 KEDA）；make deploy IMAGE_SERVICES="order gateway" 只做部分，可重复执行
	@tag="$$(scripts/cluster/images.sh tag)" && \
	IMAGE_TAG="$$tag" scripts/cluster/images.sh all $(IMAGE_SERVICES) && \
	IMAGE_TAG="$$tag" scripts/cluster/services.sh deploy $(IMAGE_SERVICES)

.PHONY: recon-cron
recon-cron: ## 装或更新对账的两个 CronJob（recon-hourly 比最近 2 小时、recon-daily 全量，命名空间 oppenheimer，镜像 tag 取现在部署的 order，RECON_IMAGE_TAG 可覆盖）；正式压测前后用 scripts/cluster/services.sh recon-cron-suspend、recon-cron-resume；可重复执行
	scripts/cluster/services.sh recon-cron

.PHONY: probe
probe: ## 装或更新常驻探针（Deployment probe，命名空间 oppenheimer）：从入口走下单、落库、支付、已支付，用 OTLP 报指标；PROBE_SKUS（默认 9001-9010，要先 scripts/cluster/loadtest.sh skus 9001 10 建好）、PROBE_IMAGE_TAG（默认取 order 的 tag，仓库里没有 chaos 镜像会构建推送）；压测前后用 scripts/cluster/services.sh probe-stop、probe-start；可重复执行
	scripts/cluster/services.sh probe

.PHONY: recon-totals
recon-totals: ## 装或更新钱的笔数和金额核对（Deployment recon-totals，命名空间 oppenheimer）：每 30 秒核对一次最近 30 分钟的支付，结果是指标 recon.totals.*；镜像 tag 按当前工作区算，没有就构建；可重复执行
	scripts/cluster/services.sh recon-totals

.PHONY: cluster-e2e
cluster-e2e: ## 在集群上跑端到端测试：构建推送镜像、建 e2e_* 库、在 oppenheimer-e2e 部署一套、以 Job 运行测试，跑完删掉；E2E_ARGS="-run TestX" 传测试参数，E2E_KEEP=1 保留现场
	scripts/cluster/e2e.sh $(E2E_ARGS)

.PHONY: cluster-chaos
cluster-chaos: ## 在集群上做删 Pod 对账测试：和 cluster-e2e 共用 oppenheimer-e2e，压测期间随机删业务 Pod，恢复、静止后 verify 和 recon；CHAOS_* 见脚本，E2E_KEEP=1 保留现场
	scripts/cluster/chaos.sh

.PHONY: cluster-fault
cluster-fault: ## 在集群上做故障注入，验证 Istio 边车的熔断、并发上限、超时和不重试；和 cluster-e2e 共用 oppenheimer-e2e；FAULT_SCENARIOS、E2E_KEEP 见脚本
	scripts/cluster/fault.sh

.PHONY: cluster-bench
cluster-bench: ## 在集群上重做 1a 与 1b 的下单吞吐对比：先 1a（732f9ab，各 1 副本）后 1b（设计副本数），共用 oppenheimer-e2e；BENCH_* 见脚本，结果在 tmp/cluster-bench/，E2E_KEEP=1 保留现场
	scripts/cluster/bench.sh

.PHONY: k6
k6: ## 从本机发起，在发压机（Mac mini）上跑 k6 压测集群：make k6 SCENARIO=order|flash|mixed|ratelimit，配置走环境变量（见 test/k6/README.md），K6_PROM=off 不写 Prometheus；结果在 tmp/k6/<RUN_ID>/
	scripts/cluster/k6.sh $(SCENARIO) $(K6_ARGS)

.PHONY: loadtest
loadtest: ## 压测的辅助动作（预检、记录条件、准备 SKU、钉副本数、灌历史订单、wait-quiet、recon、verify）：make loadtest ARGS="preflight"，子命令见 scripts/cluster/loadtest.sh 的头部
	scripts/cluster/loadtest.sh $(ARGS)

.PHONY: deploy-status
deploy-status: ## 看业务服务在集群里的状态：Pod、Service、HTTPRoute
	scripts/cluster/services.sh status

.PHONY: deploy-seed
deploy-seed: ## 给集群上的 inventory 写入示例 SKU（1001 到 1003、2001，和探针用的 3001）；要在 make deploy 之后，可重复执行
	scripts/cluster/seed.sh

.PHONY: deploy-degrade
deploy-degrade: ## 看或改集群上一个服务的降级开关（逐个 Pod）：make deploy-degrade ARGS="gateway" 或 ARGS="gateway stock_query off|auto"
	scripts/cluster/degrade.sh $(ARGS)

.PHONY: etcd-backup
etcd-backup: ## 立刻拍一份 etcd 快照并复制到 NAS（平时由 server 节点上的定时器每 12 小时做一次）；scripts/cluster/k3s.sh etcd-backup list 查看
	scripts/cluster/k3s.sh etcd-backup run

.PHONY: middleware-backup
middleware-backup: ## 立刻手工跑一次 MySQL 备份并等它完成（三个实例；只备一个：make middleware-backup RELEASE=mysql-main）。列出和钉住备份：scripts/cluster/middleware.sh mysql-backup-list、mysql-backup-pin <名字>
	scripts/cluster/middleware.sh mysql-backup $(RELEASE)

.PHONY: mysql-restore-drill
mysql-restore-drill: ## 恢复演练：把某个实例的最新备份灌进临时 Pod，和线上从库逐表比对（make mysql-restore-drill RELEASE=mysql-main；BACKUP=keep-seed-10m 指定某一份）
	@test -n "$(RELEASE)" || { echo "usage: make mysql-restore-drill RELEASE=mysql-order-0|mysql-order-1|mysql-main" >&2; exit 2; }
	scripts/cluster/mysql-restore-drill.sh $(RELEASE)

.PHONY: cluster-status
cluster-status: ## 查看各节点的 k3s 状态和集群里的 Pod
	scripts/cluster/k3s.sh status

.PHONY: cluster-smoke
cluster-smoke: ## 入口的通路检查：经 MetalLB 和 Envoy Gateway 访问一个临时的回显服务
	scripts/cluster/addons.sh smoke
