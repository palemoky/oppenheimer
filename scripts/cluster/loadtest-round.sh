#!/usr/bin/env bash
# 一轮压测的固定流程里“发压和存档”的那一段（设计文档 3b 的 3.2 第 1、2、4、7 步）：
# 预检 → 记下这一轮的条件 → 跑 k6（scripts/cluster/k6.sh）→ 中途抓 pprof → 把这一轮服务端的数从 Prometheus 取出来存档。
# 停压之后的等静止、对账（loadtest.sh wait-quiet、recon、verify）不在这里：几轮可以连着跑，最后一起等、一起对。
#
#   loadtest-round.sh <场景> [k6 的额外参数]
#
# k6 的配置和 scripts/cluster/k6.sh 一样走环境变量（RUN_ID、SKUS、RATE、DURATION、STAGES……）。RUN_ID 必须给。
# 结果都在 tmp/k6/<RUN_ID>/：k6 的那几个文件之外，多出
#   round-conditions.txt   loadtest.sh conditions 的输出（提交号、镜像、副本数、限流、采样、各表行数）
#   server.txt             这一轮的服务端指标（下面 snapshot 里的查询，窗口是 k6 的运行时长）
#   pprof/                 PPROF_AT 给了才有：开跑后第 PPROF_AT 秒，对 PPROF_SERVICES 里每个服务的一个 Pod 抓 30 秒 CPU、堆、goroutine
#
# 环境变量：
#   ROUND_SKIP_PREFLIGHT=1  跳过预检（紧接着上一轮跑、延迟关单的消息还没走完时用；对账仍然要等）
#   PPROF_AT                开跑后多少秒抓 pprof，不给就不抓
#   PPROF_SERVICES          抓哪些服务，默认 "gateway order inventory"
#   EXPECT_*                原样传给 loadtest.sh preflight
#
# 在 macOS 自带的 bash 3.2 上运行。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
ROOT="$(cd "$HERE/../.." && pwd)"
export KUBECONFIG="$KUBECONFIG_OUT"
NS="oppenheimer"

if [[ $# -lt 1 || ( "$1" != snapshot && -z "${RUN_ID:-}" ) ]]; then
  sed -n '2,20p' "${BASH_SOURCE[0]}" >&2
  exit 2
fi
OUT="$ROOT/tmp/k6/${RUN_ID:-}"

pq() { # 查询 → 每条序列一行：标签（去掉名字）<TAB>值
  "$HERE/monitoring.sh" promql "$1" 2>/dev/null |
    jq -r '.data.result[] | [(.metric | del(.__name__) | to_entries | map(.key + "=" + .value) | join(",")), (.value[1] | tonumber | . * 10000 | round / 10000)] | @tsv' |
    sort || true
}

snapshot() { # 窗口秒数 [往回偏移的秒数，默认 35]
  # R 是带偏移的范围：取数时已经停压 35 秒，往回偏移掉这一段，窗口正好落在发压期间（开头的 30 秒不算）
  local w="${1}s" off="${2:-35}s"
  local R="[${1}s] offset $off" SR="[${1}s:15s] offset $off" ns='k8s_namespace_name="oppenheimer"'
  local q
  section() {
    echo
    echo "## $1"
    echo "# $2"
    pq "$2"
  }
  echo "window: $w ending $off before $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  section "gateway 请求速率（每秒），按接口和状态码" \
    "sum by (http_route,http_request_method,http_response_status_code) (rate(http_server_request_duration_seconds_count{$ns,service_name=\"gateway\"}$R)) > 0"
  for q in 0.5 0.99; do
    section "gateway 耗时 p${q}（秒），按接口" \
      "histogram_quantile($q, sum by (le,http_route,http_request_method) (rate(http_server_request_duration_seconds_bucket{$ns,service_name=\"gateway\"}$R)))"
  done
  section "gRPC 服务端速率（每秒），按服务、方法、状态" \
    "sum by (service_name,rpc_method,rpc_response_status_code) (rate(rpc_server_call_duration_seconds_count{$ns}$R)) > 0"
  section "gRPC 服务端耗时 p99（秒），按服务、方法" \
    "histogram_quantile(0.99, sum by (le,service_name,rpc_method) (rate(rpc_server_call_duration_seconds_bucket{$ns}$R)))"
  section "gRPC 客户端耗时 p99（秒，含边车和网络），按调用方、方法" \
    "histogram_quantile(0.99, sum by (le,service_name,rpc_method) (rate(rpc_client_call_duration_seconds_bucket{$ns}$R)))"
  section "受理的结果（每秒）" "sum by (outcome) (rate(order_admissions_total{$ns}$R)) > 0"
  section "落库的结果（每秒）" "sum by (outcome) (rate(order_persists_total{$ns}$R)) > 0"
  for q in 0.5 0.99; do
    section "落库延迟 p${q}（秒，受理到落库）" \
      "histogram_quantile($q, sum by (le) (rate(order_persist_delay_seconds_bucket{$ns}$R)))"
  done
  section "订单读（每秒），按方式和来源" "sum by (mode,source,partial) (rate(order_reads_total{$ns}$R)) > 0"
  section "限流的判定（每秒）" "sum by (rule,outcome) (rate(gateway_ratelimit_decisions_total{$ns}$R)) > 0"
  section "舱壁拒绝（每秒）" "sum by (name) (rate(order_bulkhead_rejected_total{$ns}$R)) > 0"
  section "缓存（每秒）" "sum by (service_name,cache,result) (rate(cache_requests_total{$ns}$R)) > 0"
  section "数据库和 Redis 的调用速率（每秒），按服务、系统、操作" \
    "sum by (service_name,db_system_name,db_operation_name) (rate(db_client_operation_duration_seconds_count{$ns}$R)) > 1"
  section "数据库和 Redis 的调用耗时 p99（秒），按服务、系统、操作" \
    "histogram_quantile(0.99, sum by (le,service_name,db_system_name,db_operation_name) (rate(db_client_operation_duration_seconds_bucket{$ns}$R))) > 0.001"
  section "数据库和 Redis 的调用出错（每秒）" \
    "sum by (service_name,db_system_name,db_operation_name,error_type) (rate(db_client_operation_duration_seconds_count{$ns,error_type!=\"\"}$R)) > 0"
  section "MySQL 连接池等待（次/秒）" "sum by (service_name,db_namespace) (rate(db_sql_connection_wait_total{$ns}$R)) > 0"
  section "消费者处理速率（每秒），按组、topic、结果" \
    "sum by (messaging_consumer_group_name,messaging_destination_name,outcome) (rate(kafkax_consumer_process_duration_seconds_count{$ns}$R)) > 0"
  section "消费延迟的最大值（条），按组" "max_over_time(sum by (consumergroup) (kafka_consumergroup_lag)$SR) > 0"
  section "CPU（核），业务服务按 Pod 和容器" \
    "sum by (pod,container) (rate(container_cpu_usage_seconds_total{namespace=\"oppenheimer\",container!=\"\",container!=\"POD\"}$R)) > 0.005"
  section "CPU 被节流的时间片比例，业务服务" \
    "sum by (pod,container) (rate(container_cpu_cfs_throttled_periods_total{namespace=\"oppenheimer\",container!=\"\"}$R)) / sum by (pod,container) (rate(container_cpu_cfs_periods_total{namespace=\"oppenheimer\",container!=\"\"}$R)) > 0.01"
  section "CPU（核），中间件、入口、监控按 Pod" \
    "sum by (namespace,pod) (rate(container_cpu_usage_seconds_total{namespace=~\"mysql|redis|kafka|envoy-gateway-system|monitoring|istio-system\",container!=\"\",container!=\"POD\"}$R)) > 0.05"
  section "内存工作集的峰值（Mi），业务服务和 MySQL" \
    "max by (namespace,pod) (max_over_time(container_memory_working_set_bytes{namespace=~\"oppenheimer|mysql\",container!=\"\",container!=\"POD\"}$R)) / 1048576"
  section "节点 CPU 使用率" "1 - avg by (instance) (rate(node_cpu_seconds_total{mode=\"idle\"}$R))"
  section "节点磁盘 IO 等待（iowait 占比）" "avg by (instance) (rate(node_cpu_seconds_total{mode=\"iowait\"}$R))"
  section "从库延迟的最大值（秒）" "max_over_time(mysql_slave_status_seconds_behind_source$R)"
  section "etcd WAL fsync p99（秒）" "histogram_quantile(0.99, sum by (le) (rate(etcd_disk_wal_fsync_duration_seconds_bucket$R)))"
  section "副本数（窗口内的最大值）" "max by (deployment) (max_over_time(kube_deployment_status_replicas{namespace=\"oppenheimer\"}$R))"
  section "容器重启（窗口内）" "sum by (namespace,pod) (increase(kube_pod_container_status_restarts_total$R)) > 0"
  section "Collector agent 的发送队列（窗口内的最大值）" "max by (pod) (max_over_time(otelcol_exporter_queue_size{pod=~\"otel-agent.*\"}$R))"
}

grab_pprof() { # 抓一个服务的一个 Pod：pprof 只在 Pod 的回环上监听（设计文档 3a），用 port-forward
  local svc="$1" pod port pf
  pod="$(kubectl -n "$NS" get pods -l "app.kubernetes.io/name=$svc" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  [[ -n "$pod" ]] || return 0
  port=$((16060 + RANDOM % 1000))
  kubectl -n "$NS" port-forward "$pod" "$port:${PPROF_PORT:-6060}" >/dev/null 2>&1 &
  pf=$!
  sleep 2
  curl -fsS -m 45 -o "$OUT/pprof/$svc.cpu.pb.gz" "http://127.0.0.1:$port/debug/pprof/profile?seconds=30" || echo "pprof cpu $svc failed" >&2
  curl -fsS -m 15 -o "$OUT/pprof/$svc.heap.pb.gz" "http://127.0.0.1:$port/debug/pprof/heap" || true
  curl -fsS -m 15 -o "$OUT/pprof/$svc.goroutine.txt" "http://127.0.0.1:$port/debug/pprof/goroutine?debug=1" || true
  kill "$pf" 2>/dev/null || true
  echo "$svc $pod" >>"$OUT/pprof/pods.txt"
}

# 补取：loadtest-round.sh snapshot <窗口秒数> <往回偏移的秒数>，事后按时间把某一轮的服务端指标重新取一遍（标准输出）
if [[ "$1" == snapshot ]]; then
  snapshot "${2:?window seconds}" "${3:?offset seconds}"
  exit 0
fi

if [[ "${ROUND_SKIP_PREFLIGHT:-}" != 1 ]]; then
  "$HERE/loadtest.sh" preflight
fi
COND="$(mktemp)"
"$HERE/loadtest.sh" conditions >"$COND"

START=$(date +%s)
PPROF_PID=""
if [[ -n "${PPROF_AT:-}" ]]; then
  (
    sleep "$PPROF_AT"
    mkdir -p "$OUT/pprof"
    for s in ${PPROF_SERVICES:-gateway order inventory}; do
      grab_pprof "$s" &
    done
    wait
  ) &
  PPROF_PID=$!
fi

set +e
"$HERE/k6.sh" "$@"
RC=$?
set -e
END=$(date +%s)
if [[ -n "$PPROF_PID" ]]; then
  wait "$PPROF_PID" 2>/dev/null || true
fi

mkdir -p "$OUT"
mv "$COND" "$OUT/round-conditions.txt"
# 指标每 15 秒导出一次，Prometheus 的抓取是 30 秒：等一会儿再取，窗口多算这一段
sleep 35
W=$((END - START - 30))
[[ $W -ge 30 ]] || W=30
snapshot "$W" >"$OUT/server.txt"
echo "server metrics: $OUT/server.txt"
exit "$RC"
