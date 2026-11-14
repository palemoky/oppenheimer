# shellcheck shell=bash
# 被 run-local.sh、chaos.sh、bench.sh、fault.sh source，不单独执行。
#
# 服务之间的 gRPC 调用经过各自的 Envoy 边车（ADR-0020，配置在 deploy/envoy/，make up 启动）：
#   调用方进程 -> 调用方的边车（出站端口）-> 被调用方的边车（入站 15001）-> 被调用方进程
# SIDECAR=on（默认）时下游地址指向调用方自己边车的出站端口；SIDECAR=off 时直连，不经过边车。
# 端口和 deploy/envoy/<服务>.yaml 里的监听一致，改端口时只改这里。
#
# 调用 sidecar_setup 之后可用：
#   SC_ORDER_INVENTORY   order 进程的 INVENTORY_ADDR
#   SC_PAYMENT_ORDER     payment 进程的 ORDER_ADDR
#   SC_GATEWAY_ADDRS     gateway 进程的四个 *_ADDR（KEY=VALUE，空格分隔，用时不要加引号）
# 同一个变量名（比如 INVENTORY_ADDR）在不同服务上指向不同的端口：各自的边车各有一套统计和额度。

SIDECAR_CONTAINERS="gateway order payment inventory orderindex"

# sidecar_check 确认五个边车容器都在运行且健康；没起来时退出，免得服务启动后调用全部失败。
sidecar_check() {
  local c name status
  for c in $SIDECAR_CONTAINERS; do
    name="${COMPOSE_PROJECT_NAME:-oppenheimer}-envoy-$c-1"
    status="$(docker inspect -f '{{.State.Health.Status}}' "$name" 2>/dev/null)" || status=""
    [ -n "$status" ] || status="not running"
    if [ "$status" != healthy ]; then
      echo "Envoy sidecar $name is $status; run 'make up' first (or SIDECAR=off to call services directly)" >&2
      exit 2
    fi
  done
}

sidecar_setup() {
  case "${SIDECAR:-on}" in
  on)
    sidecar_check
    SC_ORDER_INVENTORY=127.0.0.1:15182
    SC_PAYMENT_ORDER=127.0.0.1:15281
    SC_GATEWAY_ADDRS="ORDER_ADDR=127.0.0.1:15081 INVENTORY_ADDR=127.0.0.1:15082 PAYMENT_ADDR=127.0.0.1:15083 ORDERINDEX_ADDR=127.0.0.1:15084"
    ;;
  off)
    SC_ORDER_INVENTORY=127.0.0.1:9082
    SC_PAYMENT_ORDER=127.0.0.1:9081
    SC_GATEWAY_ADDRS="ORDER_ADDR=127.0.0.1:9081 INVENTORY_ADDR=127.0.0.1:9082 PAYMENT_ADDR=127.0.0.1:9083 ORDERINDEX_ADDR=127.0.0.1:9084"
    ;;
  *)
    echo "SIDECAR must be on or off, got ${SIDECAR}" >&2
    exit 2
    ;;
  esac
}
