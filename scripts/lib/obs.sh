# shellcheck shell=bash
# 被 run-local.sh、bench.sh source，不单独执行（e2e.sh、chaos.sh、fault.sh 不接可观测后端，见各自文件里的说明）。
#
# 本机的可观测后端是 compose 里的 otel-lgtm 容器（profile obs，ADR-0023，make obs-up 启动）。
# 服务进程只认标准的 OTel 环境变量（pkg/otelx）：OTEL_EXPORTER_OTLP_ENDPOINT 没设就不导出，设了就把链路和指标推过去。
# 这个库根据 OBS 决定要不要设它：
#   OBS=auto（默认）  otel-lgtm 容器在运行且健康就导出，否则什么也不设
#   OBS=on            容器没在运行或不健康就报错退出，提示先 make obs-up
#   OBS=off           不导出：unset OTEL_EXPORTER_OTLP_ENDPOINT（压测的对照组、不想要后端时用）
# 调用方的环境里已经有 OTEL_EXPORTER_OTLP_ENDPOINT 时（OBS 不是 off）尊重它，不覆盖，也不检查容器。
#
# 用法：
#   . "$ROOT/scripts/lib/obs.sh"
#   obs_setup [采样器 [采样器参数]]
# 采样器和参数是调用脚本给的默认值（比如 bench.sh 传 parentbased_traceidratio 0.01）；不传就不设，服务用全量采样。
# 调用方已经设了 OTEL_TRACES_SAMPLER、OTEL_TRACES_SAMPLER_ARG、OTEL_METRIC_EXPORT_INTERVAL 的不覆盖。
# 只在导出时才设这几个默认值。变量都 export，随后启动的服务进程会继承。
#
# 调用之后：OBS_EXPORTING=1 表示在导出；OBS_GRAFANA_URL 是 Grafana 的地址。

OBS_CONTAINER="${COMPOSE_PROJECT_NAME:-oppenheimer}-otel-lgtm-1"
OBS_GRAFANA_URL="http://127.0.0.1:${GRAFANA_PORT:-3000}"
OBS_ENDPOINT="http://127.0.0.1:${OTEL_GRPC_PORT:-4317}"
OBS_EXPORTING=0

obs_setup() {
  local sampler="${1:-}" sampler_arg="${2:-}" mode="${OBS:-auto}" status

  OBS_EXPORTING=0
  case "$mode" in
  off)
    unset OTEL_EXPORTER_OTLP_ENDPOINT
    return 0
    ;;
  auto | on) ;;
  *)
    echo "OBS must be auto, on or off, got $mode" >&2
    exit 2
    ;;
  esac

  if [ -z "${OTEL_EXPORTER_OTLP_ENDPOINT:-}" ]; then
    status="$(docker inspect -f '{{.State.Health.Status}}' "$OBS_CONTAINER" 2>/dev/null)" || status=""
    if [ "$status" != healthy ]; then
      if [ "$mode" = on ]; then
        echo "OBS=on but $OBS_CONTAINER is ${status:-not running}; run 'make obs-up' first" >&2
        exit 2
      fi
      return 0
    fi
    export OTEL_EXPORTER_OTLP_ENDPOINT="$OBS_ENDPOINT"
  fi

  export OTEL_METRIC_EXPORT_INTERVAL="${OTEL_METRIC_EXPORT_INTERVAL:-10000}"
  if [ -n "$sampler" ] && [ -z "${OTEL_TRACES_SAMPLER:-}" ]; then
    export OTEL_TRACES_SAMPLER="$sampler"
    if [ -n "$sampler_arg" ] && [ -z "${OTEL_TRACES_SAMPLER_ARG:-}" ]; then
      export OTEL_TRACES_SAMPLER_ARG="$sampler_arg"
    fi
  fi
  OBS_EXPORTING=1
}
