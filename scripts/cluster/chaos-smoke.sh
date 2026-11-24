#!/usr/bin/env bash
# Chaos Mesh 的冒烟（make chaos-smoke）：六类故障各造一个最小的对象，核对三件事：生效了没有、到时自己恢复了没有、
# 目标命名空间里留下什么痕迹。升级 k3s、containerd 或 Chaos Mesh 之后重跑一遍，看哪一类不能用了。
# 设计：docs/design/4a-chaos-prep.md 的 3.2。
#
# 前提：Chaos Mesh 已装（make chaos-mesh）；目标命名空间里有一套服务（E2E_KEEP=1 make cluster-e2e）。
# 只动 $CHAOS_SMOKE_NS（默认 oppenheimer-e2e）：长期运行的那几个命名空间会被拒绝。不发压。
#
# 做的事：
#   1. 在目标命名空间里起一个探测 Pod chaos-smoke（busybox，不进网格）。它既是量延迟的客户端，
#      也是压力、IO、DNS 这几类的目标：业务镜像里没有 shell，从里面量不了。时钟那一类的目标是 gateway
#      （要验的是 Go 进程；探测 Pod 里 busybox 的进程第一遍冒烟时没有受影响，原因没有查）。
#   2. 命名空间没有注解时建一个对象，核对它不生效（enableFilterNamespace）。
#   3. 给命名空间加注解，逐类：建对象（在命名空间 chaos-mesh 里）、等注入、量、等恢复、再量、删对象。
#   4. 列出目标命名空间里的痕迹，去掉注解，删探测 Pod。中途失败也会走这一步。
#
# 环境变量：CHAOS_SMOKE_NS、CHAOS_SMOKE_ONLY（只跑列出的几类，空格分隔：filter kill delay partition cpu mem io time dns）、
# CHAOS_SMOKE_KEEP=1（结束时留着探测 Pod 和注解，查问题用）。
# 退出码：0 全部通过；1 有不通过的；2 前提不满足。
#
# 传给探测 Pod 的 sh 片段故意用单引号：里面的 $ 要在 Pod 里展开。
# shellcheck disable=SC2016
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
export KUBECONFIG="$KUBECONFIG_OUT"

NS="${CHAOS_SMOKE_NS:-oppenheimer-e2e}"
CM=chaos-mesh
ONLY="${CHAOS_SMOKE_ONLY:-filter kill delay partition cpu mem io time dns}"
DURATION=30s
IMAGE="$REGISTRY_ADDR/test/busybox:1.37.0"

case "$NS" in
  oppenheimer | monitoring | mysql | redis | kafka | kube-system | istio-system | "$CM")
    echo "refusing to inject into namespace $NS" >&2
    exit 2
    ;;
esac
kubectl -n "$CM" get deploy chaos-controller-manager >/dev/null 2>&1 || {
  echo "Chaos Mesh is not installed; run make chaos-mesh" >&2
  exit 2
}
kubectl -n "$NS" get deploy gateway >/dev/null 2>&1 || {
  echo "no gateway in namespace $NS; run E2E_KEEP=1 make cluster-e2e first" >&2
  exit 2
}

RESULTS=()
FAILED=0
record() { # 名字 生效 恢复 备注
  RESULTS+=("$(printf '%-10s %-9s %-9s %s' "$1" "$2" "$3" "${4:-}")")
  if [[ "$2" == no || "$3" == no ]]; then FAILED=1; fi
}
wants() { [[ " $ONLY " == *" $1 "* ]]; }

# 探测 Pod 里取毫秒数的函数（busybox 的 date 没有 %N）：/proc/uptime 精确到 10 毫秒，也不受 TimeChaos 影响。
NOW_MS='now_ms() { awk "{printf \"%d\", \$1 * 1000}" /proc/uptime; }'

# 在探测 Pod 里运行一段 sh。
kx() { kubectl -n "$NS" exec deploy/chaos-smoke -c probe -- sh -c "$1" 2>&1; }

# 等一个 Chaos 对象的某个状况变成 True。$1 类型 $2 名字 $3 状况 $4 最多等几秒。
wait_cond() {
  local i
  for ((i = 0; i < $4; i++)); do
    if [[ "$(kubectl -n "$CM" get "$1" "$2" -o jsonpath="{.status.conditions[?(@.type==\"$3\")].status}" 2>/dev/null)" == True ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# 从标准输入建一个 Chaos 对象；失败时打印原因（多半是 webhook 拒绝）。
apply_chaos() {
  kubectl apply -f - >/dev/null
}
drop() { kubectl -n "$CM" delete "$1" "$2" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true; }

# 探测 Pod 访问 gateway 的 /healthz 用了多少毫秒；失败时输出 fail。
probe_ms() {
  kx "$NOW_MS; s=\$(now_ms); if wget -q -T 3 -O /dev/null http://gateway:8080/healthz 2>/dev/null; then e=\$(now_ms); echo \$((e - s)); else echo fail; fi" | tail -1
}

cleanup() {
  local rc=$?
  set +e
  echo "==> traces left in namespace $NS"
  echo "-- events mentioning chaos:"
  kubectl -n "$NS" get events -o custom-columns=KIND:.involvedObject.kind,NAME:.involvedObject.name,REASON:.reason,MSG:.message --no-headers 2>/dev/null |
    awk '{ l = tolower($0); gsub(/chaos-smoke/, "", l); if (l ~ /chaos/) print }' | cut -c1-220 | sort | uniq -c | head -20
  echo "-- per-pod chaos objects (created by Chaos Mesh next to the target pods):"
  kubectl -n "$NS" get podnetworkchaos,podiochaos,podhttpchaos --no-headers 2>/dev/null | head -20
  echo "-- pod annotations mentioning chaos:"
  kubectl -n "$NS" get pods -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.metadata.annotations}{"\n"}{end}' 2>/dev/null | grep -i 'chaos-mesh' | cut -c1-220
  if [[ "${CHAOS_SMOKE_KEEP:-}" != 1 ]]; then
    for kind in networkchaos podchaos stresschaos iochaos timechaos dnschaos; do
      kubectl -n "$CM" delete "$kind" -l oppenheimer.test/suite=chaos-smoke --timeout=90s >/dev/null 2>&1
    done
    kubectl annotate namespace "$NS" chaos-mesh.org/inject- >/dev/null 2>&1
    kubectl -n "$NS" delete deploy chaos-smoke --ignore-not-found --timeout=60s >/dev/null 2>&1
  fi
  echo
  printf '%-10s %-9s %-9s %s\n' CLASS INJECTED RECOVERED NOTE
  printf '%s\n' ${RESULTS[@]+"${RESULTS[@]}"}
  [[ $rc -ne 0 ]] && exit "$rc"
  exit "$FAILED"
}
trap cleanup EXIT

echo "==> probe pod chaos-smoke in $NS"
kubectl -n "$NS" apply -f - >/dev/null <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: chaos-smoke
  labels: {app: chaos-smoke}
spec:
  replicas: 1
  selector:
    matchLabels: {app: chaos-smoke}
  template:
    metadata:
      labels: {app: chaos-smoke}
      annotations:
        sidecar.istio.io/inject: "false"
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: probe
          image: $IMAGE
          command: ["sh", "-c", "while true; do sleep 3600; done"]
          resources:
            requests: {cpu: 10m, memory: 16Mi}
            limits: {cpu: 500m, memory: 256Mi}
          volumeMounts:
            - {name: data, mountPath: /data}
      volumes:
        - name: data
          emptyDir: {}
EOF
kubectl -n "$NS" rollout status deploy/chaos-smoke --timeout=2m >/dev/null
kx 'echo hello > /data/f' >/dev/null
base="$(probe_ms)"
echo "    baseline: gateway /healthz in ${base} ms"
[[ "$base" != fail ]] || {
  echo "the probe pod cannot reach gateway even without any fault" >&2
  exit 2
}

SEL_GATEWAY="namespaces: [$NS]
    labelSelectors: {app.kubernetes.io/name: gateway}"
SEL_PROBE="namespaces: [$NS]
    labelSelectors: {app: chaos-smoke}"

delay_object() {
  apply_chaos <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata: {name: $1, namespace: $CM, labels: {oppenheimer.test/suite: chaos-smoke}}
spec:
  action: delay
  mode: all
  selector:
    $SEL_GATEWAY
  delay: {latency: 300ms}
  duration: $DURATION
EOF
}

if wants filter; then
  echo "==> filter: an object must not take effect while the namespace has no annotation"
  kubectl annotate namespace "$NS" chaos-mesh.org/inject- >/dev/null 2>&1 || true
  delay_object smoke-filter
  sleep 8
  ms="$(probe_ms)"
  sel="$(kubectl -n "$CM" get networkchaos smoke-filter -o jsonpath='{.status.conditions[?(@.type=="Selected")].status}')"
  drop networkchaos smoke-filter
  if [[ "$ms" != fail && "$ms" -lt 200 && "$sel" != True ]]; then
    record filter blocked - "healthz ${ms} ms, Selected=${sel:-unset}"
  else
    record filter no - "took effect without the annotation: healthz ${ms} ms, Selected=$sel"
  fi
fi

kubectl annotate namespace "$NS" chaos-mesh.org/inject=enabled --overwrite >/dev/null

if wants kill; then
  echo "==> PodChaos: kill one gateway pod"
  before="$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=gateway -o jsonpath='{.items[*].metadata.uid}')"
  apply_chaos <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata: {name: smoke-kill, namespace: $CM, labels: {oppenheimer.test/suite: chaos-smoke}}
spec:
  action: pod-kill
  mode: one
  selector:
    $SEL_GATEWAY
EOF
  inj=no
  if wait_cond podchaos smoke-kill AllInjected 30; then
    sleep 2
    after="$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=gateway -o jsonpath='{.items[*].metadata.uid}')"
    [[ "$before" != "$after" ]] && inj=yes
  fi
  rec=no
  kubectl -n "$NS" rollout status deploy/gateway --timeout=3m >/dev/null 2>&1 && rec=yes
  drop podchaos smoke-kill
  record kill "$inj" "$rec" "recovery = the Deployment brings a new pod to Ready"
fi

if wants delay; then
  echo "==> NetworkChaos: 300 ms delay on the gateway pods (they carry an Istio sidecar)"
  delay_object smoke-delay
  inj=no ms=-
  if wait_cond networkchaos smoke-delay AllInjected 30; then
    ms="$(probe_ms)"
    [[ "$ms" != fail && "$ms" -ge 300 ]] && inj=yes
  fi
  rec=no after=-
  if wait_cond networkchaos smoke-delay AllRecovered 90; then
    after="$(probe_ms)"
    [[ "$after" != fail && "$after" -lt 200 ]] && rec=yes
  fi
  drop networkchaos smoke-delay
  record delay "$inj" "$rec" "healthz ${base} -> ${ms} -> ${after} ms"
fi

if wants partition; then
  echo "==> NetworkChaos: partition between the gateway pods and the probe pod"
  apply_chaos <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata: {name: smoke-partition, namespace: $CM, labels: {oppenheimer.test/suite: chaos-smoke}}
spec:
  action: partition
  mode: all
  selector:
    $SEL_GATEWAY
  direction: both
  target:
    mode: all
    selector:
      namespaces: [$NS]
      labelSelectors: {app: chaos-smoke}
  duration: $DURATION
EOF
  inj=no ms=-
  if wait_cond networkchaos smoke-partition AllInjected 30; then
    ms="$(probe_ms)"
    [[ "$ms" == fail ]] && inj=yes
  fi
  rec=no after=-
  if wait_cond networkchaos smoke-partition AllRecovered 90; then
    after="$(probe_ms)"
    [[ "$after" != fail ]] && rec=yes
  fi
  drop networkchaos smoke-partition
  record partition "$inj" "$rec" "healthz during: ${ms}, after: ${after} ms"
fi

# 探测 Pod 的 cgroup 在 5 秒里用掉的 CPU 毫秒数。
cpu_ms() { kx 'a=$(awk "/usage_usec/{print \$2}" /sys/fs/cgroup/cpu.stat); sleep 5; b=$(awk "/usage_usec/{print \$2}" /sys/fs/cgroup/cpu.stat); echo $(( (b - a) / 1000 ))' | tail -1; }
mem_mb() { kx 'echo $(( $(cat /sys/fs/cgroup/memory.current) / 1048576 ))' | tail -1; }

if wants cpu; then
  echo "==> StressChaos: CPU in the probe pod (limit 500m)"
  b="$(cpu_ms)"
  apply_chaos <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata: {name: smoke-cpu, namespace: $CM, labels: {oppenheimer.test/suite: chaos-smoke}}
spec:
  mode: all
  selector:
    $SEL_PROBE
  stressors:
    cpu: {workers: 1, load: 100}
  duration: $DURATION
EOF
  inj=no d=-
  if wait_cond stresschaos smoke-cpu AllInjected 30; then
    d="$(cpu_ms)"
    [[ "$d" =~ ^[0-9]+$ && "$d" -ge 1000 ]] && inj=yes
  fi
  rec=no a=-
  if wait_cond stresschaos smoke-cpu AllRecovered 90; then
    a="$(cpu_ms)"
    [[ "$a" =~ ^[0-9]+$ && "$a" -lt 500 ]] && rec=yes
  fi
  drop stresschaos smoke-cpu
  record cpu "$inj" "$rec" "CPU ms used per 5 s: ${b} -> ${d} -> ${a}"
fi

if wants mem; then
  echo "==> StressChaos: 96 MB of memory in the probe pod (limit 256Mi)"
  b="$(mem_mb)"
  apply_chaos <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata: {name: smoke-mem, namespace: $CM, labels: {oppenheimer.test/suite: chaos-smoke}}
spec:
  mode: all
  selector:
    $SEL_PROBE
  stressors:
    memory: {workers: 1, size: 96MB}
  duration: $DURATION
EOF
  inj=no d=-
  if wait_cond stresschaos smoke-mem AllInjected 30; then
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      d="$(mem_mb)"
      if [[ "$d" =~ ^[0-9]+$ && "$d" -ge $((b + 60)) ]]; then
        inj=yes
        break
      fi
      sleep 2
    done
  fi
  rec=no a=-
  if wait_cond stresschaos smoke-mem AllRecovered 90; then
    sleep 3
    a="$(mem_mb)"
    [[ "$a" =~ ^[0-9]+$ && "$a" -lt $((b + 30)) ]] && rec=yes
  fi
  drop stresschaos smoke-mem
  record mem "$inj" "$rec" "cgroup memory MB: ${b} -> ${d} -> ${a}"
fi

# 读一次 /data/f 用了多少毫秒。
io_ms() { kx "$NOW_MS; s=\$(now_ms); cat /data/f >/dev/null; e=\$(now_ms); echo \$((e - s))" | tail -1; }

if wants io; then
  echo "==> IOChaos: 500 ms latency on the probe pod's /data volume"
  b="$(io_ms)"
  apply_chaos <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: IOChaos
metadata: {name: smoke-io, namespace: $CM, labels: {oppenheimer.test/suite: chaos-smoke}}
spec:
  action: latency
  mode: all
  selector:
    $SEL_PROBE
  containerNames: [probe]
  volumePath: /data
  path: /data/**/*
  delay: 500ms
  percent: 100
  duration: $DURATION
EOF
  inj=no d=-
  if wait_cond iochaos smoke-io AllInjected 30; then
    d="$(io_ms)"
    [[ "$d" =~ ^[0-9]+$ && "$d" -ge 400 ]] && inj=yes
  fi
  rec=no a=-
  if wait_cond iochaos smoke-io AllRecovered 90; then
    a="$(io_ms)"
    [[ "$a" =~ ^[0-9]+$ && "$a" -lt 200 ]] && rec=yes
  fi
  drop iochaos smoke-io
  record io "$inj" "$rec" "read /data/f ms: ${b} -> ${d} -> ${a}"
fi

# gateway 的 Go 进程看到的时间比这台机器慢多少秒：net/http 的服务端在每个响应上写 Date 头，取的是进程自己的时钟。
# 要量长期在跑的进程：TimeChaos 改的是注入那一刻已经在跑的进程。
clock_behind() {
  local hdr
  hdr="$(kx 'wget -S -q -T 3 -O /dev/null http://gateway:8080/healthz 2>&1' | sed -n 's/^ *[Dd]ate: *//p' | tr -d '\r' | tail -1)"
  [[ -n "$hdr" ]] || {
    echo "?"
    return
  }
  python3 -c 'import email.utils, sys, time; print(round(time.time() - email.utils.parsedate_to_datetime(sys.argv[1]).timestamp()))' "$hdr" 2>/dev/null || echo "?"
}

if wants time; then
  echo "==> TimeChaos: the gateway pods' clock one hour back (a Go process)"
  b="$(clock_behind)"
  apply_chaos <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: TimeChaos
metadata: {name: smoke-time, namespace: $CM, labels: {oppenheimer.test/suite: chaos-smoke}}
spec:
  mode: all
  selector:
    $SEL_GATEWAY
  containerNames: [gateway]
  timeOffset: "-1h"
  duration: $DURATION
EOF
  inj=no d=-
  if wait_cond timechaos smoke-time AllInjected 30; then
    sleep 2
    # 两个副本不一定同时注入完，多量几次，取到一次就算。
    for _ in 1 2 3 4 5; do
      d="$(clock_behind)"
      if [[ "$d" =~ ^[0-9]+$ && "$d" -ge 3500 ]]; then
        inj=yes
        break
      fi
      sleep 1
    done
  fi
  rec=no a=-
  if wait_cond timechaos smoke-time AllRecovered 90; then
    sleep 2
    rec=yes
    for _ in 1 2 3 4; do
      a="$(clock_behind)"
      [[ "$a" =~ ^-?[0-9]+$ && "$a" -lt 60 ]] || rec=no
    done
  fi
  drop timechaos smoke-time
  record time "$inj" "$rec" "gateway's Date header behind by s: ${b} -> ${d} -> ${a}"
fi

# busybox 的 nslookup 在 NXDOMAIN 时退出码也是 0，所以看输出里有没有应答。
resolves() { kx "nslookup $1 2>&1 | grep -q '^Name:' && echo ok || echo nx" | tail -1; }

if wants dns; then
  # 用“让一个本来能解析的名字解析失败”来验，不用“让不存在的名字解析出来”：这个集群的上游 DNS 在代理后面，
  # 不存在的名字也会得到一个假地址（198.18.0.0/15），后一种验法分不出来。
  name="gateway.$NS.svc.cluster.local"
  echo "==> DNSChaos: $name fails to resolve in the probe pod"
  b="$(resolves "$name")"
  apply_chaos <<EOF
apiVersion: chaos-mesh.org/v1alpha1
kind: DNSChaos
metadata: {name: smoke-dns, namespace: $CM, labels: {oppenheimer.test/suite: chaos-smoke}}
spec:
  action: error
  mode: all
  selector:
    $SEL_PROBE
  patterns: ["gateway.*"]
  duration: $DURATION
EOF
  inj=no d=- o=-
  if wait_cond dnschaos smoke-dns AllInjected 30; then
    sleep 2
    d="$(resolves "$name")"
    o="$(resolves "order.$NS.svc.cluster.local")"
    [[ "$b" == ok && "$d" == nx ]] && inj=yes
  fi
  rec=no a=-
  if wait_cond dnschaos smoke-dns AllRecovered 90; then
    sleep 2
    a="$(resolves "$name")"
    [[ "$a" == ok ]] && rec=yes
  fi
  drop dnschaos smoke-dns
  record dns "$inj" "$rec" "$name: ${b} -> ${d} -> ${a}; a name outside the pattern during: ${o}"
fi
