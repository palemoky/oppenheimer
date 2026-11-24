#!/usr/bin/env bash
# 故障实验的注入脚本（make chaos-blind / chaos-inject / chaos-recover / chaos-reveal）。
# 设计：docs/design/4a-chaos-prep.md 的 3.5；场景的格式：chaos/README.md。
#
#   chaos-run.sh blind   --pool <风险档>[:<类别>,...] [--dry-run]
#       盲测：在池里随机抽一个场景（最近做过的降权），密封，注入。终端只打印运行号和 sealed.json 的 SHA-256。
#       池的写法：1、1,2、1:pod,network（第三档拒绝盲测）。
#   chaos-run.sh inject  --scenario <编号> [--confirm-risk3 <编号>] [--dry-run]
#       不盲：指定场景，其余同上，会打印场景的内容。第三档要 --confirm-risk3 带上同一个编号。
#   chaos-run.sh recover [--run <运行号>] [--no-recon] [--dry-run]
#       恢复当前的运行（执行恢复步骤、去注解、记结束时刻），等回到稳态，等静止并调 recon-check，结果写进运行目录。
#   chaos-run.sh recover --all [--restart] [--dry-run]
#       总闸：删掉所有 Chaos Mesh 的实验对象，关掉所有故障开关（五个服务的每个副本），删掉脚本建的 VirtualService 等，
#       去掉注解，把还没恢复的运行标成已恢复。--restart 另外滚动重启那五个服务（泄漏类的后果要靠它清）。
#       之后列出目标命名空间里还留着的 Pod*Chaos 对象和事件（盲测的运行没有 reveal 时只写文件，不打印）。
#   chaos-run.sh reveal  [--run <运行号>] [--sha <终端上记下的摘要>] [--force]
#       打开答案，按 docs/postmortems/TEMPLATE.md 生成复盘草稿。运行还没恢复时拒绝，--force 例外。
#   chaos-run.sh lint [<编号>...]     检查场景目录的格式（不连集群）。不给编号就查全部。
#   chaos-run.sh status               当前有没有进行中的运行（只打印运行号和阶段）。
#
# 环境变量：
#   CHAOS_NS             目标命名空间，默认 oppenheimer，只允许 oppenheimer 和 oppenheimer-e2e
#                        （monitoring、kube-system、istio-system、chaos-mesh 永远拒绝，别的也一律拒绝）
#   CHAOS_STEADY_SKIP    传给 chaos-steady.sh check 的跳过项（见它的头部）
#   CHAOS_RECOVER_WAIT   恢复后等回到稳态最多多少秒，默认 900
#   CHAOS_RECON_WAIT     等 recon-check 核对到结束时刻最多多少秒，默认 1800
#
# 一次只有一个进行中的运行。注入的步骤：检查稳态 -> 选 -> 密封 -> 加注解 -> 记开始时刻 -> 按方式注入。
# 注入中途失败或者被 Ctrl-C：走一遍恢复（trap），运行标成 aborted。注入成功之后脚本就退出了，
# 故障靠自己的 duration/ttl 到时恢复，是否已经撤干净以 recover 为准。
#
# 盲测时终端和 chaos/runs/ 之外的任何地方不出现编号、目标、方式：细节只写进运行目录里的 private.log。
# 在 macOS 自带的 bash 3.2 上运行（不用关联数组、mapfile）。
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
# shellcheck source=scripts/cluster/cluster.env
source "$HERE/cluster.env"
export KUBECONFIG="$KUBECONFIG_OUT"

TNS="${CHAOS_NS:-oppenheimer}"
CM=chaos-mesh
ANNOT=chaos-mesh.org/inject
RUN_LABEL=oppenheimer.chaos/run
SCEN_DIR="$ROOT/chaos/scenarios"
RUNS_DIR="$ROOT/chaos/runs"
RECOVER_WAIT="${CHAOS_RECOVER_WAIT:-900}"
RECON_WAIT="${CHAOS_RECON_WAIT:-1800}"
# 能注入的命名空间只有这两个。别的（尤其下面四个）永远拒绝。
ALLOWED_NS="oppenheimer oppenheimer-e2e"
# 带故障开关的五个服务和它们的管理端口（总闸逐个关）。
SERVICES="gateway:8089 order:8081 inventory:8082 payment:8083 orderindex:8084"
# Chaos Mesh 的实验对象的类型：从装着的 CRD 里取（不含 Pod*Chaos 这类 Chaos Mesh 自己建的痕迹，和它的控制面对象）。
# 取不到时（没有装、连不上）退回一份固定的清单。
CHAOS_KINDS_FALLBACK="podchaos networkchaos stresschaos iochaos timechaos dnschaos httpchaos kernelchaos blockchaos jvmchaos physicalmachinechaos schedule workflow"
CHAOS_KINDS_CACHE=""
chaos_kinds() {
  if [[ -z "$CHAOS_KINDS_CACHE" ]]; then
    CHAOS_KINDS_CACHE="$(kubectl get crd -o name 2>/dev/null |
      sed -n 's|^customresourcedefinition.apiextensions.k8s.io/\([a-z]*\)\.chaos-mesh\.org$|\1|p' |
      grep -vxE 'podnetworkchaos|podiochaos|podhttpchaos|physicalmachines|remoteclusters|statuschecks|workflownodes|workflowtemplates' |
      tr '\n' ' ' || true)"
    [[ -n "$CHAOS_KINDS_CACHE" ]] || CHAOS_KINDS_CACHE="$CHAOS_KINDS_FALLBACK"
  fi
  printf '%s' "$CHAOS_KINDS_CACHE"
}

die() { echo "error: $1" >&2; exit "${2:-1}"; }
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

check_ns() {
  case "$TNS" in
  monitoring | kube-system | istio-system | chaos-mesh) die "refusing namespace $TNS: never injectable" 2 ;;
  esac
  [[ " $ALLOWED_NS " == *" $TNS "* ]] || die "namespace $TNS is not allowed (only: $ALLOWED_NS)" 2
}

need_cluster() {
  [[ -f "$KUBECONFIG" ]] || die "$KUBECONFIG not found; run scripts/cluster/k3s.sh kubeconfig first" 2
  for t in jq python3 curl kubectl shasum; do
    command -v "$t" >/dev/null 2>&1 || die "need $t" 2
  done
}

# ---- 私有日志：细节只写这里，终端上不打印 ----
PRIVLOG=/dev/null
plog() { printf '%s %s\n' "$(now)" "$*" >>"$PRIVLOG"; }
# 执行一条命令，输出进私有日志，返回它的退出码。
run_q() { "$@" >>"$PRIVLOG" 2>&1; }

# ---- 场景 YAML（子集）转 JSON ----
yaml_to_json() {
  python3 -I - "$1" <<'PY'
import json, re, sys

def scalar(s):
    s = s.strip()
    if s == "":
        return None
    if s.startswith("[") and s.endswith("]"):
        return [scalar(x) for x in s[1:-1].split(",") if x.strip()]
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"":
        return s[1:-1]
    if re.fullmatch(r"-?\d+", s):
        return int(s)
    return s

lines = []
with open(sys.argv[1], encoding="utf-8") as f:
    for raw in f:
        t = raw.rstrip("\n")
        if not t.strip() or t.lstrip().startswith("#"):
            continue
        ind = len(t) - len(t.lstrip(" "))
        if t[ind:ind + 1] == "\t":
            sys.exit("tab indentation is not supported: " + t.strip())
        lines.append((ind, t.strip()))

pos = 0

def block(ind):
    global pos
    if lines[pos][1].startswith("- "):
        out = []
        while pos < len(lines) and lines[pos][0] == ind and lines[pos][1].startswith("- "):
            out.append(scalar(lines[pos][1][2:]))
            pos += 1
        return out
    out = {}
    while pos < len(lines) and lines[pos][0] == ind:
        text = lines[pos][1]
        m = re.fullmatch(r"([A-Za-z_][\w-]*):(?:\s+(.*))?", text)
        if not m:
            sys.exit("cannot parse: " + text)
        k, v = m.group(1), m.group(2)
        pos += 1
        if v is None or v.strip() == "":
            if pos < len(lines) and lines[pos][0] > ind:
                out[k] = block(lines[pos][0])
            else:
                out[k] = None
        else:
            out[k] = scalar(v)
    if pos < len(lines) and lines[pos][0] > ind:
        sys.exit("unexpected indentation at: " + lines[pos][1])
    return out

result = block(0) if lines else {}
if pos < len(lines):
    sys.exit("unexpected content at: " + lines[pos][1])
json.dump(result, sys.stdout, ensure_ascii=False)
PY
}

scenario_json() { # 编号
  local f="$SCEN_DIR/$1/scenario.yaml"
  [[ -f "$f" ]] || die "no such scenario: $1" 2
  yaml_to_json "$f"
}

# ---- 场景的格式检查：把问题逐行打印到标准输出，返回问题的个数（通过全局 LINT_N） ----
LINT_N=0
# 文件里以 $2 开头的行中不符合 $3 的那些（没有管道，不碰 pipefail 下 grep -q 的 SIGPIPE）。
bad_lines() {
  local l
  while IFS= read -r l; do
    [[ "$l" =~ $3 ]] || printf '%s\n' "$l"
  done < <(grep -E "^[[:space:]]*$2" "$1" || true)
}
lint_err() { echo "  $1: $2"; LINT_N=$((LINT_N + 1)); }

lint_scenario() {
  local id="$1" dir="$SCEN_DIR/$1" sj m r f
  [[ -d "$dir" ]] || { lint_err "$id" "no such directory"; return; }
  if ! sj="$(yaml_to_json "$dir/scenario.yaml" 2>&1)"; then
    lint_err "$id" "scenario.yaml: $sj"
    return
  fi
  [[ "$id" =~ ^s[0-9]{3}$ ]] || lint_err "$id" "directory name must be s + 3 digits"
  [[ "$(jq -r '.id // ""' <<<"$sj")" == "$id" ]] || lint_err "$id" "id does not match the directory name"
  m="$(jq -r '.method // ""' <<<"$sj")"
  case "$m" in chaos-mesh | fault-switch | istio | script) ;; *) lint_err "$id" "method must be chaos-mesh|fault-switch|istio|script, got '$m'" ;; esac
  r="$(jq -r '.risk // ""' <<<"$sj")"
  case "$r" in 1 | 2 | 3) ;; *) lint_err "$id" "risk must be 1, 2 or 3, got '$r'" ;; esac
  [[ "$(jq -r '.category // ""' <<<"$sj")" =~ ^[a-z][a-z0-9-]*$ ]] || lint_err "$id" "category is required (lower case word)"
  [[ "$(jq -r '.target.workload // ""' <<<"$sj")" =~ ^[a-z][a-z0-9-]*$ ]] || lint_err "$id" "target.workload is required"
  [[ "$(jq -r '.target.namespace // ""' <<<"$sj")" == "" ]] || lint_err "$id" "target.namespace must not be set (CHAOS_NS decides)"
  [[ "$(jq -r '.duration // ""' <<<"$sj")" =~ ^[0-9]+[smh]$ ]] || lint_err "$id" "duration must look like 30s, 5m, 1h"
  for f in requires recover.notes recover.restart expected_alerts; do
    [[ "$(jq -r "(.${f} | type) // \"null\"" <<<"$sj")" == array ]] || lint_err "$id" "$f must be a list (use [] for none)"
  done
  # truth.md 的三栏
  if [[ -f "$dir/truth.md" ]]; then
    for f in "标准根因" "定位必需的关键证据" "容易误判成的原因"; do
      grep -q "^## $f" "$dir/truth.md" || lint_err "$id" "truth.md has no '## $f'"
    done
  else
    lint_err "$id" "truth.md is missing"
  fi
  # inject/
  [[ -d "$dir/inject" ]] || { lint_err "$id" "inject/ is missing"; return; }
  case "$m" in
  chaos-mesh | istio)
    local n=0 y
    for y in "$dir"/inject/*.yaml; do
      [[ -f "$y" ]] || continue
      n=$((n + 1))
      grep -q '__RUN_ID__' "$y" || lint_err "$id" "$(basename "$y"): no __RUN_ID__ (name and label must carry the run id)"
      grep -q "$RUN_LABEL: __RUN_ID__" "$y" || lint_err "$id" "$(basename "$y"): missing label $RUN_LABEL: __RUN_ID__"
      # 选择器里的命名空间只能是占位符。
      if [[ -n "$(bad_lines "$y" 'namespaces:' 'namespaces:[[:space:]]*\[__TARGET_NS__\][[:space:]]*$')" ]]; then
        lint_err "$id" "$(basename "$y"): selector namespaces must be exactly [__TARGET_NS__]"
      fi
      if [[ "$m" == chaos-mesh ]]; then
        grep -qE '^[[:space:]]*namespaces:' "$y" || lint_err "$id" "$(basename "$y"): no 'namespaces: [__TARGET_NS__]' selector"
        if [[ -n "$(bad_lines "$y" 'namespace:' 'namespace:[[:space:]]*chaos-mesh[[:space:]]*$')" ]]; then
          lint_err "$id" "$(basename "$y"): metadata.namespace must be chaos-mesh"
        fi
      else
        if [[ -n "$(bad_lines "$y" 'namespace:' 'namespace:[[:space:]]*__TARGET_NS__[[:space:]]*$')" ]]; then
          lint_err "$id" "$(basename "$y"): metadata.namespace must be __TARGET_NS__"
        fi
      fi
    done
    [[ $n -gt 0 ]] || lint_err "$id" "inject/ has no *.yaml"
    ;;
  fault-switch)
    if [[ -f "$dir/inject/switch.yaml" ]]; then
      local sw
      if ! sw="$(yaml_to_json "$dir/inject/switch.yaml" 2>&1)"; then
        lint_err "$id" "switch.yaml: $sw"
      else
        [[ "$(jq -r '.switch // ""' <<<"$sw")" =~ ^f[0-9]{2}$ ]] || lint_err "$id" "switch.yaml: switch must be f01..f99"
        [[ "$(jq -r '.ttl // ""' <<<"$sw")" =~ ^[0-9]+[smh]$ ]] || lint_err "$id" "switch.yaml: ttl is required (like 30m)"
        [[ "$(jq -r '.admin_port // ""' <<<"$sw")" =~ ^[0-9]+$ ]] || lint_err "$id" "switch.yaml: admin_port is required"
        if [[ "$(jq -r '.ttl // ""' <<<"$sw")" =~ ^[0-9]+[smh]$ ]] && [[ "$(jq -r '.duration // ""' <<<"$sj")" =~ ^[0-9]+[smh]$ ]]; then
          [[ "$(dur_s "$(jq -r .ttl <<<"$sw")")" -le "$(dur_s "$(jq -r .duration <<<"$sj")")" ]] || lint_err "$id" "switch.yaml: ttl is longer than duration"
        fi
      fi
    else
      lint_err "$id" "inject/switch.yaml is missing"
    fi
    ;;
  script)
    [[ -x "$dir/inject/run.sh" ]] || lint_err "$id" "inject/run.sh is missing or not executable"
    ;;
  esac
}

dur_s() { # 30s 5m 1h -> 秒
  local n="${1%[smh]}"
  case "$1" in
  *s) echo "$n" ;;
  *m) echo $((n * 60)) ;;
  *h) echo $((n * 3600)) ;;
  esac
}

cmd_lint() {
  local ids="$*" id
  if [[ -z "$ids" ]]; then
    ids="$(cd "$SCEN_DIR" && command ls -d s[0-9]* 2>/dev/null | tr '\n' ' ')"
  fi
  [[ -n "$ids" ]] || die "no scenarios under $SCEN_DIR" 2
  for id in $ids; do lint_scenario "$id"; done
  if [[ $LINT_N -gt 0 ]]; then
    echo "lint: $LINT_N problem(s)"
    exit 1
  fi
  echo "lint: ok ($ids)"
}

# ---- 运行目录和状态 ----
RUN="" RUNDIR=""
use_run() {
  RUN="$1"
  RUNDIR="$RUNS_DIR/$RUN"
  [[ -f "$RUNDIR/sealed.json" ]] || die "no such run: $RUN" 2
  PRIVLOG="$RUNDIR/private.log"
}
state_get() { jq -r --arg k "$1" '.[$k] // empty' "$RUNDIR/state.json"; }
state_set() { # 键 值
  local tmp="$RUNDIR/state.json.tmp"
  jq --arg k "$1" --arg v "$2" '.[$k] = $v' "$RUNDIR/state.json" >"$tmp" && mv "$tmp" "$RUNDIR/state.json"
}
sealed() { jq -r "$1" "$RUNDIR/sealed.json"; }

# 进行中的运行（阶段 injecting 或 injected），每行一个运行号。
active_runs() {
  local d ph
  for d in "$RUNS_DIR"/r*/; do
    [[ -f "${d}state.json" ]] || continue
    ph="$(jq -r '.phase // ""' "${d}state.json")"
    case "$ph" in injecting | injected) basename "$d" ;; esac
  done
}
# 有没有盲测的运行还没 reveal：有的话，目标和方式的痕迹不能打印到终端。
blind_open() {
  local d
  for d in "$RUNS_DIR"/r*/; do
    [[ -f "${d}sealed.json" && -f "${d}state.json" ]] || continue
    [[ "$(jq -r '.blind' "${d}sealed.json")" == true ]] || continue
    [[ "$(jq -r '.phase // ""' "${d}state.json")" == revealed ]] || return 0
  done
  return 1
}
the_active_run() { # 只有一个进行中的运行时打印它
  local n
  n="$(active_runs)"
  [[ -n "$n" ]] || die "no active run" 1
  [[ "$(printf '%s\n' "$n" | wc -l | tr -d ' ')" == 1 ]] || die "more than one active run: $(echo "$n" | tr '\n' ' '); pass --run" 1
  printf '%s' "$n"
}

# ---- 目标 Pod 和故障开关 ----
PF_PID=""
pods_of() { # 工作负载
  kubectl -n "$TNS" get pods -l "app.kubernetes.io/name=$1" --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>>"$PRIVLOG"
}

# 经端口转发对一个 Pod 的管理端口发一个 PUT。$1 Pod $2 管理端口 $3 路径加查询
pf_put() {
  local pod="$1" port="$2" q="$3" log lp="" i rc=0
  log="$(mktemp "${TMPDIR:-/tmp}/chaos-pf.XXXXXX")"
  kubectl -n "$TNS" port-forward "pod/$pod" ":$port" >"$log" 2>&1 &
  PF_PID=$!
  for ((i = 0; i < 40; i++)); do
    lp="$(awk '/^Forwarding from 127\.0\.0\.1:/ { split($3, a, ":"); print a[2]; exit }' "$log")"
    [[ -n "$lp" ]] && break
    kill -0 "$PF_PID" 2>/dev/null || break
    sleep 0.5
  done
  if [[ -n "$lp" ]]; then
    curl -fsS -m 10 -X PUT "http://127.0.0.1:$lp$q" -o /dev/null >>"$PRIVLOG" 2>&1 || rc=$?
  else
    rc=1
    cat "$log" >>"$PRIVLOG"
  fi
  kill "$PF_PID" 2>/dev/null || true
  wait "$PF_PID" 2>/dev/null || true
  PF_PID=""
  rm -f "$log"
  return "$rc"
}

# 对一个工作负载的每个副本发 PUT。$1 工作负载 $2 端口 $3 路径加查询。有一个失败就返回 1，但每个都会试。
fault_each() {
  local w="$1" port="$2" q="$3" pod pods bad=0 n=0
  pods="$(pods_of "$w")"
  for pod in $pods; do
    n=$((n + 1))
    pf_put "$pod" "$port" "$q" || { bad=$((bad + 1)); plog "fault PUT failed on $pod ($q)"; }
  done
  [[ $n -gt 0 ]] || { plog "no running pod for workload $w"; return 1; }
  [[ $bad -eq 0 ]]
}

# ---- 渲染清单 ----
render() { # 文件
  sed -e "s|__TARGET_NS__|$TNS|g" -e "s|__RUN_ID__|$RUN|g" "$1"
}

# ---- 注入和撤销，按方式 ----
inject_by_method() {
  local m sid w f
  m="$(sealed .method)"
  sid="$(sealed .scenario)"
  w="$(sealed .workload)"
  case "$m" in
  chaos-mesh | istio)
    for f in "$SCEN_DIR/$sid"/inject/*.yaml; do
      plog "apply $(basename "$f")"
      render "$f" | run_q kubectl apply -f - || return 1
    done
    ;;
  fault-switch)
    local sw ttl port
    sw="$(sealed .params.switch)"
    ttl="$(sealed .params.ttl)"
    port="$(sealed .params.admin_port)"
    [[ -n "$ttl" ]] || { plog "fault switch without ttl refused"; return 1; }
    plog "switch $sw on, ttl $ttl, port $port, workload $w"
    fault_each "$w" "$port" "/fault/$sw?mode=on&ttl=$ttl" || return 1
    ;;
  script)
    plog "script $sid inject"
    TARGET_NS="$TNS" RUN_ID="$RUN" RUN_DIR="$RUNDIR" run_q "$SCEN_DIR/$sid/inject/run.sh" inject || return 1
    ;;
  esac
}

# 撤销一个运行：尽力而为，每一步失败只记录，不中止。返回 0 全部成功，1 有失败。
undo_run() {
  local m sid w bad=0 k
  m="$(sealed .method)"
  sid="$(sealed .scenario)"
  w="$(sealed .workload)"
  case "$m" in
  chaos-mesh)
    for k in $(chaos_kinds); do
      run_q kubectl -n "$CM" delete "$k" -l "$RUN_LABEL=$RUN" --ignore-not-found --timeout=90s || bad=1
    done
    ;;
  istio)
    run_q kubectl -n "$TNS" delete virtualservice,destinationrule,envoyfilter -l "$RUN_LABEL=$RUN" --ignore-not-found --timeout=90s || bad=1
    ;;
  fault-switch)
    local sw port
    sw="$(sealed .params.switch)"
    port="$(sealed .params.admin_port)"
    fault_each "$w" "$port" "/fault/$sw?mode=off" || { bad=1; plog "switch off failed on some replica; the ttl will turn it off"; }
    ;;
  script)
    TARGET_NS="$TNS" RUN_ID="$RUN" RUN_DIR="$RUNDIR" run_q "$SCEN_DIR/$sid/inject/run.sh" recover || bad=1
    ;;
  esac
  # 泄漏类的后果关了开关不会消失：按场景的 recover.restart 滚动重启。
  local r
  for r in $(jq -r '.scenario_spec.recover.restart[]?' "$RUNDIR/sealed.json"); do
    plog "rollout restart $r"
    run_q kubectl -n "$TNS" rollout restart "deploy/$r" || bad=1
    run_q kubectl -n "$TNS" rollout status "deploy/$r" --timeout=5m || bad=1
  done
  # 注解：只有一个进行中的运行，撤销它之后就没有别的了。
  run_q kubectl annotate namespace "$TNS" "$ANNOT-" || bad=1
  return "$bad"
}

# ---- 稳态 ----
check_steady() {
  echo "==> steady state check"
  if ! "$HERE/chaos-steady.sh" check; then
    die "not in steady state: refusing to inject (fix it, or start the load with: make chaos-steady ARGS=start)" 1
  fi
}

# ---- 抽签 ----
parse_pool() { # "1,2:pod,network" -> POOL_RISKS POOL_CATS
  local spec="$1"
  POOL_RISKS="${spec%%:*}"
  POOL_CATS=""
  [[ "$spec" == *:* ]] && POOL_CATS="${spec#*:}"
  [[ -n "$POOL_RISKS" ]] || die "empty pool" 2
  local r
  for r in ${POOL_RISKS//,/ }; do
    case "$r" in
    1 | 2) ;;
    3) die "risk 3 scenarios are never run blind (and never in a pool)" 2 ;;
    *) die "bad risk '$r' in pool (1 or 2)" 2 ;;
    esac
  done
}

pick_from_pool() { # 打印选中的编号
  local id sj risk categ recent cands="" w
  # 最近 5 次运行用过的编号降权。
  recent="$(
    for d in $(command ls -1d "$RUNS_DIR"/r*/ 2>/dev/null | sort | tail -n 5); do
      [[ -f "${d}sealed.json" ]] && jq -r '.scenario' "${d}sealed.json"
    done | tr '\n' ' '
  )"
  for id in $(cd "$SCEN_DIR" && command ls -d s[0-9]* 2>/dev/null); do
    sj="$(scenario_json "$id")" || continue
    # 格式有问题的场景不进池（盲测时不能打印是哪一个；lint 命令会报）。
    LINT_N=0
    lint_scenario "$id" >/dev/null 2>&1
    [[ $LINT_N -eq 0 ]] || continue
    risk="$(jq -r '.risk' <<<"$sj")"
    categ="$(jq -r '.category' <<<"$sj")"
    [[ ",$POOL_RISKS," == *",$risk,"* ]] || continue
    if [[ -n "$POOL_CATS" ]]; then [[ ",$POOL_CATS," == *",$categ,"* ]] || continue; fi
    w=4
    [[ " $recent" == *" $id "* ]] && w=1
    cands="$cands$id $w
"
  done
  [[ -n "$cands" ]] || die "no scenario in the pool" 2
  printf '%s' "$cands" | python3 -I -c '
import secrets, sys
items = [l.split() for l in sys.stdin if l.strip()]
total = sum(int(w) for _, w in items)
x = secrets.randbelow(total)
for i, w in items:
    x -= int(w)
    if x < 0:
        print(i)
        break'
}

# ---- 注入（blind 和 inject 共用） ----
INJECT_RUN=""
on_exit() {
  local rc=$?
  trap - EXIT INT TERM
  set +e
  [[ -n "$PF_PID" ]] && kill "$PF_PID" 2>/dev/null
  if [[ -n "$INJECT_RUN" ]]; then
    echo "!! injection did not finish; undoing run $INJECT_RUN" >&2
    use_run "$INJECT_RUN"
    undo_run
    state_set ended_at "$(now)"
    state_set phase aborted
    echo "!! undone (details in the run directory); check with: make chaos-recover ALL=1 if in doubt" >&2
  fi
  exit "$rc"
}

do_inject() { # blind(true|false) 编号 池描述 dry confirm
  local blind="$1" sid="$2" pool="$3" dry="$4" confirm="$5"
  local sj risk m
  sj="$(scenario_json "$sid")"
  risk="$(jq -r '.risk' <<<"$sj")"
  m="$(jq -r '.method' <<<"$sj")"
  if [[ "$risk" == 3 ]]; then
    [[ "$blind" == false ]] || die "risk 3 scenarios are never run blind" 2
    [[ "$confirm" == "$sid" ]] || die "scenario $sid is risk 3 (may lose data or need a human decision): pass --confirm-risk3 $sid" 2
  fi
  LINT_N=0
  if [[ "$blind" == true ]]; then lint_scenario "$sid" >/dev/null 2>&1; else lint_scenario "$sid" >&2; fi
  [[ $LINT_N -eq 0 ]] || die "the scenario fails lint (chaos-run.sh lint)" 2

  if [[ "$dry" == 1 ]]; then
    echo "dry-run: nothing is sent to the cluster, no run directory is created"
    echo "would: check the steady state (chaos-steady.sh check), namespace $TNS"
    if [[ "$blind" == true ]]; then
      echo "would: pick one scenario from the pool (not shown), write chaos/runs/<run>/sealed.json, print only the run id and its SHA-256"
      echo "would: annotate namespace $TNS with $ANNOT=enabled, record the start time, inject, return"
    else
      echo "would: seal scenario $sid (risk $risk, method $m, workload $(jq -r '.target.workload' <<<"$sj"), duration $(jq -r '.duration' <<<"$sj"))"
      echo "would: annotate namespace $TNS with $ANNOT=enabled, record the start time, then:"
      case "$m" in
      chaos-mesh) echo "  kubectl apply (objects in $CM, selector namespace $TNS):"; command ls "$SCEN_DIR/$sid/inject" | sed 's/^/    /' ;;
      istio) echo "  kubectl apply in $TNS:"; command ls "$SCEN_DIR/$sid/inject" | sed 's/^/    /' ;;
      fault-switch)
        local sw
        sw="$(yaml_to_json "$SCEN_DIR/$sid/inject/switch.yaml")"
        echo "  PUT /fault/$(jq -r .switch <<<"$sw")?mode=on&ttl=$(jq -r .ttl <<<"$sw") on port $(jq -r .admin_port <<<"$sw") of every running pod of $(jq -r '.target.workload' <<<"$sj") (via kubectl port-forward)"
        ;;
      script) echo "  inject/run.sh inject" ;;
      esac
      echo "  requires: $(jq -r '.requires | join("; ")' <<<"$sj")"
    fi
    echo "on failure or Ctrl-C: undo (delete objects / switch off / run.sh recover / remove the annotation) and mark the run aborted"
    return 0
  fi

  need_cluster
  [[ -z "$(active_runs)" ]] || die "there is already an active run ($(active_runs | tr '\n' ' ')): recover it first" 1
  kubectl get ns "$TNS" >/dev/null 2>&1 || die "cannot reach the cluster or namespace $TNS does not exist" 2
  if [[ "$m" == chaos-mesh ]]; then
    kubectl -n "$CM" get deploy chaos-controller-manager >/dev/null 2>&1 || die "Chaos Mesh is not installed (make chaos-mesh)" 2
  fi
  check_steady

  # 密封
  local run nonce sealed_at params
  run="r$(date -u +%Y%m%d-%H%M%S)-$(od -An -N2 -tx1 /dev/urandom | tr -d ' \n')"
  nonce="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
  mkdir -p "$RUNS_DIR/$run"
  RUN="$run"
  RUNDIR="$RUNS_DIR/$run"
  PRIVLOG="$RUNDIR/private.log"
  : >"$PRIVLOG"
  chmod 700 "$RUNDIR"
  params='{}'
  if [[ "$m" == fault-switch ]]; then
    params="$(yaml_to_json "$SCEN_DIR/$sid/inject/switch.yaml")"
  fi
  sealed_at="$(now)"
  jq -n --arg run "$run" --arg nonce "$nonce" --arg at "$sealed_at" --argjson blind "$blind" --arg pool "$pool" \
    --arg sid "$sid" --argjson spec "$sj" --arg ns "$TNS" --argjson params "$params" \
    '{run: $run, nonce: $nonce, sealed_at: $at, blind: $blind, pool: $pool, scenario: $sid, method: $spec.method, risk: $spec.risk,
      category: $spec.category, workload: $spec.target.workload, duration: $spec.duration, target_ns: $ns, params: $params,
      scenario_spec: $spec}' >"$RUNDIR/sealed.json"
  local sha
  sha="$(shasum -a 256 "$RUNDIR/sealed.json" | cut -d' ' -f1)"
  echo "$sha" >"$RUNDIR/sealed.sha256"
  jq -n --arg at "$sealed_at" '{phase: "injecting", sealed_at: $at}' >"$RUNDIR/state.json"
  echo "run:    $run"
  echo "sha256: $sha   (of chaos/runs/$run/sealed.json; write it down)"
  if [[ "$blind" == false ]]; then
    echo "scenario $sid: method $m, risk $risk, workload $(jq -r '.target.workload' <<<"$sj"), duration $(jq -r '.duration' <<<"$sj")"
    echo "requires: $(jq -r '.requires | join("; ")' <<<"$sj")"
  fi

  # 注入。从这里起任何失败都走撤销。
  INJECT_RUN="$run"
  trap on_exit EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  plog "scenario $sid method $m"
  run_q kubectl annotate namespace "$TNS" "$ANNOT=enabled" --overwrite || die "cannot annotate namespace $TNS" 1
  state_set started_at "$(now)"
  inject_by_method || die "injection failed (details in the run directory); undoing" 1
  state_set phase injected
  INJECT_RUN=""
  trap - EXIT INT TERM
  echo "injected at $(state_get started_at). The fault ends by itself (duration/ttl); recover with: make chaos-recover"
}

cmd_blind() {
  local pool="" dry=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
    --pool) pool="${2:-}"; shift 2 ;;
    --dry-run) dry=1; shift ;;
    *) die "unknown argument $1" 2 ;;
    esac
  done
  [[ -n "$pool" ]] || die "usage: chaos-run.sh blind --pool <risk>[:<category>,...]  (e.g. 1 or 1,2:pod,network)" 2
  check_ns
  parse_pool "$pool"
  local sid
  sid="$(pick_from_pool)"
  do_inject true "$sid" "$pool" "$dry" ""
}

cmd_inject() {
  local sid="" dry=0 confirm=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
    --scenario) sid="${2:-}"; shift 2 ;;
    --confirm-risk3) confirm="${2:-}"; shift 2 ;;
    --dry-run) dry=1; shift ;;
    *) die "unknown argument $1" 2 ;;
    esac
  done
  [[ -n "$sid" ]] || die "usage: chaos-run.sh inject --scenario <id> [--confirm-risk3 <id>] [--dry-run]" 2
  check_ns
  do_inject false "$sid" "" "$dry" "$confirm"
}

# ---- recover ----
list_leftovers() { # 输出到标准输出
  echo "-- Pod*Chaos objects left next to the target pods:"
  kubectl -n "$TNS" get podnetworkchaos,podiochaos,podhttpchaos --no-headers 2>&1 | grep -v 'No resources found' || true
  echo "-- events about them:"
  kubectl -n "$TNS" get events --no-headers \
    -o custom-columns=KIND:.involvedObject.kind,NAME:.involvedObject.name,REASON:.reason,MSG:.message 2>/dev/null |
    awk '$1 ~ /^Pod.*Chaos$/' | sort | uniq -c | head -n 20 || true
  echo "-- Chaos Mesh experiment objects still present anywhere:"
  local k
  for k in $(chaos_kinds); do
    kubectl get "$k" -A --no-headers 2>/dev/null || true
  done
}

recover_all() { # restart dry
  local restart="$1" dry="$2" ns s w port bad=0
  if [[ "$dry" == 1 ]]; then
    echo "dry-run: nothing is sent to the cluster. Would, for namespaces: $ALLOWED_NS"
    echo "  - delete every Chaos Mesh experiment object in all namespaces (every kind of the chaos-mesh.org CRDs except Pod*Chaos)"
    echo "  - on every running pod of $SERVICES (name:admin port): GET /fault via kubectl port-forward, then switch off every switch listed"
    echo "  - delete VirtualService/DestinationRule/EnvoyFilter carrying the label $RUN_LABEL"
    echo "  - run recover for scripts of active runs, remove the annotation $ANNOT"
    [[ "$restart" == 1 ]] && echo "  - rollout restart of the five services"
    echo "  - mark active runs as recovered, then list the Pod*Chaos leftovers (to a file while a blind run is not revealed)"
    return 0
  fi
  need_cluster
  echo "==> master switch: undoing everything the injector can create"
  local k
  for k in $(chaos_kinds); do
    kubectl delete "$k" --all -A --ignore-not-found --timeout=90s >>"$PRIVLOG" 2>&1 || { bad=1; echo "  could not delete kind $k (see log; it may not be installed)"; }
  done
  # 故障开关：先 GET 看开着哪些，再逐个关。没有 jq 之外的依赖。
  for ns in $ALLOWED_NS; do
    kubectl get ns "$ns" >/dev/null 2>&1 || continue
    TNS="$ns"
    for s in $SERVICES; do
      w="${s%%:*}"
      port="${s##*:}"
      pf_each_off "$w" "$port" || bad=1
    done
    kubectl -n "$ns" delete virtualservice,destinationrule,envoyfilter -l "$RUN_LABEL" --ignore-not-found --timeout=90s >>"$PRIVLOG" 2>&1 || true
    kubectl annotate namespace "$ns" "$ANNOT-" >>"$PRIVLOG" 2>&1 || true
    if [[ "$restart" == 1 ]]; then
      for s in $SERVICES; do
        w="${s%%:*}"
        kubectl -n "$ns" rollout restart "deploy/$w" >>"$PRIVLOG" 2>&1 || true
      done
    fi
  done
  TNS="${CHAOS_NS:-oppenheimer}"
  # 脚本类的运行：执行它的 recover。同时把进行中的运行标成已恢复。
  local r
  for r in $(active_runs); do
    use_run "$r"
    if [[ "$(sealed .method)" == script ]]; then
      TNS="$(sealed .target_ns)" TARGET_NS="$(sealed .target_ns)" RUN_ID="$r" RUN_DIR="$RUNDIR" run_q "$SCEN_DIR/$(sealed .scenario)/inject/run.sh" recover || bad=1
    fi
    state_set ended_at "$(now)"
    state_set phase recovered
    state_set recovered_by master_switch
  done
  PRIVLOG=/dev/null
  RUN="" RUNDIR=""
  echo "==> leftovers in $TNS"
  if blind_open; then
    mkdir -p "$RUNS_DIR"
    local f
    f="$RUNS_DIR/leftovers-$(date -u +%Y%m%d-%H%M%S).txt"
    list_leftovers >"$f" 2>&1
    echo "a blind run is not revealed yet: the list of leftover Pod*Chaos objects and events was written to $f; do not read it until reveal"
  else
    list_leftovers
  fi
  [[ $bad -eq 0 ]] || { echo "some steps failed (see the run logs); check by hand" >&2; return 1; }
}

# 对一个工作负载的每个副本：GET /fault，把开着的开关逐个关掉。
pf_each_off() { # 工作负载 端口
  local w="$1" port="$2" pod pods bad=0 log lp i names sw
  pods="$(pods_of "$w")"
  for pod in $pods; do
    log="$(mktemp "${TMPDIR:-/tmp}/chaos-pf.XXXXXX")"
    kubectl -n "$TNS" port-forward "pod/$pod" ":$port" >"$log" 2>&1 &
    PF_PID=$!
    lp=""
    for ((i = 0; i < 40; i++)); do
      lp="$(awk '/^Forwarding from 127\.0\.0\.1:/ { split($3, a, ":"); print a[2]; exit }' "$log")"
      [[ -n "$lp" ]] && break
      kill -0 "$PF_PID" 2>/dev/null || break
      sleep 0.5
    done
    if [[ -n "$lp" ]]; then
      # 平时的构建没有 /fault（404）：不是错误。开关的名字是 f01..f99，不依赖响应的格式，从里面找。
      names="$(curl -sS -m 10 "http://127.0.0.1:$lp/fault" 2>>"$PRIVLOG" | grep -oE 'f[0-9]{2}' | sort -u || true)"
      for sw in $names; do
        curl -fsS -m 10 -X PUT "http://127.0.0.1:$lp/fault/$sw?mode=off" -o /dev/null >>"$PRIVLOG" 2>&1 || { bad=1; plog "switch $sw off failed on $pod"; }
      done
    else
      bad=1
      plog "port-forward to $pod failed"
      cat "$log" >>"$PRIVLOG"
    fi
    kill "$PF_PID" 2>/dev/null || true
    wait "$PF_PID" 2>/dev/null || true
    PF_PID=""
    rm -f "$log"
  done
  return "$bad"
}

cmd_recover() {
  local all=0 restart=0 dry=0 run="" recon=1
  while [[ $# -gt 0 ]]; do
    case "$1" in
    --all) all=1; shift ;;
    --restart) restart=1; shift ;;
    --run) run="${2:-}"; shift 2 ;;
    --no-recon) recon=0; shift ;;
    --dry-run) dry=1; shift ;;
    *) die "unknown argument $1" 2 ;;
    esac
  done
  check_ns
  if [[ $all -eq 1 ]]; then
    recover_all "$restart" "$dry"
    return
  fi
  if [[ "$dry" == 1 ]]; then
    echo "dry-run: nothing is sent to the cluster. Would, for the active run (${run:-the only active one}):"
    echo "  - undo the injection (delete its objects / switch off on every replica / run.sh recover), rollout restart what the scenario lists, remove the annotation"
    echo "  - record the end time; wait up to ${RECOVER_WAIT}s for chaos-steady.sh check to pass"
    echo "  - recon-check --from <start> --to <end> (retry while it says 'not covered yet', up to ${RECON_WAIT}s) -> chaos/runs/<run>/recon.txt"
    return 0
  fi
  need_cluster
  [[ -n "$run" ]] || run="$(the_active_run)"
  use_run "$run"
  TNS="$(sealed .target_ns)"
  case "$(state_get phase)" in
  injecting | injected) ;;
  *) die "run $run is not active (phase $(state_get phase))" 1 ;;
  esac
  echo "==> recovering run $run"
  local bad=0
  undo_run || bad=1
  state_set ended_at "$(now)"
  state_set phase recovered
  state_set undo_ok "$([[ $bad -eq 0 ]] && echo yes || echo no)"
  if [[ $bad -ne 0 ]]; then
    echo "!! some recovery steps failed (details in the run directory). Run: make chaos-recover ALL=1" >&2
  fi

  # 等回到稳态
  local t0 waited=0
  t0="$(date +%s)"
  echo "==> waiting for the steady state (up to ${RECOVER_WAIT}s)"
  while true; do
    if "$HERE/chaos-steady.sh" check >>"$PRIVLOG" 2>&1; then
      state_set steady_again_at "$(now)"
      echo "    steady again after $(($(date +%s) - t0))s"
      break
    fi
    waited=$(($(date +%s) - t0))
    if [[ $waited -ge $RECOVER_WAIT ]]; then
      state_set steady_again_at "never"
      echo "!! not steady after ${RECOVER_WAIT}s (see private.log); the recon-check below still runs" >&2
      bad=1
      break
    fi
    sleep 30
  done

  if [[ $recon -eq 0 ]]; then
    echo "recon-check skipped (--no-recon)"
    return "$bad"
  fi
  # 对账检查：TO 之后核对要走过一轮才算覆盖到（exit 3），等。
  echo "==> recon-check from $(state_get started_at) to $(state_get ended_at)"
  sleep 2
  local rc=0 out="$RUNDIR/recon.txt"
  t0="$(date +%s)"
  while true; do
    rc=0
    "$HERE/recon-check.sh" --from "$(state_get started_at)" --to "$(state_get ended_at)" >"$out" 2>&1 || rc=$?
    [[ $rc -eq 3 ]] || break
    if [[ $(($(date +%s) - t0)) -ge $RECON_WAIT ]]; then break; fi
    sleep 60
  done
  state_set recon_rc "$rc"
  case "$rc" in
  0) echo "    recon-check: no differences (chaos/runs/$run/recon.txt)" ;;
  1) echo "!! recon-check: DIFFERENCES found (chaos/runs/$run/recon.txt)" >&2; bad=1 ;;
  3) echo "!! recon-check: the checkers have not covered the end time after ${RECON_WAIT}s; run recon-check by hand later" >&2; bad=1 ;;
  *) echo "!! recon-check: no conclusion (exit $rc, see chaos/runs/$run/recon.txt)" >&2; bad=1 ;;
  esac
  echo "==> done. Reveal the answer with: make chaos-reveal"
  return "$bad"
}

# ---- reveal ----
cmd_reveal() {
  local run="" sha="" force=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
    --run) run="${2:-}"; shift 2 ;;
    --sha) sha="${2:-}"; shift 2 ;;
    --force) force=1; shift ;;
    *) die "unknown argument $1" 2 ;;
    esac
  done
  if [[ -z "$run" ]]; then
    # 最近的、还没 reveal 的运行。
    local d
    for d in $(command ls -1d "$RUNS_DIR"/r*/ 2>/dev/null | sort -r); do
      [[ -f "${d}state.json" ]] || continue
      [[ "$(jq -r '.phase' "${d}state.json")" == revealed ]] && continue
      run="$(basename "$d")"
      break
    done
    [[ -n "$run" ]] || die "no run to reveal" 1
  fi
  use_run "$run"
  case "$(state_get phase)" in
  recovered | aborted | revealed) ;;
  *) [[ $force -eq 1 ]] || die "run $run is still active: recover it first (or --force)" 1 ;;
  esac
  local actual
  actual="$(shasum -a 256 "$RUNDIR/sealed.json" | cut -d' ' -f1)"
  if [[ -n "$sha" && "$sha" != "$actual" ]]; then
    die "sealed.json does NOT match the SHA-256 you wrote down ($sha); it was changed after sealing" 1
  fi
  if [[ "$actual" != "$(cat "$RUNDIR/sealed.sha256")" ]]; then
    die "sealed.json does not match chaos/runs/$run/sealed.sha256" 1
  fi
  [[ -n "$sha" ]] && echo "sealed.json matches the SHA-256 you gave"
  local sid date_s out
  sid="$(sealed .scenario)"
  date_s="$(sealed .sealed_at | cut -c1-10)"
  out="$ROOT/docs/postmortems/$date_s-$run.md"
  mkdir -p "$ROOT/docs/postmortems"
  [[ ! -e "$out" ]] || die "$out already exists" 1
  python3 -I - "$ROOT/docs/postmortems/TEMPLATE.md" "$SCEN_DIR/$sid/truth.md" "$RUNDIR/sealed.json" "$RUNDIR/state.json" "$RUNDIR/recon.txt" "$out" <<'PY'
import json, re, sys

tpl_path, truth_path, sealed_path, state_path, recon_path, out_path = sys.argv[1:]
sealed = json.load(open(sealed_path, encoding="utf-8"))
state = json.load(open(state_path, encoding="utf-8"))
try:
    recon = open(recon_path, encoding="utf-8").read().strip().splitlines()
except FileNotFoundError:
    recon = []

# truth.md 按 "## " 分栏
truth = {}
cur = None
for line in open(truth_path, encoding="utf-8").read().splitlines():
    if line.startswith("## "):
        cur = line[3:].strip()
        truth[cur] = []
    elif cur is not None:
        truth[cur].append(line)

def sect(name):
    body = truth.get(name, [])
    while body and not body[0].strip():
        body.pop(0)
    while body and not body[-1].strip():
        body.pop()
    return body

def indent(lines):
    return ["  " + l if l.strip() else "" for l in lines]

sid = sealed["scenario"]
run = sealed["run"]
started = state.get("started_at", "")
ended = state.get("ended_at", "")
steady = state.get("steady_again_at", "")
rc = state.get("recon_rc", "")
impact = ["", "- 对账检查（recon-check）退出码：%s（0 无差异，1 有差异，3 还没核对到结束时刻；原文在 `chaos/runs/%s/recon.txt`）" % (rc if rc != "" else "未运行", run)]
impact += ["- 注入的时刻：%s；恢复动作完成：%s；回到稳态：%s" % (started, ended, steady or "未记录")]
if recon:
    impact += ["", "```", *recon[-12:], "```"]

out = []
for line in open(tpl_path, encoding="utf-8").read().splitlines():
    if line.startswith("# 复盘："):
        line = "# 复盘：%s（运行 %s）" % (sid, run)
    elif line.startswith("- 日期："):
        line = "- 日期：" + sealed["sealed_at"][:10]
    elif line.startswith("- 实验编号："):
        line = "- 实验编号：%s（运行 %s）" % (sid, run)
    elif line.startswith("- 是否盲测："):
        line = "- 是否盲测：" + ("是" if sealed["blind"] else "否")
    elif line.startswith("- 实验清单："):
        line = "- 实验清单：`chaos/scenarios/%s/`" % sid
    elif line.startswith("| | 注入故障 |"):
        line = "| %s | 注入故障 |" % started
    elif line.startswith("| | 完全恢复 |") and steady and steady != "never":
        line = "| %s | 完全恢复（回到稳态） |" % steady
    out.append(line)
    if line.startswith("- 标准根因（一句话）："):
        out += indent(sect("标准根因"))
    elif line.startswith("- 定位根因必需的关键证据："):
        out += indent(sect("定位必需的关键证据"))
    elif line.startswith("- 容易误判成的其他原因："):
        out += indent(sect("容易误判成的原因"))
    elif line == "失败请求数、持续时间、数据一致性检查结果（超卖、对账差异）。":
        out += impact
with open(out_path, "w", encoding="utf-8") as f:
    f.write("\n".join(out) + "\n")
PY
  state_set phase revealed
  state_set revealed_at "$(now)"
  echo "run $run: scenario $sid, method $(sealed .method), risk $(sealed .risk), workload $(sealed .workload), namespace $(sealed .target_ns)"
  echo "started $(state_get started_at), ended $(state_get ended_at)"
  echo "postmortem draft: $out"
  echo "--- truth:"
  cat "$SCEN_DIR/$sid/truth.md"
}

cmd_status() {
  local r n=0
  for r in $(active_runs); do
    echo "active run $r (phase $(jq -r .phase "$RUNS_DIR/$r/state.json"))"
    n=$((n + 1))
  done
  [[ $n -gt 0 ]] || echo "no active run"
}

cmd="${1:-}"
[[ $# -gt 0 ]] && shift
case "$cmd" in
blind) cmd_blind "$@" ;;
inject) cmd_inject "$@" ;;
recover) cmd_recover "$@" ;;
reveal) cmd_reveal "$@" ;;
lint) cmd_lint "$@" ;;
status) cmd_status ;;
-h | --help | "") sed -n '2,32p' "${BASH_SOURCE[0]}"; [[ -n "$cmd" ]] || exit 2 ;;
*) die "usage: chaos-run.sh blind|inject|recover|reveal|lint|status (see --help)" 2 ;;
esac
