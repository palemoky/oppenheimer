#!/usr/bin/env bash
# 核对平时的构建里没有故障代码（设计 4a 的 3.3、pkg/fault 的包注释）。
#
#   scripts/check-faults.sh        （make check-faults；CI 的 faults 这一组也跑）
#
# 做法：
#   1. 对每个服务模块，找出只在 -tags faults 下才参加编译的 Go 文件（go list 带标签和不带标签的差），
#      取出里面声明的函数和方法名；同一个包里平时的构建也声明了的名字（接缝函数本身）不算。
#   2. 不带标签编出每个 cmd 的二进制（不去符号表），go tool nm 里不能出现这些名字，也不能出现 pkg/fault.Switch。
#   3. 反过来带标签编一次，这些名字至少要出现一个：否则第 2 步只是因为名字对不上才“通过”的。
#
# 构建标签本身已经保证带标签的文件不参加编译；这里防的是另一种错：有人把有缺陷的实现写进了不带标签的文件，
# 或者把 //go:build 那一行写错了（写错的约束会被当成普通注释，文件就进了平时的构建）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$(mktemp -d)"
trap 'rm -rf "${OUT:?}"' EXIT

rc=0
found_tagged=0
for mod in "$ROOT"/services/*/; do
  mod="${mod%/}"
  name="$(basename "$mod")"
  [[ -d "$mod/cmd" ]] || continue
  # 每行：包的导入路径<TAB>目录<TAB>文件名（空格分隔）
  (cd "$mod" && go list -f '{{.ImportPath}}{{"\t"}}{{.Dir}}{{"\t"}}{{join .GoFiles " "}}' ./...) >"$OUT/default.txt"
  (cd "$mod" && go list -tags faults -f '{{.ImportPath}}{{"\t"}}{{.Dir}}{{"\t"}}{{join .GoFiles " "}}' ./...) >"$OUT/faults.txt"
  # 输出要找的符号的正则（每行一个），没有带标签的文件时为空。
  python3 - "$OUT/default.txt" "$OUT/faults.txt" >"$OUT/symbols.txt" <<'PY'
import re, sys

def load(path):
    pkgs = {}
    for line in open(path):
        imp, d, files = (line.rstrip("\n").split("\t") + ["", ""])[:3]
        pkgs[imp] = (d, set(files.split()))
    return pkgs

FUNC = re.compile(r"^func\s+(?:\(\s*\w*\s*\*?\s*(\w+)[^)]*\)\s*)?(\w+)\s*[(\[]", re.M)

def decls(d, files):
    out = set()
    for f in files:
        for recv, name in FUNC.findall(open(f"{d}/{f}").read()):
            out.add((recv, name))
    return out

default, faults = load(sys.argv[1]), load(sys.argv[2])
for imp, (d, files) in sorted(faults.items()):
    base = default.get(imp, (d, set()))[1]
    only = files - base
    if not only:
        continue
    for recv, name in sorted(decls(d, only) - decls(d, base)):
        if name == "init":
            continue
        sym = re.escape(imp) + r"\." + (r"\(\*?" + recv + r"\)\." if recv else "") + re.escape(name)
        print(sym + r"(\.|$)")
PY
  if [[ ! -s "$OUT/symbols.txt" ]]; then
    continue
  fi
  found_tagged=1
  mkdir -p "$OUT/$name/default" "$OUT/$name/faults"
  (cd "$mod" && go build -o "$OUT/$name/default/" ./cmd/... && go build -tags faults -o "$OUT/$name/faults/" ./cmd/...)
  for bin in "$OUT/$name/default"/*; do
    go tool nm "$bin" | awk '{print $NF}' >"$OUT/nm.txt"
    if grep -E -f "$OUT/symbols.txt" "$OUT/nm.txt" >"$OUT/hits.txt" || grep -E 'oppenheimer/pkg/fault\.(Switch|Set)$' "$OUT/nm.txt" >>"$OUT/hits.txt"; then
      echo "FAIL: $name/$(basename "$bin") built without tags contains fault code:" >&2
      sed 's/^/    /' "$OUT/hits.txt" >&2
      rc=1
    fi
  done
  # 带标签的那一份里至少要找得到一个，证明上面的名字是对得上的。
  hit=0
  for bin in "$OUT/$name/faults"/*; do
    # 先落到文件再查：grep -q 找到就退出，管道前面的命令会收到 SIGPIPE，在 pipefail 下整条算失败。
    go tool nm "$bin" | awk '{print $NF}' >"$OUT/nm.txt"
    if grep -E -q -f "$OUT/symbols.txt" "$OUT/nm.txt"; then
      hit=1
    fi
  done
  if [[ $hit -eq 0 ]]; then
    echo "FAIL: $name: none of the tagged symbols show up in the binary built with -tags faults; the check would pass vacuously" >&2
    rc=1
  else
    echo "ok: $name ($(wc -l <"$OUT/symbols.txt" | tr -d ' ') tagged symbols, absent from the default build)"
  fi
done

if [[ $found_tagged -eq 0 ]]; then
  echo "FAIL: no files with the faults build tag were found in any service; the check has nothing to compare" >&2
  rc=1
fi
exit "$rc"
