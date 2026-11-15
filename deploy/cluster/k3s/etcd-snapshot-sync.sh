#!/usr/bin/env bash
# 模板：scripts/cluster/k3s.sh etcd-backup install 把占位符替换后装到 server 节点的
# /usr/local/sbin/etcd-snapshot-sync，由 etcd-snapshot-sync.timer 定时运行（root）。设计文档 2b 的 7.2。
#
# 做一件事：把 k3s 写在本地的 etcd 快照复制到 NAS，再把 NAS 上的旧快照删到只剩 KEEP 份。
#
# 为什么不让 k3s 直接把快照写到 NFS：快照目录挂着 NFS 时，NAS 一关机 k3s 的快照任务就卡在写上。
# 所以快照照常写本地盘（k3s 默认每 12 小时一次、本地留 5 份），这里只在复制的那一会儿挂载 NFS：
#   - 挂载用 soft 加短超时：NAS 不在时这个脚本失败退出（下一次定时再试），而不是永远挂住；
#   - 用 soft 的代价是写到一半 NAS 断开会得到一个 I/O 错误，这里可以接受：先写临时名字，
#     校验大小和 SHA-256 之后才改名，半截的文件不会被当成快照。
#
# 和快照一起复制的还有 server 的 token（/var/lib/rancher/k3s/server/token）。k3s 把集群的引导数据
# （CA 的私钥等）用这个 token 加密后存在 etcd 里，在一台新的机器上从快照恢复时必须给同一个 token，
# 否则快照读得出来、集群却起不来。只备份快照不备份 token，等于没有备份。
# token 放到 NAS 上没有扩大暴露面：快照本身就含有集群里全部的 Secret（没有开静态加密），
# 能读到快照的人已经拿到了一切；NAS 上的目录属于 root、权限 700。
set -euo pipefail

NAS="__NAS_IP__:__NAS_EXPORT__/backup/etcd" # 直接挂子目录：群晖共享文件夹的根带 ACL，从根走不进子目录
KEEP="__KEEP__"                             # NAS 上保留的份数
SRC=/var/lib/rancher/k3s/server/db/snapshots
TOKEN=/var/lib/rancher/k3s/server/token

log() { echo "etcd-snapshot-sync: $*"; }

mnt="$(mktemp -d /run/etcd-snapshot-sync.XXXXXX)"
cleanup() {
  mountpoint -q "$mnt" && umount "$mnt"
  rmdir "$mnt"
}
trap cleanup EXIT

# timeo 的单位是 0.1 秒：每次请求等 10 秒、重试 2 次，之后向调用方返回错误。
mount -t nfs -o nfsvers=4.1,soft,timeo=100,retrans=2 "$NAS" "$mnt"
install -d -m 700 "$mnt/snapshots"

# 上一次中断留下的临时文件
find "$mnt/snapshots" -maxdepth 1 -name '.tmp.*' -delete

copied=0
skipped=0
# k3s 的快照文件叫 <前缀>-<节点名>-<unix 时间>，前缀默认是 etcd-snapshot；按需快照是 on-demand-...。
# 只认普通文件。etcd 的客户端拍快照时先写 <名字>.part、写完才改名，所以跳过 .part 就不会拿到半截的。
while IFS= read -r -d '' f; do
  name="$(basename "$f")"
  dst="$mnt/snapshots/$name"
  # 名字里带时间戳，同名就是同一份；大小相同就不再复制。
  if [[ -f "$dst" && "$(stat -c %s "$dst")" == "$(stat -c %s "$f")" ]]; then
    skipped=$((skipped + 1))
    continue
  fi
  tmp="$mnt/snapshots/.tmp.$name"
  cp --preserve=timestamps "$f" "$tmp"
  # 读回来比对：确认 NAS 上落下的字节和本地的一样，再改名成正式的名字。
  want="$(sha256sum "$f" | cut -d' ' -f1)"
  got="$(sha256sum "$tmp" | cut -d' ' -f1)"
  if [[ "$want" != "$got" ]]; then
    log "checksum mismatch for $name (local $want, nas $got)"
    exit 1
  fi
  mv "$tmp" "$dst"
  printf '%s  %s\n' "$want" "$name" >"$dst.sha256"
  copied=$((copied + 1))
  log "copied $name ($(stat -c %s "$f") bytes)"
done < <(find "$SRC" -maxdepth 1 -type f ! -name '*.part' -print0)

install -m 600 "$TOKEN" "$mnt/token"

# 保留最新的 KEEP 份，按修改时间（复制时保留了本地的时间）。先复制成功、后删旧的。
# 只动符合快照命名的文件，目录里别的东西不碰。
removed=0
i=0
while IFS= read -r name; do
  i=$((i + 1))
  if ((i > KEEP)); then
    rm -f "$mnt/snapshots/$name" "$mnt/snapshots/$name.sha256"
    removed=$((removed + 1))
    log "removed $name"
  fi
done < <(cd "$mnt/snapshots" && find . -maxdepth 1 -type f ! -name '*.sha256' ! -name '.tmp.*' -printf '%T@ %f\n' | sort -rn | cut -d' ' -f2-)

total="$(find "$mnt/snapshots" -maxdepth 1 -type f ! -name '*.sha256' | wc -l)"
log "done: copied $copied, already there $skipped, removed $removed, on NAS $total (keep $KEEP)"
# 本地一份快照都没有不算成功：定时器在跑、NAS 上却永远是空的，这种“安静的失败”要让它显出来。
if ((copied + skipped == 0)); then
  log "no local snapshots in $SRC yet"
  exit 1
fi
