#!/usr/bin/env python3
"""对集群入口持续发请求，记下每一个“不该出现”的响应和它出现的时刻。

用来验证滚动发布、删 Pod 期间请求有没有失败（设计文档 2c 的 8.3）。只用标准库。

    scripts/cluster/probe.py --duration 120
    scripts/cluster/probe.py --duration 0        # 一直跑到 Ctrl-C

每个 worker 用自己的用户号，循环做三件事，覆盖三条同步调用链：
  stock   GET  /skus/{sku}/stock       gateway -> inventory
  create  POST /orders                 gateway -> order -> Redis 闸门、Kafka
  get     GET  /orders/{id}            gateway -> order（查刚才那一单）
比例是 stock 和 get 各每轮一次，create 每 --create-every 轮一次。

“不该出现”的定义：连接失败、超时、5xx、以及 create 和 get 上不是 2xx 的响应。
429（限流、背压）单独计数，不算失败：它是系统有意的答复，但数量多说明发得太快，结果不可信。
带降级原因码的 503 算失败：没有故障注入时不应该出现降级。

输出：每秒一行（只在这一秒有失败时打印），结束时按操作和结果汇总，并给出失败集中的时间段。
时间都是 UTC，和 kubectl 的事件、服务的日志对得上。
"""

import argparse
import collections
import http.client
import json
import signal
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit


def utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M:%S.%f")[:-3]


class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.counts = collections.Counter()  # (op, outcome) -> n
        self.failures = []  # (ts, op, outcome, detail)
        self.latency = collections.defaultdict(list)  # op -> [seconds]

    def record(self, op, outcome, failed, detail, elapsed):
        with self.lock:
            self.counts[(op, outcome)] += 1
            self.latency[op].append(elapsed)
            if failed:
                self.failures.append((time.time(), op, outcome, detail))


class Worker(threading.Thread):
    def __init__(self, args, user_id, stats, stop):
        super().__init__(daemon=True)
        self.args, self.user_id, self.stats, self.stop_event = args, user_id, stats, stop
        u = urlsplit(args.gateway)
        self.addr = (u.hostname, u.port or 80)
        self.conn = None
        self.seq = 0
        self.last_order = None

    def call(self, op, method, path, body=None, key=None):
        headers = {"Host": self.args.host, "X-User-Id": str(self.user_id)}
        if key:
            headers["Idempotency-Key"] = key
        data = None
        if body is not None:
            data = json.dumps(body)
            headers["Content-Type"] = "application/json"
        start = time.monotonic()
        try:
            if self.conn is None:
                self.conn = http.client.HTTPConnection(*self.addr, timeout=self.args.timeout)
            self.conn.request(method, "/api/v1" + path, body=data, headers=headers)
            resp = self.conn.getresponse()
            raw = resp.read()
            status = resp.status
        except Exception as e:  # 连接被重置、超时等：连接不能再用
            if self.conn is not None:
                self.conn.close()
            self.conn = None
            self.stats.record(op, type(e).__name__, True, str(e)[:120], time.monotonic() - start)
            return None, None
        elapsed = time.monotonic() - start
        payload = None
        try:
            payload = json.loads(raw) if raw else None
        except ValueError:
            pass
        if 200 <= status < 300:
            self.stats.record(op, str(status), False, "", elapsed)
        elif status == 429:
            self.stats.record(op, "429", False, "", elapsed)
        else:
            reason = ""
            if isinstance(payload, dict):
                reason = str(payload.get("reason") or payload.get("code") or payload.get("message") or "")
            self.stats.record(op, f"{status} {reason}".strip(), True, raw[:160].decode("utf-8", "replace"), elapsed)
        return status, payload

    def run(self):
        interval = 1.0 / self.args.rate
        rounds = 0
        nxt = time.monotonic()
        while not self.stop_event.is_set():
            self.call("stock", "GET", f"/skus/{self.args.sku}/stock")
            if rounds % self.args.create_every == 0:
                self.seq += 1
                key = f"probe-{self.args.run_id}-{self.user_id}-{self.seq}"
                status, payload = self.call(
                    "create", "POST", "/orders",
                    body={"items": [{"sku_id": str(self.args.sku), "quantity": 1}]}, key=key)
                if status is not None and 200 <= status < 300 and isinstance(payload, dict):
                    self.last_order = payload.get("order_id")
            if self.last_order:
                self.call("get", "GET", f"/orders/{self.last_order}")
            rounds += 1
            nxt += interval
            delay = nxt - time.monotonic()
            if delay > 0:
                self.stop_event.wait(delay)
            else:
                nxt = time.monotonic()  # 跟不上就不追，免得停顿之后一下子补发


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--gateway", default="http://192.168.50.51", help="入口的地址（MetalLB 给 Envoy Gateway 的那个）")
    p.add_argument("--host", default="api.oppenheimer.test", help="HTTPRoute 的主机名，放在 Host 头里")
    p.add_argument("--workers", type=int, default=20)
    p.add_argument("--rate", type=float, default=2.0, help="每个 worker 每秒多少轮（一轮 2 到 3 个请求）")
    p.add_argument("--create-every", type=int, default=4, help="每多少轮下一单。按用户的下单限额是每秒 5 个")
    p.add_argument("--sku", type=int, default=3001, help="库存足够多的 SKU")
    p.add_argument("--user-base", type=int, default=900000)
    p.add_argument("--duration", type=float, default=60, help="秒，0 表示一直跑到 Ctrl-C")
    p.add_argument("--timeout", type=float, default=5)
    p.add_argument("--run-id", default=str(int(time.time())))
    args = p.parse_args()

    stats, stop = Stats(), threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    workers = [Worker(args, args.user_base + i, stats, stop) for i in range(args.workers)]
    started = time.time()
    print(f"{utc(started)} probe start: {args.workers} workers x {args.rate}/s rounds, "
          f"gateway {args.gateway} host {args.host}", flush=True)
    for w in workers:
        w.start()

    reported = 0
    deadline = started + args.duration if args.duration > 0 else None
    while not stop.is_set():
        stop.wait(1.0)
        with stats.lock:
            new = stats.failures[reported:]
            reported = len(stats.failures)
            total = sum(stats.counts.values())
        if new:
            kinds = collections.Counter(f"{op}:{outcome}" for _, op, outcome, _ in new)
            print(f"{utc(time.time())} +{len(new)} failures  " +
                  "  ".join(f"{k} x{n}" for k, n in kinds.most_common()), flush=True)
        if deadline and time.time() >= deadline:
            stop.set()
    for w in workers:
        w.join(timeout=args.timeout + 1)

    elapsed = time.time() - started
    with stats.lock:
        total = sum(stats.counts.values())
        print(f"\n{utc(time.time())} probe end: {total} requests in {elapsed:.0f}s "
              f"({total / elapsed:.0f}/s), {len(stats.failures)} failures")
        for (op, outcome), n in sorted(stats.counts.items()):
            print(f"  {op:7s} {outcome:40s} {n}")
        for op, xs in sorted(stats.latency.items()):
            xs.sort()
            print(f"  {op:7s} latency p50 {xs[len(xs) // 2] * 1000:.1f} ms  "
                  f"p99 {xs[min(len(xs) - 1, int(len(xs) * 0.99))] * 1000:.1f} ms  max {xs[-1] * 1000:.0f} ms")
        if stats.failures:
            first, last = stats.failures[0][0], stats.failures[-1][0]
            print(f"  failures between {utc(first)} and {utc(last)} ({last - first:.1f}s)")
            for ts, op, outcome, detail in stats.failures[:5]:
                print(f"    {utc(ts)} {op} {outcome}: {detail}")
    raise SystemExit(1 if stats.failures else 0)


if __name__ == "__main__":
    main()
