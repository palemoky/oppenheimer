package e2e

// 主从延迟下的读己之写。
//
// 这组用例通过 STOP REPLICA SQL_THREAD 人为制造从库延迟（IO 线程继续收 binlog，只是不回放），
// 验证对外查询在从库落后时仍然返回正确的状态。
//
// 并行性：这里的用例都不调用 t.Parallel()。Go 先顺序跑完所有非并行的顶层用例，
// 再放行被 t.Parallel() 暂停的用例，所以停复制的这段时间里不会有别的用例在跑，
// 不会出现“别的用例的读路径被我们停复制影响”的情况。每个用例也只断言自己创建的订单。

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
)

const (
	primaryDSNsEnv = "E2E_ORDER_PRIMARY_DSNS"
	replicaDSNsEnv = "E2E_ORDER_REPLICA_DSNS"
	readModeEnv    = "E2E_ORDER_READ_MODE"
	pinTTLEnv      = "E2E_ORDER_READ_PIN_TTL"
)

// lagEnv 是主从延迟用例需要的环境。数组下标就是分片号（用户号 % 2）。
type lagEnv struct {
	mode     string
	pinTTL   time.Duration
	primary  []*sql.DB
	replicas []*sql.DB
}

// setupLag 读取环境并连接两个主库和两个从库；条件不满足时跳过。
func setupLag(t *testing.T) *lagEnv {
	t.Helper()
	mode := os.Getenv(readModeEnv)
	switch mode {
	case "":
		t.Skipf("%s is not set; run via `make e2e`", readModeEnv)
	case "primary":
		// 一律读主库时从库延迟与读路径无关，这些用例没有可验证的东西。
		t.Skip("ORDER_READ_MODE=primary never reads replicas")
	}
	pinTTL, err := time.ParseDuration(os.Getenv(pinTTLEnv))
	if err != nil {
		t.Fatalf("parse %s: %v", pinTTLEnv, err)
	}
	env := &lagEnv{mode: mode, pinTTL: pinTTL}
	env.primary = openAdmin(t, primaryDSNsEnv)
	env.replicas = openAdmin(t, replicaDSNsEnv)
	return env
}

func openAdmin(t *testing.T, envName string) []*sql.DB {
	t.Helper()
	raw := os.Getenv(envName)
	if raw == "" {
		t.Skipf("%s is not set; run via `make e2e`", envName)
	}
	dsns := strings.Split(raw, ",")
	if len(dsns) != 2 {
		t.Fatalf("%s has %d DSNs, want 2", envName, len(dsns))
	}
	dbs := make([]*sql.DB, 0, len(dsns))
	for _, dsn := range dsns {
		db, err := mysqlx.Open(t.Context(), mysqlx.Config{DSN: dsn, MaxOpenConns: 2})
		if err != nil {
			t.Fatalf("connect %s: %v", envName, err)
		}
		t.Cleanup(func() { _ = db.Close() })
		dbs = append(dbs, db)
	}
	return dbs
}

// execAll 在每个从库上执行一条语句。用独立的 context：测试结束时 t.Context() 已经取消，
// 而 Cleanup 里恢复复制恰恰发生在那之后。
func (e *lagEnv) execAll(t *testing.T, stmt string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, db := range e.replicas {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Errorf("replica %d: %s: %v", i, stmt, err)
		}
	}
}

// stopApply 停掉两个从库的回放线程，并注册 Cleanup：无论用例怎么结束，
// 都恢复回放并等到追上主库，不把停着的复制留给后面的用例。
func (e *lagEnv) stopApply(t *testing.T) {
	t.Helper()
	e.execAll(t, "STOP REPLICA SQL_THREAD")
	t.Cleanup(func() {
		e.startApply(t)
		e.waitCaughtUp(t)
	})
}

func (e *lagEnv) startApply(t *testing.T) {
	t.Helper()
	e.execAll(t, "START REPLICA SQL_THREAD")
}

// waitCaughtUp 等每个从库回放到对应主库当前的 GTID 位置。
// WAIT_FOR_EXECUTED_GTID_SET 的超时传 0 表示无限等待，所以必须传正数；返回 0 成功，1 超时。
func (e *lagEnv) waitCaughtUp(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range e.replicas {
		var gtids string
		if err := e.primary[i].QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&gtids); err != nil {
			t.Errorf("primary %d: read gtid_executed: %v", i, err)
			continue
		}
		var rc int
		if err := e.replicas[i].QueryRowContext(ctx, "SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 20)", gtids).Scan(&rc); err != nil {
			t.Errorf("replica %d: wait for gtid: %v", i, err)
			continue
		}
		if rc != 0 {
			t.Errorf("replica %d did not catch up to %q within 20s (rc=%d)", i, gtids, rc)
		}
	}
}

// waitPinExpired 等标记过期。多留 0.5 秒余量：标记在写之前打下，过期时刻比这里的起点更早，
// 余量只是吸收 Redis 过期的粒度和调度抖动。
func (e *lagEnv) waitPinExpired() {
	time.Sleep(e.pinTTL + 500*time.Millisecond)
}

// userOnShard 返回落在指定分片的新用户。slot 取用户号低 3 位，slot % 2 选库，
// 也就是用户号的奇偶决定分片。
func userOnShard(shard int64) int64 {
	for {
		if u := newUser(); u%2 == shard {
			return u
		}
	}
}

func cancelOrder(t *testing.T, userID int64, id string) order {
	t.Helper()
	r := mustCall(t, http.MethodPost, "/api/v1/orders/"+id+"/cancel", userID, "", nil)
	if r.status != http.StatusOK {
		t.Fatalf("cancel %s: status %d: %s", id, r.status, r.body)
	}
	var o order
	r.decode(t, &o)
	return o
}

// A：复制停着时下单，立刻能查到。
// 从库里根本没有这一行，从库答“不存在”不可信，必须由主库确认；
// 取消的返回值总是主库的最新状态。标记过期之后再查仍然是 cancelled 而不是 404。
func TestReplicaLagFreshOrderIsVisible(t *testing.T) {
	env := setupLag(t)
	for shard := range int64(2) {
		t.Run(fmt.Sprintf("shard%d", shard), func(t *testing.T) {
			env.stopApply(t)
			sku, user := newSKU(t, 5), userOnShard(shard)

			// waitLanded 内部轮询：落库后必须看到 pending_payment，不能是 404 或一直 creating。
			o := mustCreateOrder(t, user, sku, 1)

			if got := cancelOrder(t, user, o.OrderID); got.Status != "cancelled" {
				t.Fatalf("cancel response status = %q, want cancelled", got.Status)
			}
			if got := getOrder(t, user, o.OrderID); got.Status != "cancelled" {
				t.Fatalf("get right after cancel = %q, want cancelled", got.Status)
			}

			// 终态已被缓存，这次查询多半读的是缓存；它验证的是整条链路在标记过期后仍然不是 404。
			env.waitPinExpired()
			if got := getOrder(t, user, o.OrderID); got.Status != "cancelled" {
				t.Fatalf("get after pin expiry = %q, want cancelled", got.Status)
			}
		})
	}
}

// B：从库里有旧行时，gtid 模式不读旧状态。
//
// 先让从库拿到 pending_payment 的这一行，再停回放并取消，主库是 cancelled 而从库还是旧的。
// 取消之后、标记过期之前不查询：取消后立刻查一次会读主库、得到 cancelled 并写进缓存
// （终态会缓存），之后的查询就只读缓存，测不到从库路径。所以第一次查询必须发生在标记过期之后。
//
//   - gtid：从库等不到主库的 GTID 位置（回放停着），超时后读主库，必须是 cancelled。
//   - window：已知局限。标记过期后从库有这一行但状态是旧的，会读到 pending_payment。
//     不当作失败，记录实际读到的状态，只要求是这两者之一。
func TestReplicaLagStaleRowOnReplica(t *testing.T) {
	env := setupLag(t)
	for shard := range int64(2) {
		t.Run(fmt.Sprintf("shard%d", shard), func(t *testing.T) {
			sku, user := newSKU(t, 5), userOnShard(shard)
			o := mustCreateOrder(t, user, sku, 1)
			env.waitCaughtUp(t) // 从库此刻已有 pending_payment 这一行

			env.stopApply(t)
			if got := cancelOrder(t, user, o.OrderID); got.Status != "cancelled" {
				t.Fatalf("cancel response status = %q, want cancelled", got.Status)
			}
			env.waitPinExpired() // 这期间不查询，见函数注释

			start := time.Now()
			got := getOrder(t, user, o.OrderID).Status
			t.Logf("mode=%s: first get after pin expiry = %q (took %v)", env.mode, got, time.Since(start))

			switch env.mode {
			case "gtid":
				if got != "cancelled" {
					t.Fatalf("gtid mode read %q after pin expiry with a stale replica, want cancelled", got)
				}
			default:
				if got != "pending_payment" && got != "cancelled" {
					t.Fatalf("%s mode read %q, want pending_payment (known limitation) or cancelled", env.mode, got)
				}
			}
		})
	}
}

// C：恢复复制后能读到最新状态。
// 停回放、取消、恢复回放并追上，等标记过期后查询必须是 cancelled。
// 外部无法区分这次读的是主库还是已追上的从库（两者答案相同），所以它验证的是
// “恢复之后读路径是通的、结果正确”，而不是具体走了哪个库。
func TestReplicaLagRecovery(t *testing.T) {
	env := setupLag(t)
	for shard := range int64(2) {
		t.Run(fmt.Sprintf("shard%d", shard), func(t *testing.T) {
			sku, user := newSKU(t, 5), userOnShard(shard)
			o := mustCreateOrder(t, user, sku, 1)
			env.waitCaughtUp(t)

			env.stopApply(t)
			if got := cancelOrder(t, user, o.OrderID); got.Status != "cancelled" {
				t.Fatalf("cancel response status = %q, want cancelled", got.Status)
			}
			env.startApply(t)
			env.waitCaughtUp(t)
			env.waitPinExpired()

			if got := getOrder(t, user, o.OrderID); got.Status != "cancelled" {
				t.Fatalf("get after recovery = %q, want cancelled", got.Status)
			}
		})
	}
}
