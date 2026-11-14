package e2e

// 订单冷热归档（ADR-0016）：往热库塞一年前的订单，运行 archiver，核对订单从热库搬到了冷库，
// 而且经网关查到的订单详情和列表在归档前后完全一致。
//
// 依赖：E2E_ORDER_DSNS（订单主库）、E2E_ARCHIVE_DSN（冷库）、E2E_ARCHIVER_BIN（archiver 可执行文件），
// 都由 scripts/e2e.sh 设置；缺任何一个就跳过。

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/mysqlx"
	"github.com/palemoky/oppenheimer/pkg/sharding"
)

const (
	archiveDSNEnv   = "E2E_ARCHIVE_DSN"
	archiverBinEnv  = "E2E_ARCHIVER_BIN"
	archiveOldSKU   = 1 // 旧订单明细里的 SKU，只是个数，不要求库存库里存在
	archiveOldPrice = 1999
)

// 订单状态的数值，与 proto OrderStatus 一致。
const (
	statusPendingPayment = 2
	statusPaid           = 3
	statusCancelled      = 4
	statusClosed         = 5
	statusFailed         = 6
)

// orderDetail 是 GET /api/v1/orders/{id} 的响应里归档前后要保持一致的部分。
// items 不关心内部字段，按解码出的通用结构整体比较。
type orderDetail struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
	Status  string `json:"status"`
	Total   money  `json:"total"`
	Items   []any  `json:"items"`
}

type orderSummaryPage struct {
	Orders []struct {
		OrderID string `json:"order_id"`
		Status  string `json:"status"`
	} `json:"orders"`
	NextCursor string `json:"next_cursor"`
}

func getOrderDetail(t testing.TB, userID int64, orderID string) orderDetail {
	t.Helper()
	r := mustCall(t, http.MethodGet, "/api/v1/orders/"+orderID, userID, "", nil)
	if r.status != http.StatusOK {
		t.Fatalf("get order %s: status %d: %s", orderID, r.status, r.body)
	}
	var d orderDetail
	r.decode(t, &d)
	return d
}

// existsIn 报告 table 里有没有 order_id 这一行。
func existsIn(ctx context.Context, db *sql.DB, table string, orderID int64) (bool, error) {
	var n int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE order_id = ?", orderID).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("count %s order %d: %w", table, orderID, err)
	}
	return n > 0, nil
}

func TestArchiveMovesOldTerminalOrdersToColdStore(t *testing.T) {
	orderDSNs := splitDSNs(os.Getenv(orderDSNsEnv))
	archiveDSN := os.Getenv(archiveDSNEnv)
	archiverBin := os.Getenv(archiverBinEnv)
	if len(orderDSNs) == 0 || archiveDSN == "" || archiverBin == "" {
		t.Skipf("%s, %s and %s are required; run via `make e2e`", orderDSNsEnv, archiveDSNEnv, archiverBinEnv)
	}
	ctx := t.Context()

	router, err := sharding.New(sharding.OrderLayout(len(orderDSNs)))
	if err != nil {
		t.Fatal(err)
	}
	hotDBs := openAll(t, orderDSNsEnv, orderDSNs)
	cold, err := mysqlx.Open(ctx, mysqlx.Config{DSN: archiveDSN, MaxOpenConns: 2})
	if err != nil {
		t.Fatalf("connect %s: %v", archiveDSNEnv, err)
	}
	t.Cleanup(func() { _ = cold.Close() })

	// 同一个随机的大用户号：一个用户的订单都在同一个分片里，列表翻页覆盖旧新两类订单。
	userID := 1<<40 + rand.Int64N(1<<40)
	slot := router.ForUser(userID)
	hot := hotDBs[router.DB(slot)]
	ordersTable := sharding.Table("orders", slot)
	itemsTable := sharding.Table("order_items", slot)

	// ---------- 1. 直接往热库插入一年前的订单 ----------
	yearAgo := time.Now().AddDate(-1, 0, 0).UTC()
	tenYears := time.Now().AddDate(10, 0, 0).UTC()
	type oldOrder struct {
		id     int64
		status int
	}
	statuses := []int{statusPaid, statusCancelled, statusClosed, statusFailed, statusPaid, statusPendingPayment}
	var olds []oldOrder
	for i, st := range statuses {
		// 订单号低 6 位必须是用户的基因；序号从 1 起，让 6 个订单的号严格递增。
		id := idgen.LowerBound(yearAgo) | int64(i+1)<<6 | idgen.GeneOf(userID)
		expire := yearAgo
		if st == statusPendingPayment {
			// 很久以后才到期：后台的超时关单任务不会碰它，它一直是待支付。
			expire = tenYears
		}
		var paidAt any
		var paymentID any
		if st == statusPaid {
			paidAt = yearAgo
			paymentID = id
		}
		tx, err := hot.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO "+ordersTable+
			" (order_id, user_id, merchant_id, status, total_amount, currency, idempotency_key, request_hash,"+
			" payment_id, expire_at, paid_at, created_at, updated_at, version)"+
			" VALUES (?, ?, 1, ?, ?, 'CNY', ?, ?, ?, ?, ?, ?, ?, 3)",
			id, userID, st, archiveOldPrice, fmt.Sprintf("e2e-archive-%d", id), make([]byte, 32),
			paymentID, expire, paidAt, yearAgo, yearAgo)
		if err == nil {
			_, err = tx.ExecContext(ctx, "INSERT INTO "+itemsTable+
				" (order_id, sku_id, quantity, unit_price, created_at) VALUES (?, ?, 1, ?, ?)",
				id, archiveOldSKU, archiveOldPrice, yearAgo)
		}
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert old order %d: %v", id, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit old order %d: %v", id, err)
		}
		olds = append(olds, oldOrder{id: id, status: st})
	}
	t.Cleanup(func() {
		// 库每次运行都重建，这里只是不让失败的运行留下半截数据；清理失败不影响结果。
		ctx := context.WithoutCancel(ctx)
		for _, o := range olds {
			_, _ = hot.ExecContext(ctx, "DELETE FROM "+ordersTable+" WHERE order_id = ?", o.id)
			_, _ = hot.ExecContext(ctx, "DELETE FROM "+itemsTable+" WHERE order_id = ?", o.id)
			_, _ = cold.ExecContext(ctx, "DELETE FROM orders_archive WHERE order_id = ?", o.id)
		}
	})

	// ---------- 2. 通过网关正常下一个新订单 ----------
	sku := newSKU(t, 10)
	fresh := mustCreateOrder(t, userID, sku, 1)
	freshID, err := strconv.ParseInt(fresh.OrderID, 10, 64)
	if err != nil {
		t.Fatalf("parse fresh order id %q: %v", fresh.OrderID, err)
	}

	// ---------- 3. 归档前经网关查 6 个旧订单 ----------
	before := make(map[int64]orderDetail, len(olds))
	for _, o := range olds {
		d := getOrderDetail(t, userID, strconv.FormatInt(o.id, 10))
		if d.OrderID != strconv.FormatInt(o.id, 10) {
			t.Fatalf("before archive: asked for %d, got order %s", o.id, d.OrderID)
		}
		before[o.id] = d
	}

	// ---------- 4. 运行 archiver ----------
	runArchiver := func() {
		t.Helper()
		cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(cctx, archiverBin)
		cmd.Env = append(os.Environ(), "MYSQL_DSNS="+os.Getenv(orderDSNsEnv), "ARCHIVE_DSN="+archiveDSN)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("archiver failed: %v\n%s", err, out)
		}
	}
	runArchiver()

	// ---------- 5 + 6 + 7 ----------
	check := func(round string) {
		t.Helper()
		for _, o := range olds {
			inHot, err := existsIn(ctx, hot, ordersTable, o.id)
			if err != nil {
				t.Fatal(err)
			}
			inCold, err := existsIn(ctx, cold, "orders_archive", o.id)
			if err != nil {
				t.Fatal(err)
			}
			if o.status == statusPendingPayment {
				if !inHot || inCold {
					t.Errorf("%s: pending order %d: hot=%v cold=%v, want hot only", round, o.id, inHot, inCold)
				}
				continue
			}
			if inHot || !inCold {
				t.Errorf("%s: terminal order %d (status %d): hot=%v cold=%v, want cold only", round, o.id, o.status, inHot, inCold)
			}
			// 订单明细也一并搬走。
			if n, err := existsIn(ctx, hot, itemsTable, o.id); err != nil {
				t.Fatal(err)
			} else if n {
				t.Errorf("%s: order %d still has items in hot %s", round, o.id, itemsTable)
			}
		}
		if inHot, err := existsIn(ctx, hot, ordersTable, freshID); err != nil {
			t.Fatal(err)
		} else if !inHot {
			t.Errorf("%s: fresh order %d is not in hot store", round, freshID)
		}
		if inCold, err := existsIn(ctx, cold, "orders_archive", freshID); err != nil {
			t.Fatal(err)
		} else if inCold {
			t.Errorf("%s: fresh order %d was archived", round, freshID)
		}

		// 经网关查，归档前后一致：订单号、状态、金额、明细。
		for _, o := range olds {
			after := getOrderDetail(t, userID, strconv.FormatInt(o.id, 10)) //nolint:contextcheck // 网关的辅助函数自己用 t.Context()
			if !reflect.DeepEqual(before[o.id], after) {
				t.Errorf("%s: order %d changed through the gateway:\n before %+v\n after  %+v", round, o.id, before[o.id], after)
			}
		}

		// 列表用 limit=2 翻到底：7 个订单，不重不漏，订单号严格递减。
		var ids []int64
		cursor := ""
		for range 10 {
			q := url.Values{"limit": {"2"}}
			if cursor != "" {
				q.Set("cursor", cursor)
			}
			r := mustCall(t, http.MethodGet, "/api/v1/orders?"+q.Encode(), userID, "", nil) //nolint:contextcheck // 网关的辅助函数自己用 t.Context()
			if r.status != http.StatusOK {
				t.Fatalf("%s: list orders: status %d: %s", round, r.status, r.body)
			}
			var page orderSummaryPage
			r.decode(t, &page)
			if len(page.Orders) > 2 {
				t.Fatalf("%s: page has %d orders, limit is 2", round, len(page.Orders))
			}
			for _, o := range page.Orders {
				id, err := strconv.ParseInt(o.OrderID, 10, 64)
				if err != nil {
					t.Fatalf("%s: parse listed order id %q: %v", round, o.OrderID, err)
				}
				ids = append(ids, id)
			}
			if page.NextCursor == "" {
				break
			}
			cursor = page.NextCursor
		}
		want := []int64{freshID}
		for _, o := range slices.Backward(olds) {
			want = append(want, o.id)
		}
		if !slices.Equal(ids, want) {
			t.Errorf("%s: listed orders = %v, want %v (newest first, none missing or repeated)", round, ids, want)
		}
	}
	check("after first archive")

	// ---------- 8. 再运行一次，结果不变 ----------
	runArchiver()
	check("after second archive")
}
