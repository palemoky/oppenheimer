// Package e2e 通过网关的 HTTP 接口对整套系统做端到端验证。
//
// 测试需要四个服务都在运行，由 scripts/e2e.sh（make e2e）负责启动并设置环境变量。
// 直接 go test 时环境变量未设置，全部跳过。
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/palemoky/Oppenheimer/pkg/mysqlx"
)

const (
	gatewayEnv        = "E2E_GATEWAY_URL"
	inventoryDSNEnv   = "E2E_INVENTORY_DSN"
	paymentTimeoutEnv = "E2E_PAYMENT_TIMEOUT"

	unitPrice = 1999
)

var (
	baseURL     string
	inventoryDB *sql.DB
	payTimeout  time.Duration

	// 每个测试用自己的 SKU 和用户，互不干扰，可以并行。
	nextSKU  atomic.Int64
	nextUser atomic.Int64

	client = &http.Client{
		Timeout: 15 * time.Second,
		// 并发抢购会同时发出上百个请求，默认每主机只保留 2 个空闲连接，
		// 其余的每次都要重新建连，测的就成了建连而不是业务。
		Transport: &http.Transport{MaxIdleConnsPerHost: 256, MaxConnsPerHost: 256},
	}
)

func TestMain(m *testing.M) {
	baseURL = os.Getenv(gatewayEnv)
	if baseURL == "" {
		fmt.Printf("%s is not set; skipping end-to-end tests (run `make e2e`)\n", gatewayEnv)
		return
	}
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := mysqlx.Open(ctx, mysqlx.Config{DSN: os.Getenv(inventoryDSNEnv), MaxOpenConns: 4})
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to inventory db (%s): %v\n", inventoryDSNEnv, err)
		return 1
	}
	defer db.Close()
	inventoryDB = db

	payTimeout, err = time.ParseDuration(os.Getenv(paymentTimeoutEnv))
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse %s: %v\n", paymentTimeoutEnv, err)
		return 1
	}

	// 以时间为基数，重复运行时不会撞上之前的 SKU 和用户。
	base := time.Now().UnixMilli() % 1_000_000_000 * 1000
	nextSKU.Store(base)
	nextUser.Store(base)

	return m.Run()
}

// ---------- 与网关交互 ----------

type money struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

type order struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
	Status  string `json:"status"`
	Total   money  `json:"total"`
}

type payment struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
	Amount    money  `json:"amount"`
	Notified  bool   `json:"notified"`
}

type stock struct {
	Available int64 `json:"available"`
	Reserved  int64 `json:"reserved"`
	Sold      int64 `json:"sold"`
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type response struct {
	status int
	body   []byte
}

func (r response) decode(t testing.TB, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decode response (status %d): %v: %s", r.status, err, r.body)
	}
}

func (r response) errorCode() string {
	var e apiError
	if err := json.Unmarshal(r.body, &e); err != nil {
		return ""
	}
	return e.Error.Code
}

// call 发出请求。不在这里 t.Fatal：并发场景里它运行在非测试 goroutine 上。
func call(ctx context.Context, method, path string, userID int64, idemKey string, body any) (response, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return response{}, err
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, reader)
	if err != nil {
		return response{}, err
	}
	req.Header.Set("X-User-Id", strconv.FormatInt(userID, 10))
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return response{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return response{}, err
	}
	return response{status: resp.StatusCode, body: b}, nil
}

func mustCall(t testing.TB, method, path string, userID int64, idemKey string, body any) response {
	t.Helper()
	r, err := call(t.Context(), method, path, userID, idemKey, body)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return r
}

func createOrderBody(skuID int64, quantity int) map[string]any {
	return map[string]any{"items": []map[string]any{{"sku_id": strconv.FormatInt(skuID, 10), "quantity": quantity}}}
}

func mustCreateOrder(t testing.TB, userID, skuID int64, quantity int) order {
	t.Helper()
	r := mustCall(t, http.MethodPost, "/api/v1/orders", userID, newKey(), createOrderBody(skuID, quantity))
	if r.status != http.StatusCreated {
		t.Fatalf("create order: status %d: %s", r.status, r.body)
	}
	var o order
	r.decode(t, &o)
	return o
}

func getOrder(t testing.TB, userID int64, orderID string) order {
	t.Helper()
	r := mustCall(t, http.MethodGet, "/api/v1/orders/"+orderID, userID, "", nil)
	if r.status != http.StatusOK {
		t.Fatalf("get order %s: status %d: %s", orderID, r.status, r.body)
	}
	var o order
	r.decode(t, &o)
	return o
}

func getStock(t testing.TB, skuID int64) stock {
	t.Helper()
	r := mustCall(t, http.MethodGet, "/api/v1/skus/"+strconv.FormatInt(skuID, 10)+"/stock", 1, "", nil)
	if r.status != http.StatusOK {
		t.Fatalf("get stock %d: status %d: %s", skuID, r.status, r.body)
	}
	var s stock
	r.decode(t, &s)
	return s
}

// getPayment 返回订单的支付单；订单没有支付单时 found 为 false。
func getPayment(t testing.TB, userID int64, orderID string) (p payment, found bool) {
	t.Helper()
	r := mustCall(t, http.MethodGet, "/api/v1/orders/"+orderID+"/payment", userID, "", nil)
	switch r.status {
	case http.StatusOK:
		r.decode(t, &p)
		return p, true
	case http.StatusNotFound:
		return payment{}, false
	default:
		t.Fatalf("get payment of order %s: status %d: %s", orderID, r.status, r.body)
		return payment{}, false
	}
}

// ---------- 测试数据与断言 ----------

var keySeq atomic.Int64

func newKey() string {
	return fmt.Sprintf("e2e-%d-%d", time.Now().UnixNano(), keySeq.Add(1))
}

func newUser() int64 { return nextUser.Add(1) }

// newSKU 直接向库存库插入一个 SKU。1a 没有商品管理接口，测试数据只能这样准备。
func newSKU(t testing.TB, available int64) int64 {
	t.Helper()
	id := nextSKU.Add(1)
	_, err := inventoryDB.ExecContext(t.Context(),
		"INSERT INTO sku_stock (sku_id, name, unit_price, currency, available) VALUES (?, ?, ?, 'CNY', ?)",
		id, fmt.Sprintf("e2e sku %d", id), unitPrice, available)
	if err != nil {
		t.Fatalf("insert sku: %v", err)
	}
	return id
}

// eventually 轮询直到 cond 返回 true。系统是最终一致的：
// 支付结果、关单、库存结清都由后台任务异步完成，断言必须容忍这段延迟。
func eventually(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func wantStock(t testing.TB, skuID int64, want stock) {
	t.Helper()
	var got stock
	eventually(t, 10*time.Second, fmt.Sprintf("stock of sku %d to become %+v", skuID, want), func() bool {
		got = getStock(t, skuID)
		return got == want
	})
}

// ---------- 场景 ----------

// 主流程：下单 → 支付 → 订单变为已支付 → 库存从预占转为已售。
func TestOrderAndPay(t *testing.T) {
	t.Parallel()
	sku, user := newSKU(t, 10), newUser()

	o := mustCreateOrder(t, user, sku, 3)
	if o.Status != "pending_payment" {
		t.Fatalf("status = %q, want pending_payment", o.Status)
	}
	if want := (money{Amount: 3 * unitPrice, Currency: "CNY"}); o.Total != want {
		t.Errorf("total = %+v, want %+v", o.Total, want)
	}
	// 订单号低 6 位是用户号的分片基因。
	id, err := strconv.ParseInt(o.OrderID, 10, 64)
	if err != nil {
		t.Fatalf("order_id %q is not a decimal string: %v", o.OrderID, err)
	}
	if id%64 != user%64 {
		t.Errorf("order_id %% 64 = %d, user_id %% 64 = %d; shard gene missing", id%64, user%64)
	}
	wantStock(t, sku, stock{Available: 7, Reserved: 3, Sold: 0})

	r := mustCall(t, http.MethodPost, "/api/v1/orders/"+o.OrderID+"/pay", user, "", nil)
	if r.status != http.StatusAccepted {
		t.Fatalf("pay: status %d: %s", r.status, r.body)
	}

	eventually(t, 10*time.Second, "order to become paid", func() bool {
		return getOrder(t, user, o.OrderID).Status == "paid"
	})
	wantStock(t, sku, stock{Available: 7, Reserved: 0, Sold: 3})

	eventually(t, 10*time.Second, "payment to be acknowledged", func() bool {
		p, found := getPayment(t, user, o.OrderID)
		return found && p.Status == "succeeded" && p.Notified
	})

	// 别的用户看不到这个订单。
	if r := mustCall(t, http.MethodGet, "/api/v1/orders/"+o.OrderID, newUser(), "", nil); r.status != http.StatusNotFound {
		t.Errorf("other user's GET: status %d, want 404", r.status)
	}
}

// 并发抢购：远多于库存的用户同时下单，成交数必须恰好等于库存，不多不少。
func TestFlashSaleNeverOversells(t *testing.T) {
	t.Parallel()
	const stockQty, buyers = 20, 300
	sku := newSKU(t, stockQty)

	var created, soldOut atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range buyers {
		user := newUser()
		wg.Go(func() {
			<-start
			r, err := call(t.Context(), http.MethodPost, "/api/v1/orders", user, newKey(), createOrderBody(sku, 1))
			switch {
			case err != nil:
				t.Errorf("create order: %v", err)
			case r.status == http.StatusCreated:
				created.Add(1)
			case r.status == http.StatusConflict && r.errorCode() == "INSUFFICIENT_STOCK":
				soldOut.Add(1)
			default:
				t.Errorf("unexpected response: status %d: %s", r.status, r.body)
			}
		})
	}
	close(start)
	wg.Wait()

	if created.Load() != stockQty {
		t.Errorf("%d orders created, want exactly %d", created.Load(), stockQty)
	}
	if soldOut.Load() != buyers-stockQty {
		t.Errorf("%d buyers saw sold-out, want %d", soldOut.Load(), buyers-stockQty)
	}
	wantStock(t, sku, stock{Available: 0, Reserved: stockQty, Sold: 0})
}

// 同一个幂等键被并发重复提交（用户连点、客户端重试），只能产生一个订单、扣一次库存。
func TestConcurrentDuplicateSubmitCreatesOneOrder(t *testing.T) {
	t.Parallel()
	const attempts = 30
	sku, user, key := newSKU(t, 100), newUser(), newKey()

	ids := make([]string, attempts)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range attempts {
		wg.Go(func() {
			<-start
			r, err := call(t.Context(), http.MethodPost, "/api/v1/orders", user, key, createOrderBody(sku, 2))
			if err != nil {
				t.Errorf("create order: %v", err)
				return
			}
			if r.status != http.StatusCreated {
				t.Errorf("attempt %d: status %d: %s", i, r.status, r.body)
				return
			}
			var o order
			if err := json.Unmarshal(r.body, &o); err != nil {
				t.Errorf("decode: %v", err)
				return
			}
			ids[i] = o.OrderID
		})
	}
	close(start)
	wg.Wait()

	for i, id := range ids {
		if id != ids[0] {
			t.Fatalf("attempt %d got order %q, attempt 0 got %q; want the same order", i, id, ids[0])
		}
	}
	wantStock(t, sku, stock{Available: 98, Reserved: 2, Sold: 0})

	// 同一个键换了内容，是客户端的错误，必须拒绝而不是返回旧订单。
	r := mustCall(t, http.MethodPost, "/api/v1/orders", user, key, createOrderBody(sku, 5))
	if r.status != http.StatusConflict || r.errorCode() != "IDEMPOTENCY_KEY_REUSED" {
		t.Errorf("reused key with different body: status %d code %q, want 409 IDEMPOTENCY_KEY_REUSED", r.status, r.errorCode())
	}
}

// 下单后不支付：超过支付时限后订单被关闭，库存归还。
func TestUnpaidOrderIsClosedAndStockReturned(t *testing.T) {
	t.Parallel()
	sku, user := newSKU(t, 5), newUser()

	o := mustCreateOrder(t, user, sku, 2)
	wantStock(t, sku, stock{Available: 3, Reserved: 2, Sold: 0})

	eventually(t, payTimeout+10*time.Second, "order to be closed", func() bool {
		return getOrder(t, user, o.OrderID).Status == "closed"
	})
	wantStock(t, sku, stock{Available: 5, Reserved: 0, Sold: 0})

	// 关闭后再支付会被拒绝。
	r := mustCall(t, http.MethodPost, "/api/v1/orders/"+o.OrderID+"/pay", user, "", nil)
	if r.status != http.StatusConflict || r.errorCode() != "ORDER_NOT_PAYABLE" {
		t.Errorf("pay closed order: status %d code %q, want 409 ORDER_NOT_PAYABLE", r.status, r.errorCode())
	}
}

// 取消订单归还库存，重复取消仍然成功且只归还一次。
func TestCancelReturnsStockOnce(t *testing.T) {
	t.Parallel()
	sku, user := newSKU(t, 5), newUser()
	o := mustCreateOrder(t, user, sku, 2)

	for range 3 {
		r := mustCall(t, http.MethodPost, "/api/v1/orders/"+o.OrderID+"/cancel", user, "", nil)
		if r.status != http.StatusOK {
			t.Fatalf("cancel: status %d: %s", r.status, r.body)
		}
	}
	wantStock(t, sku, stock{Available: 5, Reserved: 0, Sold: 0})
}

// 支付与取消同时发生。两者是互斥的状态转换，无论谁赢，最终状态必须自洽：
//   - 支付赢：订单已支付，库存已售，支付单成功。
//   - 取消赢：订单已取消，库存归还；若钱已经付了，支付单必须被退款。
//
// 不允许出现“订单取消了但钱没退”或“订单付了但库存被归还”。
func TestPayRacingCancelEndsConsistent(t *testing.T) {
	t.Parallel()
	const orders, perOrder = 40, 1
	sku := newSKU(t, orders*perOrder)

	type entry struct {
		user int64
		id   string
	}
	entries := make([]entry, orders)
	for i := range entries {
		user := newUser()
		entries[i] = entry{user: user, id: mustCreateOrder(t, user, sku, perOrder).OrderID}
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i, e := range entries {
		wg.Go(func() {
			<-start
			if _, err := call(t.Context(), http.MethodPost, "/api/v1/orders/"+e.id+"/pay", e.user, "", nil); err != nil {
				t.Errorf("pay: %v", err)
			}
		})
		wg.Go(func() {
			<-start
			// 模拟支付在受理后几百毫秒内出结果并回调，具体时刻取决于后台任务的节拍。
			// 把取消均匀撒在 0 到 800ms：最早的一定赶在支付结果之前，最晚的一定在之后，
			// 中间的正好撞上，两种结局都会出现。
			time.Sleep(time.Duration(i%9) * 100 * time.Millisecond)
			if _, err := call(t.Context(), http.MethodPost, "/api/v1/orders/"+e.id+"/cancel", e.user, "", nil); err != nil {
				t.Errorf("cancel: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()

	var paid, cancelled int64
	for _, e := range entries {
		var o order
		var p payment
		var hasPayment bool
		// 等到这个订单的所有异步处理都落定：支付单若存在，必须已有最终结果并被订单服务确认。
		eventually(t, 15*time.Second, "order "+e.id+" to settle", func() bool {
			o = getOrder(t, e.user, e.id)
			p, hasPayment = getPayment(t, e.user, e.id)
			if o.Status != "paid" && o.Status != "cancelled" {
				return false
			}
			return !hasPayment || (p.Notified && p.Status != "pending")
		})

		switch o.Status {
		case "paid":
			paid++
			if !hasPayment || p.Status != "succeeded" {
				t.Errorf("order %s is paid but payment is %+v (found=%v)", e.id, p, hasPayment)
			}
		case "cancelled":
			cancelled++
			if hasPayment && p.Status != "refunded" {
				t.Errorf("order %s is cancelled but its payment is %q, want refunded", e.id, p.Status)
			}
		}
	}

	t.Logf("paid=%d cancelled=%d", paid, cancelled)
	// 两种结局都要出现，否则这个测试没有真正覆盖到竞争。
	if paid == 0 || cancelled == 0 {
		t.Errorf("paid=%d cancelled=%d; the race was not exercised in both directions", paid, cancelled)
	}
	wantStock(t, sku, stock{
		Available: cancelled * perOrder,
		Reserved:  0,
		Sold:      paid * perOrder,
	})
}

// 模拟支付失败：订单保持待支付，库存仍被占用，直到超时关闭后归还。
func TestFailedPaymentLeavesOrderPendingUntilClosed(t *testing.T) {
	t.Parallel()
	sku, user := newSKU(t, 4), newUser()
	o := mustCreateOrder(t, user, sku, 1)

	r := mustCall(t, http.MethodPost, "/api/v1/orders/"+o.OrderID+"/pay", user, "", map[string]any{"outcome": "failure"})
	if r.status != http.StatusAccepted {
		t.Fatalf("pay: status %d: %s", r.status, r.body)
	}
	eventually(t, 10*time.Second, "payment to fail", func() bool {
		p, found := getPayment(t, user, o.OrderID)
		return found && p.Status == "failed" && p.Notified
	})
	if got := getOrder(t, user, o.OrderID).Status; got != "pending_payment" && got != "closed" {
		t.Errorf("status after failed payment = %q, want pending_payment (or closed if the deadline passed)", got)
	}

	eventually(t, payTimeout+10*time.Second, "order to be closed", func() bool {
		return getOrder(t, user, o.OrderID).Status == "closed"
	})
	wantStock(t, sku, stock{Available: 4, Reserved: 0, Sold: 0})
}
