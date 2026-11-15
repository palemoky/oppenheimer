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

	"github.com/palemoky/oppenheimer/pkg/mysqlx"
)

const (
	gatewayEnv        = "E2E_GATEWAY_URL"
	inventoryDSNEnv   = "E2E_INVENTORY_DSN"
	paymentTimeoutEnv = "E2E_PAYMENT_TIMEOUT"
	waitReadyEnv      = "E2E_WAIT_READY"

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

	// 链路的接收端：服务把 span 导出到这里，trace_test.go 据此断言。没设就不起，对应的用例跳过。
	if ep := os.Getenv(otlpAddrEnv); ep != "" {
		otlp, err = startCollector(ep)
		if err != nil {
			fmt.Fprintf(os.Stderr, "start OTLP receiver: %v\n", err)
			return 1
		}
		defer otlp.stop()
	}

	// 集群上（scripts/cluster/e2e.sh）测试 Job 先于各服务启动：接收端先监听，服务一起来就连得上，
	// 不会因为连不上而退避到几十秒之后才重连，丢掉 trace_test.go 要断言的 span。所以要在这里等网关就绪。
	// 本机的 make e2e 不设这个变量，行为不变。
	if v := os.Getenv(waitReadyEnv); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse %s: %v\n", waitReadyEnv, err)
			return 1
		}
		if err := waitGatewayReady(d); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}

	// 以时间为基数，重复运行时不会撞上之前的 SKU 和用户。
	base := time.Now().UnixMilli() % 1_000_000_000 * 1000
	nextSKU.Store(base)
	nextUser.Store(base)

	return m.Run()
}

// waitGatewayReady 轮询网关的 /readyz，直到连续 3 次 200 或超过 d。连续几次是因为入口刚挂上路由、
// 第二个副本还没就绪时结果会来回变。
func waitGatewayReady(d time.Duration) error {
	deadline := time.Now().Add(d)
	ok, last := 0, ""
	for ok < 3 {
		if time.Now().After(deadline) {
			return fmt.Errorf("gateway at %s not ready within %s (last: %s)", baseURL, d, last)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/readyz", nil)
		if err != nil {
			cancel()
			return fmt.Errorf("build readiness request: %w", err)
		}
		resp, err := client.Do(req)
		if err != nil {
			ok, last = 0, err.Error()
		} else {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ok++
			} else {
				ok, last = 0, resp.Status
			}
		}
		cancel()
		time.Sleep(time.Second)
	}
	fmt.Printf("gateway at %s is ready\n", baseURL)
	return nil
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
	header http.Header
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
	return response{status: resp.StatusCode, header: resp.Header, body: b}, nil
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

// mustCreateOrder 提交订单并等它落库。
//
// 下单是异步的：提交只表示请求通过了库存闸门并进入队列（202，状态 creating），
// 订单随后才落库并在 MySQL 里预占库存。这里轮询到状态离开 creating，
// 返回的是已经可以支付的订单，调用方不用关心中间状态。
func mustCreateOrder(t testing.TB, userID, skuID int64, quantity int) order {
	t.Helper()
	id, err := submitOrder(t.Context(), userID, skuID, quantity)
	if err != nil {
		t.Fatal(err)
	}
	return waitLanded(t, userID, id)
}

// submitOrder 提交订单，返回受理（202，状态 creating）的订单号，不等落库。
// 不调用 t.Fatal，可以在并发提交的 goroutine 里使用。
func submitOrder(ctx context.Context, userID, skuID int64, quantity int) (string, error) {
	r, err := call(ctx, http.MethodPost, "/api/v1/orders", userID, newKey(), createOrderBody(skuID, quantity))
	if err != nil {
		return "", fmt.Errorf("create order: %w", err)
	}
	if r.status != http.StatusAccepted {
		return "", fmt.Errorf("create order: status %d: %s", r.status, r.body)
	}
	var o order
	if err := json.Unmarshal(r.body, &o); err != nil {
		return "", fmt.Errorf("decode accepted order: %w: %s", err, r.body)
	}
	if o.Status != "creating" {
		return "", fmt.Errorf("accepted order %s status = %q, want creating", o.OrderID, o.Status)
	}
	return o.OrderID, nil
}

// waitLanded 轮询到订单离开 creating，要求它落库后处于 pending_payment。
func waitLanded(t testing.TB, userID int64, id string) order {
	t.Helper()
	var o order
	eventually(t, 10*time.Second, "order "+id+" to leave creating", func() bool {
		o = getOrder(t, userID, id)
		return o.Status != "creating"
	})
	if o.Status != "pending_payment" {
		t.Fatalf("order %s became %q, want pending_payment", id, o.Status)
	}
	return o
}

// getOrder 查订单。带 Retry-After 的 503 按接口的约定处理：稍后再试，最多等 10 秒。
//
// 这个 503 在一切正常时也会出现：订单还没落库、查询又没占到读闸门占用的名额（READ_GATE_MAX_CONCURRENCY）时，
// 服务说不清订单的状态，答的是 STOCK_GATE_UNAVAILABLE 加 Retry-After，而不是猜一个 404。并行的用例一起轮询
// 就可能碰上（CI 上出现过，PROGRESS 的 2026-11-15）。正确的客户端应当重试，这里照做；
// 不看 Retry-After 的秒数（固定 5 秒，是给真实客户端的），隔 100 毫秒再问。别的非 200 仍然立刻失败。
func getOrder(t testing.TB, userID int64, orderID string) order {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var r response
	for {
		r = mustCall(t, http.MethodGet, "/api/v1/orders/"+orderID, userID, "", nil)
		retryable := r.status == http.StatusServiceUnavailable && r.header.Get("Retry-After") != ""
		if !retryable || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if r.status != http.StatusOK {
		t.Fatalf("get order %s: status %d: %s", orderID, r.status, r.body)
	}
	var o order
	r.decode(t, &o)
	return o
}

func getStock(t testing.TB, skuID int64) stock {
	t.Helper()
	// 每次用新的用户号：网关按用户加接口限流（getStock 默认 50/s），所有用例、所有轮询共用一个用户号会共用一个桶，
	// 并行的用例一起轮询库存就会被限住。
	r := mustCall(t, http.MethodGet, "/api/v1/skus/"+strconv.FormatInt(skuID, 10)+"/stock", newUser(), "", nil)
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

// newLimitedSKU 插入一个每人限购 limit 件的 SKU。
func newLimitedSKU(t testing.TB, available, limit int64) int64 {
	t.Helper()
	id := newSKU(t, available)
	if _, err := inventoryDB.ExecContext(t.Context(),
		"UPDATE sku_stock SET purchase_limit = ? WHERE sku_id = ?", limit, id); err != nil {
		t.Fatalf("set purchase limit: %v", err)
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
			case r.status == http.StatusAccepted:
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
	// 同一个用户的下单有按用户的限流（createOrder 默认 5/s、桶 10，限流在幂等之前，被限流的请求不会走到下单逻辑）。
	// 并发发得太多，超出桶容量的请求会得到 429，而这个用例断言的是“每个请求都得到同一个订单”。
	// 所以只发 8 个：加上最后一次换内容的请求共 9 个，不超过桶容量 10，任何一个都不会被限流；8 个并发足以打到去重的竞争。
	const attempts = 8
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
			if r.status != http.StatusAccepted {
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
//   - 关单赢：支付时限（e2e 里只有几秒）从受理时刻算起，到期的延迟消息也在和支付、取消抢同一个
//     PENDING_PAYMENT。机器慢时最早受理的订单可能在支付结果和取消到达之前就被关闭。
//     要求与取消相同：库存归还；若钱已经付了，支付单必须被退款。
//
// 不允许出现“订单取消或关闭了但钱没退”或“订单付了但库存被归还”。
//
// 订单并发提交、再一起等落库，尽量少占用支付时限，让大部分订单留给支付与取消去竞争。
// 逐个提交并等落库要两到三秒，几乎耗尽时限，最早的订单会在竞争开始前就被关单。
func TestPayRacingCancelEndsConsistent(t *testing.T) {
	t.Parallel()
	const orders, perOrder = 40, 1
	sku := newSKU(t, orders*perOrder)

	type entry struct {
		user int64
		id   string
	}
	entries := make([]entry, orders)
	var submit sync.WaitGroup
	for i := range entries {
		entries[i].user = newUser()
		submit.Go(func() {
			id, err := submitOrder(t.Context(), entries[i].user, sku, perOrder)
			if err != nil {
				t.Error(err)
				return
			}
			entries[i].id = id
		})
	}
	submit.Wait()
	if t.Failed() {
		t.FailNow()
	}
	for _, e := range entries {
		waitLanded(t, e.user, e.id)
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
			// 模拟支付在受理后几百毫秒内出结果并回调，具体时刻取决于后台任务的节拍和机器的快慢。
			// 把取消均匀撒在 0 到 2s：最早的一定赶在支付结果之前，最晚的一定在之后，
			// 中间的正好撞上，两种结局都会出现。
			// 窗口原来是 0 到 800ms。CI 上支付结果要六七百毫秒才到，40 个订单里只有 5 到 16 个能走到已支付；
			// 加了 cdc、orderindex 两个进程和几个并行用例之后，有一次全部晚于 800ms，一个已支付都没有。
			// 最晚的取消仍然早于 3s 的支付时限；即使个别订单先被超时关单，也只是记为 closed，不影响断言。
			time.Sleep(time.Duration(i%9) * 250 * time.Millisecond)
			if _, err := call(t.Context(), http.MethodPost, "/api/v1/orders/"+e.id+"/cancel", e.user, "", nil); err != nil {
				t.Errorf("cancel: %v", err)
			}
		})
	}
	close(start)
	wg.Wait()

	var paid, cancelled, closed int64
	for _, e := range entries {
		var o order
		var p payment
		var hasPayment bool
		// 等到这个订单的所有异步处理都落定：支付单若存在，必须已有最终结果并被订单服务确认。
		eventually(t, 15*time.Second, "order "+e.id+" to settle", func() bool {
			o = getOrder(t, e.user, e.id)
			p, hasPayment = getPayment(t, e.user, e.id)
			switch o.Status {
			case "paid", "cancelled", "closed":
			default:
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
		case "cancelled", "closed":
			if o.Status == "cancelled" {
				cancelled++
			} else {
				closed++
			}
			if hasPayment && p.Status != "refunded" {
				t.Errorf("order %s is %s but its payment is %q, want refunded", e.id, o.Status, p.Status)
			}
		}
	}

	t.Logf("paid=%d cancelled=%d closed=%d", paid, cancelled, closed)
	// 支付与取消两种结局都要出现，否则这个测试没有真正覆盖到它们之间的竞争。关单可以出现，但不算数。
	if paid == 0 || cancelled == 0 {
		t.Errorf("paid=%d cancelled=%d closed=%d; the pay/cancel race was not exercised in both directions",
			paid, cancelled, closed)
	}
	wantStock(t, sku, stock{
		Available: (cancelled + closed) * perOrder,
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

// 限购：每人最多买 2 件。买满后再买被当场拒绝；取消后额度恢复；别的用户不受影响。
func TestPurchaseLimit(t *testing.T) {
	t.Parallel()
	sku, user := newLimitedSKU(t, 10, 2), newUser()

	first := mustCreateOrder(t, user, sku, 2)

	r := mustCall(t, http.MethodPost, "/api/v1/orders", user, newKey(), createOrderBody(sku, 1))
	if r.status != http.StatusConflict || r.errorCode() != "PURCHASE_LIMIT_EXCEEDED" {
		t.Fatalf("over limit: status %d code %q, want 409 PURCHASE_LIMIT_EXCEEDED", r.status, r.errorCode())
	}
	// 被闸门拒绝的请求没有碰库存。
	wantStock(t, sku, stock{Available: 8, Reserved: 2, Sold: 0})

	mustCreateOrder(t, newUser(), sku, 2)

	if r := mustCall(t, http.MethodPost, "/api/v1/orders/"+first.OrderID+"/cancel", user, "", nil); r.status != http.StatusOK {
		t.Fatalf("cancel: status %d: %s", r.status, r.body)
	}
	// 额度的恢复是异步的：库存服务消费取消事件，先释放 MySQL 的预占，再归还闸门里的占用。
	// 这里每 50ms 下一次单，会超过 createOrder 的限额（5/s、桶 10）：被限流的请求得到 429，同样不是 202，
	// 下一轮再试，令牌补充之后总能试到；429 的请求没有走到下单逻辑，不会误占额度。
	eventually(t, 10*time.Second, "purchase quota to be restored after cancel", func() bool {
		r := mustCall(t, http.MethodPost, "/api/v1/orders", user, newKey(), createOrderBody(sku, 2))
		return r.status == http.StatusAccepted
	})
	wantStock(t, sku, stock{Available: 6, Reserved: 4, Sold: 0})
}
