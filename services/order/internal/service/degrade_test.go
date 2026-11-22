package service

import (
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/breaker"
	"github.com/palemoky/oppenheimer/pkg/degrade"
	"github.com/palemoky/oppenheimer/pkg/errx"
	"github.com/palemoky/oppenheimer/pkg/idgen"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/stockgate"
)

// 降级（设计文档 1d 第 6 节）：受理开关、闸门的熔断器、落库健康、回收任务在闸门恢复后的等待。

const testCooldown = 5 * time.Second

// degradeService 返回一个熔断参数可控的 Service：连续 failures 次失败打开，冷却 testCooldown（按测试的时钟）。
func degradeService(t *testing.T, e *env, failures int, mod func(d *Deps, c *Config)) *Service {
	t.Helper()
	return e.serviceWith(t, func(d *Deps, c *Config) {
		c.BreakerFailures = failures
		c.BreakerCooldown = testCooldown
		if mod != nil {
			mod(d, c)
		}
	})
}

// usersByDB 返回两个用户号，分别落在 0 号库和 1 号库。
func usersByDB(t *testing.T, e *env) (db0, db1 int64) {
	t.Helper()
	for uid := int64(1); uid < 1000 && (db0 == 0 || db1 == 0); uid++ {
		switch e.router.DB(e.router.ForUser(uid)) {
		case 0:
			db0 = cmpOr(db0, uid)
		case 1:
			db1 = cmpOr(db1, uid)
		}
	}
	if db0 == 0 || db1 == 0 {
		t.Fatal("no users found for both dbs")
	}
	return db0, db1
}

func cmpOr(a, b int64) int64 {
	if a != 0 {
		return a
	}
	return b
}

func submitWith(t *testing.T, svc *Service, userID int64, key string) (*orderv1.Order, error) {
	t.Helper()
	resp, err := svc.SubmitOrder(t.Context(), submitReq(userID, key, items(skuA, 1)))
	return resp.GetOrder(), err
}

func landWith(t *testing.T, e *env, svc *Service, orderID int64) error {
	t.Helper()
	return svc.SubmitHandler(0)(t.Context(), e.submitMessage(t, orderID))
}

// 受理开关关掉：拒绝下单，闸门和消息都没有动；其余接口照常；打开后恢复。
func TestAdmissionSwitch(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "before")
	sent := e.sender.sendCount()

	if err := e.svc.degrade.SetMode(SwitchAdmission, degrade.ModeOff); err != nil {
		t.Fatal(err)
	}
	_, err := submitWith(t, e.svc, 1, "off")
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonFeatureDisabled)
	if e.sender.sendCount() != sent {
		t.Error("a message was sent for a rejected order")
	}
	e.wantGate(t, skuA, 998, 2)
	mustGetStatus(t, e.svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)

	if err := e.svc.degrade.SetMode(SwitchAdmission, degrade.ModeAuto); err != nil {
		t.Fatal(err)
	}
	// 被拒绝时幂等键没有写进去，同一个键可以重新下单。
	if _, err := submitWith(t, e.svc, 1, "off"); err != nil {
		t.Fatalf("submit after switching admission back on: %v", err)
	}
}

// 闸门挂住：前几个请求各等一次时限，熔断器打开之后立刻被拒绝，已落库的订单查询也不再等。
func TestGateBreakerFailsFast(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.mustPending(t, 1, "landed")
	sent := e.sender.sendCount()
	svc := degradeService(t, e, 2, func(d *Deps, _ *Config) {
		d.Gate = stockgate.New(blackHoleRedis(t), e.prefix)
	})

	// 第一个请求：Hold 超时一次、撤销超时一次，正好两次失败，熔断器打开。
	start := time.Now()
	_, err := submitWith(t, svc, 1, "slow")
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonStockGateUnavailable)
	if elapsed := time.Since(start); elapsed < gateTimeout {
		t.Fatalf("first submit took %s, want at least one gate timeout (%s)", elapsed, gateTimeout)
	}
	if got := svc.gateBreaker.State(); got != breaker.Open {
		t.Fatalf("gate breaker = %s after two timeouts, want open", got)
	}

	// 之后：不碰 Redis，立刻返回。
	const fast = 100 * time.Millisecond
	start = time.Now()
	for i := range 20 {
		_, err := submitWith(t, svc, 1, "fast")
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonStockGateUnavailable)
		if i == 0 {
			mustGetStatus(t, svc, o.GetOrderId(), 1, orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
		}
	}
	if elapsed := time.Since(start); elapsed > fast {
		t.Fatalf("20 submits and a query with the breaker open took %s, want under %s", elapsed, fast)
	}
	// 没落库的订单：分不清在排队还是不存在。
	_, err = getOrder(t, svc, 424242, 1)
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonStockGateUnavailable)
	if e.sender.sendCount() != sent {
		t.Error("messages were sent while the gate was unavailable")
	}

	// 冷却之后放一个探测：Redis 还是挂着，探测失败，继续打开。
	e.clock.Advance(testCooldown)
	start = time.Now()
	_, err = submitWith(t, svc, 1, "probe")
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonStockGateUnavailable)
	if elapsed := time.Since(start); elapsed < gateTimeout {
		t.Fatalf("probe took %s, want one gate timeout", elapsed)
	}
	start = time.Now()
	_, err = submitWith(t, svc, 1, "after-probe")
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonStockGateUnavailable)
	if elapsed := time.Since(start); elapsed > fast {
		t.Fatalf("submit after a failed probe took %s, want under %s", elapsed, fast)
	}
}

// 闸门恢复：半开的探测成功，受理恢复正常。
func TestGateBreakerRecovers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := degradeService(t, e, 3, nil)
	e.mustPending(t, 1, "load") // 把 skuA 装进闸门
	for range 3 {
		svc.gateBreaker.Report(false)
	}
	_, err := submitWith(t, svc, 1, "k")
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonStockGateUnavailable)

	e.clock.Advance(testCooldown)
	o, err := submitWith(t, svc, 1, "k")
	if err != nil {
		t.Fatalf("submit after the cooldown with a healthy gate: %v", err)
	}
	if got := svc.gateBreaker.State(); got != breaker.Closed {
		t.Fatalf("gate breaker = %s after a successful probe, want closed", got)
	}
	if err := landWith(t, e, svc, o.GetOrderId()); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
}

// 落库时闸门不可用：消息不被丢弃、不进死信，每次都返回可重试的错误；闸门恢复后落库，只有一个订单。
func TestLandingWaitsForGate(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	o := e.submit(t, 1, "queued", items(skuA, 1))
	rows := e.orderRows(t)
	bad := degradeService(t, e, 2, func(d *Deps, _ *Config) {
		d.Gate = stockgate.New(blackHoleRedis(t), e.prefix)
	})

	// 多于默认的 10 次：消费者不设重试上限（cmd/order），这里确认 handler 每一次都给出“稍后重试”。
	for i := range 15 {
		err := landWith(t, e, bad, o.GetOrderId())
		if err == nil || kafkax.IsPermanent(err) {
			t.Fatalf("attempt %d with the gate down: err = %v, want a retryable error", i, err)
		}
	}
	if got := e.orderRows(t); got != rows {
		t.Fatalf("order rows = %d while the gate is down, want %d", got, rows)
	}
	if h, ok := e.hold(t, o.GetOrderId()); !ok || h.Claimed {
		t.Fatalf("hold = %+v, exists = %v; want it untouched and unclaimed", h, ok)
	}
	// 闸门的故障不记到落库健康上：它有自己的熔断器。
	for i, h := range bad.landing {
		if h.b.State() != breaker.Closed {
			t.Errorf("landing breaker of db %d = %s, want closed", i, h.b.State())
		}
	}

	// 恢复（用好的闸门处理同一条消息，重复两次）。
	e.mustLand(t, o.GetOrderId())
	e.mustLand(t, o.GetOrderId())
	wantStatus(t, e.load(t, o.GetOrderId()), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if got := e.orderRows(t); got != rows+1 {
		t.Fatalf("order rows = %d after recovery, want %d", got, rows+1)
	}
}

// 库存服务不可用：落库连续失败之后受理被拒绝；落库重试成功后受理恢复。
func TestLandingHealthRejectsAdmission(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := degradeService(t, e, 3, nil)
	u0, u1 := usersByDB(t, e)

	var queued []*orderv1.Order
	for i := range 3 {
		o, err := submitWith(t, svc, u0, "q"+string(rune('a'+i)))
		if err != nil {
			t.Fatal(err)
		}
		queued = append(queued, o)
	}
	for _, o := range queued {
		e.inv.inject("Reserve", fault{err: errTimeout})
		if err := landWith(t, e, svc, o.GetOrderId()); err == nil || kafkax.IsPermanent(err) {
			t.Fatalf("land with inventory down: err = %v, want a retryable error", err)
		}
	}
	if got := svc.landing[0].b.State(); got != breaker.Open {
		t.Fatalf("landing breaker of db 0 = %s after 3 failed landings, want open", got)
	}

	sent := e.sender.sendCount()
	_, err := submitWith(t, svc, u0, "rejected")
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderLandingUnavailable)
	if e.sender.sendCount() != sent {
		t.Error("a message was sent for a rejected order")
	}
	// 被拒绝的请求没有碰闸门：占用还是那 3 个。
	e.wantGate(t, skuA, 997, 3)
	// 这是按订单库判断的：另一个库没有失败过，它的用户照常下单。
	if svc.landing[1].b.State() != breaker.Closed {
		t.Fatalf("landing breaker of db 1 = %s, want closed", svc.landing[1].b.State())
	}
	if _, err := submitWith(t, svc, u1, "other-db"); err != nil {
		t.Fatalf("submit for a user on the healthy db: %v", err)
	}

	// 消费者一直在重试排队的消息。库存服务恢复后第一次重试成功，受理立刻恢复，不用等冷却。
	if err := landWith(t, e, svc, queued[0].GetOrderId()); err != nil {
		t.Fatal(err)
	}
	if got := svc.landing[0].b.State(); got != breaker.Closed {
		t.Fatalf("landing breaker of db 0 = %s after a successful landing, want closed", got)
	}
	if _, err := submitWith(t, svc, u0, "rejected"); err != nil {
		t.Fatalf("submit after landing recovered: %v", err)
	}
}

// 在途的落库一起超时（依赖停顿了一下）不打开落库健康：失败要持续够 LandingFailureSpan 才算依赖坏了。
// 集群上落库是上百个并发在途，光数次数的话一次停顿就凑满了“连续 N 次”，受理被白白拒绝（3b 第 3 轮）。
func TestLandingHealthIgnoresBurstOfFailures(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := degradeService(t, e, 3, func(_ *Deps, c *Config) { c.LandingFailureSpan = time.Second })
	u0, _ := usersByDB(t, e)

	var queued []*orderv1.Order
	for i := range 5 {
		o, err := submitWith(t, svc, u0, "q"+string(rune('a'+i)))
		if err != nil {
			t.Fatal(err)
		}
		queued = append(queued, o)
	}
	failAll := func() {
		t.Helper()
		for _, o := range queued {
			e.inv.inject("Reserve", fault{err: errTimeout})
			if err := landWith(t, e, svc, o.GetOrderId()); err == nil || kafkax.IsPermanent(err) {
				t.Fatalf("land with inventory down: err = %v, want a retryable error", err)
			}
		}
	}

	// 同一刻的 5 次失败：次数够了，时间不够，不打开，受理照常。
	failAll()
	if got := svc.landing[0].b.State(); got != breaker.Closed {
		t.Fatalf("landing breaker = %s after a burst of failures at one instant, want closed", got)
	}
	if _, err := submitWith(t, svc, u0, "during-burst"); err != nil {
		t.Fatalf("submit right after a burst of landing failures: %v", err)
	}

	// 停顿过去，下一次落库成功，这一串失败就不算了：一秒之后再来一批同时的失败，仍然不打开。
	if err := landWith(t, e, svc, queued[0].GetOrderId()); err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(time.Second)
	queued = queued[1:]
	failAll()
	if got := svc.landing[0].b.State(); got != breaker.Closed {
		t.Fatalf("landing breaker = %s after a second burst separated by a success, want closed", got)
	}

	// 依赖真的坏了：消费者重试，一秒之后还在失败，打开。
	e.clock.Advance(time.Second)
	failAll()
	if got := svc.landing[0].b.State(); got != breaker.Open {
		t.Fatalf("landing breaker = %s after failures lasting a second, want open", got)
	}
	_, err := submitWith(t, svc, u0, "rejected")
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderLandingUnavailable)
}

// 落库健康打开期间，什么依赖都没碰就结束的消息不能把它关上：
// 被 sweeper 置为失败的订单重试结束、占用已经不在的消息被丢弃。
func TestLandingHealthIgnoresMessagesThatExerciseNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := degradeService(t, e, 2, nil)
	sweeper, err := NewSweeper(svc, SweeperConfig{})
	if err != nil {
		t.Fatal(err)
	}
	u0, _ := usersByDB(t, e)

	var ids []int64
	for _, key := range []string{"a", "b"} {
		o, err := submitWith(t, svc, u0, key)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, o.GetOrderId())
		e.inv.inject("Reserve", fault{err: errTimeout})
		if err := landWith(t, e, svc, o.GetOrderId()); err == nil {
			t.Fatal("land with inventory down succeeded")
		}
	}
	if got := svc.landing[0].b.State(); got != breaker.Open {
		t.Fatalf("landing breaker = %s, want open", got)
	}

	// 创建超时：sweeper 把两个订单置为失败。之后消费者的重试看到终态，直接结束（这就是不设重试上限时
	// 消息的终点），但它没有调用库存服务，不能算“落库恢复了”。
	e.clock.Advance(DefaultCreateTimeout + time.Second)
	sweeper.RunOnce(t.Context())
	wantStatus(t, e.load(t, ids[0]), orderv1.OrderStatus_ORDER_STATUS_FAILED)
	if err := landWith(t, e, svc, ids[0]); err != nil {
		t.Fatalf("retry of a failed order: %v, want nil (message done)", err)
	}
	// 库存服务（由 consumeEvents 扮演）处理 failed 事件、归还占用之后，另一条消息的重试认领不到占用，被丢弃。
	e.consumeEvents(t)
	if err := landWith(t, e, svc, ids[1]); err != nil {
		t.Fatalf("retry after the hold was returned: %v, want nil (message dropped)", err)
	}
	// 时钟走过了冷却时间，所以是“打开”还是“还没人来探测的打开”都算；关键是没有被关上。
	if got := svc.landing[0].b.State(); got == breaker.Closed {
		t.Fatal("landing breaker closed by messages that did not call inventory")
	}
}

// 落库健康的半开：没有在重试的消息时，放一个下单请求进去当探测。
func TestLandingHealthProbe(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	u0, _ := usersByDB(t, e)

	open := func(t *testing.T, cooldown time.Duration) *Service {
		t.Helper()
		svc := e.serviceWith(t, func(_ *Deps, c *Config) {
			c.BreakerFailures = 2
			c.BreakerCooldown = cooldown
		})
		svc.landing[0].b.Report(false)
		svc.landing[0].b.Report(false)
		_, err := submitWith(t, svc, u0, t.Name()+"-open")
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderLandingUnavailable)
		e.clock.Advance(cooldown)
		return svc
	}

	t.Run("probe lands", func(t *testing.T) {
		svc := open(t, testCooldown)
		probe, err := submitWith(t, svc, u0, t.Name()+"-probe")
		if err != nil {
			t.Fatalf("probe order rejected: %v", err)
		}
		// 探测的结果还没回来：其余的请求仍然被拒绝。
		_, err = submitWith(t, svc, u0, t.Name()+"-second")
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderLandingUnavailable)
		if err := landWith(t, e, svc, probe.GetOrderId()); err != nil {
			t.Fatal(err)
		}
		if _, err := submitWith(t, svc, u0, t.Name()+"-second"); err != nil {
			t.Fatalf("submit after the probe landed: %v", err)
		}
	})

	t.Run("probe fails to land", func(t *testing.T) {
		svc := open(t, testCooldown)
		probe, err := submitWith(t, svc, u0, t.Name()+"-probe")
		if err != nil {
			t.Fatalf("probe order rejected: %v", err)
		}
		e.inv.inject("Reserve", fault{err: errTimeout})
		if err := landWith(t, e, svc, probe.GetOrderId()); err == nil {
			t.Fatal("land with inventory down succeeded")
		}
		if got := svc.landing[0].b.State(); got != breaker.Open {
			t.Fatalf("landing breaker = %s after the probe failed to land, want open", got)
		}
	})

	t.Run("probe not admitted", func(t *testing.T) {
		svc := open(t, testCooldown)
		// 探测的那个请求因为业务原因没受理成（买 2 件只有 1 件的）：名额让给下一个请求。
		_, err := svc.SubmitOrder(t.Context(), submitReq(u0, t.Name()+"-scarce", items(skuScarce, 2)))
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonInsufficientStock)
		if _, err := submitWith(t, svc, u0, t.Name()+"-next"); err != nil {
			t.Fatalf("next submit after a probe that was not admitted: %v", err)
		}
	})

	t.Run("no verdict", func(t *testing.T) {
		// 探测的订单发出去了，但落库的结果没有回到这个实例（分区在别的实例上）：
		// 一个冷却时间内没有失败报上来就当成功，不能永远停在半开。
		svc := open(t, testCooldown)
		if _, err := submitWith(t, svc, u0, t.Name()+"-probe"); err != nil {
			t.Fatalf("probe order rejected: %v", err)
		}
		e.clock.Advance(testCooldown - time.Second)
		_, err := submitWith(t, svc, u0, t.Name()+"-early")
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderLandingUnavailable)
		e.clock.Advance(time.Second)
		if _, err := submitWith(t, svc, u0, t.Name()+"-late"); err != nil {
			t.Fatalf("submit a cooldown after the probe was published: %v", err)
		}
		if got := svc.landing[0].b.State(); got != breaker.Closed {
			t.Fatalf("landing breaker = %s, want closed", got)
		}
	})

	t.Run("no verdict after a failure", func(t *testing.T) {
		// 探测的订单落库失败，熔断器重新打开；那个探测的“超时当成功”不能再把它关上。
		svc := open(t, testCooldown)
		probe, err := submitWith(t, svc, u0, t.Name()+"-probe")
		if err != nil {
			t.Fatalf("probe order rejected: %v", err)
		}
		e.inv.inject("Reserve", fault{err: errTimeout})
		if err := landWith(t, e, svc, probe.GetOrderId()); err == nil {
			t.Fatal("land with inventory down succeeded")
		}
		// 消费者还在不断重试、不断失败，冷却一直被重新算起。
		e.clock.Advance(testCooldown - time.Second)
		svc.landing[0].b.Report(false)
		e.clock.Advance(time.Second)
		_, err = submitWith(t, svc, u0, t.Name()+"-late")
		wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderLandingUnavailable)
	})
}

// 闸门恢复之后的半个宽限期内，回收任务不回收未认领的占用：排队的订单在故障期间没法被认领，
// 等待时长不算数。之后照常回收。
func TestReaperWaitsAfterGateRecovery(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := degradeService(t, e, 3, nil)
	reaper, err := NewReaper(svc, ReaperConfig{})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := submitWith(t, svc, 1, "queued")
	if err != nil {
		t.Fatal(err)
	}

	// 闸门不可用，持续超过宽限期。
	for range 3 {
		svc.gateBreaker.Report(false)
	}
	if st := reaper.RunOnce(t.Context()); st != (ReapStats{}) {
		t.Fatalf("stats with the gate breaker open = %+v, want nothing", st)
	}
	e.clock.Advance(DefaultReapGrace + time.Second)

	// 恢复：这一轮的列占用就是探测，成功了；排队的那个订单早就过了宽限期，但不回收。
	if st := reaper.RunOnce(t.Context()); st.Returned != 0 {
		t.Fatalf("returned %d holds right after the gate recovered, want 0", st.Returned)
	}
	if got := svc.gateBreaker.State(); got != breaker.Closed {
		t.Fatalf("gate breaker = %s, want closed", got)
	}
	e.clock.Advance(DefaultReapGrace/2 - time.Second)
	if st := reaper.RunOnce(t.Context()); st.Returned != 0 {
		t.Fatalf("returned %d holds within the hold-off, want 0", st.Returned)
	}
	// 这期间消费者赶上来把它落库了：占用被认领，回收任务以后也不会碰它。
	if err := landWith(t, e, svc, queued.GetOrderId()); err != nil {
		t.Fatal(err)
	}

	// 一个真正的孤儿：等待结束后照常回收。
	orphan, err := submitWith(t, svc, 1, "orphan")
	if err != nil {
		t.Fatal(err)
	}
	e.clock.Advance(DefaultReapGrace + time.Second)
	st := reaper.RunOnce(t.Context())
	if st.Returned != 1 {
		t.Fatalf("stats after the hold-off = %+v, want 1 returned", st)
	}
	if _, ok := e.hold(t, orphan.GetOrderId()); ok {
		t.Error("orphan hold still exists")
	}
	if h, ok := e.hold(t, queued.GetOrderId()); !ok || !h.Claimed {
		t.Error("landed order's hold was touched")
	}
}

// 回收任务自己发现闸门不可用（本进程里没有别的调用去喂熔断器）：恢复后同样先等。
func TestReaperWaitsAfterItsOwnFailure(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	orphan := e.submit(t, 1, "orphan", items(skuA, 1))
	e.clock.Advance(DefaultReapGrace + time.Second)

	e.reaper.failing = true // 上一轮列占用失败
	if st := e.reaper.RunOnce(t.Context()); st.Returned != 0 {
		t.Fatalf("returned %d holds in the first round after a failure, want 0", st.Returned)
	}
	e.clock.Advance(DefaultReapGrace / 2)
	if st := e.reaper.RunOnce(t.Context()); st.Returned != 1 {
		t.Fatalf("stats after the hold-off = %+v, want 1 returned", st)
	}
	if _, ok := e.hold(t, orphan.GetOrderId()); ok {
		t.Error("orphan hold still exists")
	}
}

// GET /degrade 上受理这一项的状态。
func TestAdmissionStatus(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := degradeService(t, e, 1, nil)
	view := func() degrade.View {
		t.Helper()
		for _, v := range svc.degrade.Snapshot() {
			if v.Name == SwitchAdmission {
				return v
			}
		}
		t.Fatal("no admission switch")
		return degrade.View{}
	}
	if v := view(); v.Degraded || v.Mode != degrade.ModeAuto {
		t.Fatalf("healthy: %+v, want auto and not degraded", v)
	}
	svc.landing[1].b.Report(false)
	if v := view(); !v.Degraded {
		t.Fatalf("with a landing breaker open: %+v, want degraded", v)
	}
	svc.landing[1].b.Report(true)
	svc.gateBreaker.Report(false)
	if v := view(); !v.Degraded {
		t.Fatalf("with the gate breaker open: %+v, want degraded", v)
	}
}

// expiredIDs 模拟节点号的租约续不上（ADR-0028）。
type expiredIDs struct{}

func (expiredIDs) Next(int64) (int64, error) { return 0, idgen.ErrLeaseExpired }

// 节点号的租约过期时受理按降级拒绝（FAILED_PRECONDITION 加原因码，不是 INTERNAL，免得调用方的边车把 order 摘掉），
// 而且什么都没有发生：没有占用，没有消息。
func TestSubmitRejectedWhenIDLeaseExpired(t *testing.T) {
	e := newEnvWith(t, envOptions{ids: expiredIDs{}})
	_, err := e.svc.SubmitOrder(t.Context(), submitReq(1, "lease-expired", items(skuA, 1)))
	wantErr(t, err, codes.FailedPrecondition, errx.ReasonOrderLandingUnavailable)
	if !errx.IsDegraded(err) {
		t.Errorf("err = %v, want a degraded error", err)
	}
	if n := e.sender.sendCount(); n != 0 {
		t.Errorf("%d messages sent for a rejected order", n)
	}
	holds, err := e.gate.HoldsBefore(t.Context(), time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(holds) != 0 {
		t.Errorf("holds %v left behind by a rejected order", holds)
	}
}
