package e2e

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// 一次下单加支付的链路要贯穿所有服务（设计文档 1d 第 8.4 到 8.6 节）。
// 服务把 span 导出到测试进程里的接收端（otlp_test.go），这里按 trace id 取出来断言。
func TestTraceSpansAllServices(t *testing.T) {
	if otlp == nil {
		t.Skipf("%s is not set", otlpAddrEnv)
	}
	t.Parallel()
	sku, user := newSKU(t, 10), newUser()

	r := mustCall(t, http.MethodPost, "/api/v1/orders", user, newKey(), createOrderBody(sku, 1))
	if r.status != http.StatusAccepted {
		t.Fatalf("create order: status %d: %s", r.status, r.body)
	}
	var o order
	r.decode(t, &o)
	orderTrace := r.header.Get("X-Trace-Id")
	if orderTrace == "" {
		t.Fatal("create order: no X-Trace-Id header")
	}
	waitLanded(t, user, o.OrderID)

	pr := mustCall(t, http.MethodPost, "/api/v1/orders/"+o.OrderID+"/pay", user, "", nil)
	if pr.status != http.StatusAccepted {
		t.Fatalf("pay: status %d: %s", pr.status, pr.body)
	}
	payTrace := pr.header.Get("X-Trace-Id")
	if payTrace == "" {
		t.Fatal("pay: no X-Trace-Id header")
	}
	eventually(t, 10*time.Second, "order to become paid", func() bool {
		return getOrder(t, user, o.OrderID).Status == "paid"
	})

	// span 在服务里批量异步导出，轮询到全部断言成立或到时限。
	var problems []string
	deadline := time.Now().Add(20 * time.Second)
	for {
		problems = checkOrderTrace(otlp.trace(orderTrace))
		problems = append(problems, checkPaymentTraces(o.OrderID, payTrace)...)
		if len(problems) == 0 {
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Logf("order trace %s:\n%s", orderTrace, dumpSpans(otlp.trace(orderTrace)))
	t.Logf("pay trace %s:\n%s", payTrace, dumpSpans(otlp.trace(payTrace)))
	for _, p := range problems {
		t.Error(p)
	}
	// 其余带这个订单号的链路（经 outbox 延续出去的部分、延迟关单的 link 等），记下来看结构。
	var rest []recvSpan
	for _, s := range spansOfOrder(o.OrderID) {
		if s.traceID != orderTrace && s.traceID != payTrace && !strings.HasPrefix(s.name, "mysql ") && !hasAttr(s, dbKeys, "redis") && !strings.HasPrefix(s.name, "GET ") && !strings.Contains(s.name, "/GetOrder") {
			rest = append(rest, s)
		}
	}
	t.Logf("other traces mentioning order %s (mysql, redis and GET spans left out):\n%s", o.OrderID, dumpSpans(rest))
}

func hasAttr(s recvSpan, keys []string, val string) bool {
	for _, k := range keys {
		if v, ok := s.attrs[k]; ok && (val == "" || v == val) {
			return true
		}
	}
	return false
}

var (
	rpcKeys   = []string{"rpc.system", "rpc.system.name"}
	dbKeys    = []string{"db.system.name", "db.system"}
	topicKeys = []string{"messaging.destination.name"}
)

func isTopic(s recvSpan, kind tracepb.Span_SpanKind, topic string) bool {
	return s.kind == kind && strings.HasSuffix(s.attrs[topicKeys[0]], topic) && hasAttr(s, []string{"messaging.system"}, "kafka")
}

// checkOrderTrace 检查下单那条链路，返回不满足的项。
func checkOrderTrace(spans []recvSpan) []string {
	var problems []string
	fail := func(f string, a ...any) { problems = append(problems, "order trace: "+fmt.Sprintf(f, a...)) }
	if len(spans) == 0 {
		return []string{"order trace: no spans received"}
	}

	services := map[string]bool{}
	byID := map[string]recvSpan{}
	for _, s := range spans {
		services[s.service] = true
		byID[s.spanID] = s
	}
	for _, want := range []string{"gateway", "order", "inventory"} {
		if !services[want] {
			fail("no span from service %q", want)
		}
	}

	has := func(what string, f func(recvSpan) bool) {
		if !slices.ContainsFunc(spans, f) {
			fail("no %s span", what)
		}
	}
	has("HTTP SERVER (POST /api/v1/orders)", func(s recvSpan) bool {
		return s.kind == tracepb.Span_SPAN_KIND_SERVER && s.service == "gateway" && s.name == "POST /api/v1/orders"
	})
	has("gRPC CLIENT", func(s recvSpan) bool { return s.kind == tracepb.Span_SPAN_KIND_CLIENT && hasAttr(s, rpcKeys, "grpc") })
	has("gRPC SERVER", func(s recvSpan) bool { return s.kind == tracepb.Span_SPAN_KIND_SERVER && hasAttr(s, rpcKeys, "grpc") })
	has("Kafka PRODUCER (order.submit)", func(s recvSpan) bool { return isTopic(s, tracepb.Span_SPAN_KIND_PRODUCER, "order.submit") })
	has("Kafka CONSUMER (order.submit)", func(s recvSpan) bool { return isTopic(s, tracepb.Span_SPAN_KIND_CONSUMER, "order.submit") })
	has("MySQL", func(s recvSpan) bool { return strings.HasPrefix(s.name, "mysql ") })
	has("Redis", func(s recvSpan) bool { return hasAttr(s, dbKeys, "redis") })

	// 连通：除根之外每个 span 的父都在这条链路里。
	roots := 0
	for _, s := range spans {
		if s.parent == "" || strings.Trim(s.parent, "0") == "" {
			roots++
			continue
		}
		if _, ok := byID[s.parent]; !ok {
			fail("span %q (%s) has parent %s that is not in the trace", s.name, s.service, s.parent)
		}
	}
	if roots != 1 {
		fail("%d root spans, want 1", roots)
	}

	// 消费者的 span 是生产者 span 的子 span。
	for _, s := range spans {
		if isTopic(s, tracepb.Span_SPAN_KIND_CONSUMER, "order.submit") {
			p, ok := byID[s.parent]
			if !ok || !isTopic(p, tracepb.Span_SPAN_KIND_PRODUCER, "order.submit") {
				fail("consumer span %q is not a child of the order.submit producer span (parent %q)", s.name, p.name)
			}
		}
	}
	return problems
}

// spansOfOrder 返回所有带这个订单号属性的 span 所在链路里的全部 span。
func spansOfOrder(orderID string) []recvSpan {
	all := otlp.all()
	traces := map[string]bool{}
	for _, s := range all {
		for _, v := range s.attrs {
			if v == orderID {
				traces[s.traceID] = true
			}
		}
	}
	var out []recvSpan
	for _, s := range all {
		if traces[s.traceID] {
			out = append(out, s)
		}
	}
	return out
}

// checkPaymentTraces 检查支付这一段：支付请求的链路里出现 payment 服务；支付结果经 outbox 回到 order 时，
// order 里消费 payment.events 的 span 有父 span，且它所在的链路里有 payment 服务的 span。
func checkPaymentTraces(orderID, payTrace string) []string {
	var problems []string
	fail := func(f string, a ...any) { problems = append(problems, "payment: "+fmt.Sprintf(f, a...)) }

	pay := otlp.trace(payTrace)
	if !slices.ContainsFunc(pay, func(s recvSpan) bool { return s.service == "payment" }) {
		// 支付请求本身不一定直接碰到 payment：看这个订单的所有链路里有没有。
		if !slices.ContainsFunc(spansOfOrder(orderID), func(s recvSpan) bool { return s.service == "payment" }) {
			fail("no span from the payment service in the pay trace or any trace of order %s", orderID)
		}
	}

	related := spansOfOrder(orderID)
	byTrace := map[string][]recvSpan{}
	for _, s := range related {
		byTrace[s.traceID] = append(byTrace[s.traceID], s)
	}
	found := false
	for _, s := range related {
		if s.service != "order" || !isTopic(s, tracepb.Span_SPAN_KIND_CONSUMER, "payment.events") {
			continue
		}
		if s.parent == "" || strings.Trim(s.parent, "0") == "" {
			continue
		}
		inTrace := byTrace[s.traceID]
		parentIn := slices.ContainsFunc(inTrace, func(x recvSpan) bool { return x.spanID == s.parent })
		// 这条链路从 payment 的后台处理器出结果开始（根 span 是 payment.complete，写 outbox 的事务在它下面），
		// 经 outbox 到 order，再经 order 的 outbox 到 inventory：两次过表，链路都没有断。
		started := slices.ContainsFunc(inTrace, func(x recvSpan) bool { return x.service == "payment" && x.name == "payment.complete" })
		reached := slices.ContainsFunc(inTrace, func(x recvSpan) bool { return x.service == "inventory" })
		if parentIn && started && reached {
			found = true
		}
	}
	if !found {
		fail("no trace runs from payment.complete (payment) through the order consumer of payment.events to inventory (the outbox did not carry the trace)")
	}
	return problems
}
