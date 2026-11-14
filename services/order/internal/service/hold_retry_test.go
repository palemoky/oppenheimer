package service

import (
	"testing"

	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	"github.com/palemoky/oppenheimer/pkg/idgen"
)

// fixedIDs 每次都发同一个订单号，用来模拟“同一次受理调用的 Hold 被执行了两次”。
type fixedIDs struct{ id int64 }

func (f fixedIDs) Next(int64) (int64, error) { return f.id, nil }

// Redis 客户端在读超时后会自动重试命令。Hold 的脚本第一次已经执行成功、回复却没收到时，
// 重试的那一次看到自己刚写的幂等键，返回“重复”，带的订单号就是这次调用自己的。
// 这时必须按受理成功继续发落库消息。以前把它当成“以前受理过”而不发消息：
// 用户拿到 202，占用等不到落库，宽限期后被回收，订单消失。
//
// 用一个每次都发同一个订单号的发号器来构造：第二次 SubmitOrder 的 Hold 看到的正是
// “幂等键已存在、记着的订单号等于本次的订单号”。
func TestHoldRetriedByTheClientStillPublishes(t *testing.T) {
	t.Parallel()
	const user = 1
	gen, err := idgen.New(0)
	if err != nil {
		t.Fatal(err)
	}
	id, err := gen.Next(user)
	if err != nil {
		t.Fatal(err)
	}
	e := newEnvWith(t, envOptions{ids: fixedIDs{id}})

	first := e.submit(t, user, "k", items(skuA, 2))
	if first.GetOrderId() != id {
		t.Fatalf("order id = %d, want the fixed id %d", first.GetOrderId(), id)
	}
	published := len(e.sender.records(testSubmitTopic))

	second := e.submit(t, user, "k", items(skuA, 2))
	if second.GetOrderId() != id || second.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATING {
		t.Fatalf("second call = order %d %s, want the same order, CREATING", second.GetOrderId(), second.GetStatus())
	}
	if got := len(e.sender.records(testSubmitTopic)) - published; got != 1 {
		t.Fatalf("the retried hold published %d submit messages, want 1: without it the order never lands", got)
	}
	// 占用只有一份：脚本的第二次执行在写入之前就返回了。
	e.wantGate(t, skuA, 998, 2)

	// 两条消息都是这个订单的，落库是幂等的。
	e.mustLand(t, id)
	e.mustLand(t, id)
	wantStatus(t, e.load(t, id), orderv1.OrderStatus_ORDER_STATUS_PENDING_PAYMENT)
	if n := e.orderRows(t); n != 1 {
		t.Fatalf("order rows = %d, want 1", n)
	}
}

// 对照：真正的重复请求（新的调用拿到新的订单号）不发消息，返回第一次受理的那个订单。
func TestRealDuplicateDoesNotPublishAgain(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	first := e.submit(t, 1, "k", items(skuA, 2))
	published := len(e.sender.records(testSubmitTopic))
	second := e.submit(t, 1, "k", items(skuA, 2))
	if second.GetOrderId() != first.GetOrderId() {
		t.Fatalf("duplicate got order %d, want the first order %d", second.GetOrderId(), first.GetOrderId())
	}
	if got := len(e.sender.records(testSubmitTopic)) - published; got != 0 {
		t.Fatalf("a real duplicate published %d messages, want 0", got)
	}
	e.wantGate(t, skuA, 998, 2)
}
