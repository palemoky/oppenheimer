//go:build faults

package service

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	feature "github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

// 这个文件只在带 faults 标签的构建里编译。它钉住的是 f04 的行为（答案在 chaos/scenarios/ 里，这里不写）：
// 开关关着时列表的每一行和平时的构建一样；开着时结果仍然正确，但每一行的开销（这里用分配次数量）
// 高出几个数量级，并且和这一页的行数成正比。
//
// 不并行：开关是进程级的。其他测试里的 t.Parallel 用例要等所有顶层测试的串行部分跑完才开始，
// 这时开关已经在 Cleanup 里关了。
func TestSummaryCurrencyVariant(t *testing.T) {
	now := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	row := func(id int64, currency string) store.OrderSummary {
		return store.OrderSummary{
			OrderID: id, Status: orderv1.OrderStatus_ORDER_STATUS_PAID, TotalAmount: 1990, Currency: currency,
			CreatedAt: now, ExpireAt: now.Add(15 * time.Minute), PaidAt: now.Add(time.Minute),
		}
	}
	want := func(id int64, currency string) *orderv1.OrderSummary {
		return &orderv1.OrderSummary{
			OrderId: id, Status: orderv1.OrderStatus_ORDER_STATUS_PAID,
			Total:      &commonv1.Money{Amount: 1990, Currency: currency},
			CreateTime: timestamppb.New(now), ExpireTime: timestamppb.New(now.Add(15 * time.Minute)),
			PayTime: timestamppb.New(now.Add(time.Minute)),
		}
	}
	page := func(n int) []store.OrderSummary {
		rows := make([]store.OrderSummary, n)
		for i := range rows {
			rows[i] = row(int64(1000-i), "CNY")
		}
		return rows
	}
	allocs := func(rows []store.OrderSummary) float64 {
		return testing.AllocsPerRun(20, func() { _ = newListResponse(rows) })
	}
	check := func(state string, in store.OrderSummary, out *orderv1.OrderSummary) {
		t.Helper()
		if got := toSummaryProto(in); !proto.Equal(got, out) {
			t.Errorf("switch %s: toSummaryProto(%q) = %v, want %v", state, in.Currency, got, out)
		}
	}

	// 关着：和平时的构建一样，币种原样返回，每行只有构造消息的几次分配。
	check("off", row(1, "CNY"), want(1, "CNY"))
	check("off", row(2, " cny"), want(2, " cny"))
	full := page(DefaultListPageSize)
	off := allocs(full)
	if perRow := off / DefaultListPageSize; perRow > 10 {
		t.Errorf("switch off: %.1f allocations per row, want at most 10", perRow)
	}

	if err := feature.Set("f04", true, time.Minute); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = feature.Set("f04", false, 0) })

	// 开着：结果仍然正确（认得出的规整成大写，认不出的原样返回）。
	check("on", row(1, "CNY"), want(1, "CNY"))
	check("on", row(2, " cny"), want(2, "CNY"))
	check("on", row(3, "ABC"), want(3, "ABC"))

	// 症状：一页 20 行的分配次数是关着时的几百倍，并且随行数线性增长（只有一两单的用户几乎不受影响）。
	on := allocs(full)
	one := allocs(page(1))
	start := time.Now()
	for range 20 {
		_ = newListResponse(full)
	}
	t.Logf("allocations per page of %d: off %.0f, on %.0f; on with 1 row %.0f; %s per page with the switch on",
		DefaultListPageSize, off, on, one, time.Since(start)/20)
	if on < 100*off {
		t.Errorf("switch on: %.0f allocations per page, want at least 100x the %.0f with the switch off", on, off)
	}
	if ratio := on / one; ratio < DefaultListPageSize/2 {
		t.Errorf("switch on: a page of %d rows allocates %.1fx a page of 1, want it to grow with the rows", DefaultListPageSize, ratio)
	}
}
