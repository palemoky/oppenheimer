//go:build faults

package indexer

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
	"github.com/palemoky/oppenheimer/pkg/fault"
)

// 这个文件只在带 faults 标签的构建里编译，钉住开关 f07 的行为（答案在 chaos/scenarios/ 里，这里不写）。
//
// 开着时的症状是进程崩溃。测试进程不能崩，所以分三处验：
//   - rowDirect 在当前 goroutine 里直接调，用 recover 断言它 panic；
//   - decodeChanges 会把 panic 带到它自己起的 goroutine 里，那里没有人能 recover，
//     所以在子进程（重新执行本测试二进制，只跑 TestFanoutChildProcess）里调，断言子进程以 panic 退出；
//   - 不满足触发条件的输入，开着和关着的结果逐条相同。

// orderImage 返回一个形状真实的订单映像：status 为 status，paid 为空串时 paid_at 是 NULL。
func orderImage(merchant, order, version, status int64, paid string) map[string]*cdcv1.Value {
	img := image(merchant, order, version)
	img[colStatus] = vInt(status)
	img[colPaidAt] = vNull()
	if paid != "" {
		img[colPaidAt] = vStr(paid)
	}
	return img
}

// closedUnpaid 是一张没付款、超时关掉的订单（CLOSED，paid_at 是 NULL）。
func closedUnpaid() *cdcv1.RowChange {
	return change(cdcv1.Op_OP_UPDATE, orderImage(3, 900, 4, 5, ""))
}

// ordinaryChanges 是不含“付款之后的状态却没有 paid_at”的一批：各种状态、NULL 商家、DELETE、不认识的表和 op、
// 超过一个块的长度。
func ordinaryChanges() []*cdcv1.RowChange {
	var chs []*cdcv1.RowChange
	for i := range int64(decodeSpan + 10) {
		o := 100 + i
		switch i % 5 {
		case 0:
			chs = append(chs, change(cdcv1.Op_OP_INSERT, orderImage(0, o, 1, 1, ""))) // CREATING，商家还是 NULL
		case 1:
			chs = append(chs, change(cdcv1.Op_OP_UPDATE, orderImage(1+i%4, o, 2, 2, ""))) // PENDING_PAYMENT
		case 2:
			chs = append(chs, change(cdcv1.Op_OP_UPDATE, orderImage(1+i%4, o, 3, 3, "2026-06-01 12:05:00.250000"))) // PAID
		case 3:
			chs = append(chs, deletion(orderImage(1+i%4, o, 3, 3, "2026-06-01 12:05:00.000001")))
		case 4:
			ch := change(cdcv1.Op_OP_UPDATE, orderImage(1, o, 2, 2, ""))
			ch.Table = "order_items"
			chs = append(chs, ch)
		}
	}
	// 类型不对、走回 decodeRow 的：Uint 的商家号仍然是合法的。
	img := orderImage(2, 990, 2, 2, "")
	img[colMerchantID] = &cdcv1.Value{Kind: &cdcv1.Value_Uint{Uint: 2}}
	return append(chs, change(cdcv1.Op_OP_UPDATE, img))
}

func setFanout(t *testing.T, on bool) {
	t.Helper()
	if err := fault.Set("f07", on, time.Minute); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fault.Set("f07", false, 0) })
}

func sameDecoded(t *testing.T, got, want []decodedChange) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.kind != w.kind || g.archived != w.archived || !sameImage(g.row, w.row) {
			t.Errorf("change %d: got %+v, want %+v", i, g, w)
		}
	}
}

// 不能和别的测试并行：开关是进程级的。
func TestFanoutMatchesInOrder(t *testing.T) {
	ordinary := ordinaryChanges()
	want, err := decodeInOrder(ordinary)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("off", func(t *testing.T) {
		setFanout(t, false)
		// 关着时连 CLOSED 没付款的订单也照常解码（不进 rowDirect）。
		chs := slices.Concat(ordinary, []*cdcv1.RowChange{closedUnpaid()})
		wantAll, err := decodeInOrder(chs)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeChanges(chs)
		if err != nil {
			t.Fatal(err)
		}
		sameDecoded(t, got, wantAll)
		if r := got[len(got)-1]; r.kind != kindRow || !r.row.PaidAt.IsZero() || r.row.Status != 5 {
			t.Errorf("closed order decoded as %+v", r)
		}
	})

	t.Run("on", func(t *testing.T) {
		setFanout(t, true)
		got, err := decodeChanges(ordinary)
		if err != nil {
			t.Fatal(err)
		}
		sameDecoded(t, got, want)
		for _, n := range []int{1, decodeSpan, decodeSpan + 1} {
			got, err := decodeChanges(ordinary[:n])
			if err != nil {
				t.Fatal(err)
			}
			sameDecoded(t, got, want[:n])
		}
	})

	t.Run("on, schema errors", func(t *testing.T) {
		setFanout(t, true)
		missing := orderImage(1, 50, 2, 2, "")
		delete(missing, colCurrency)
		badStatus := orderImage(1, 51, 2, 300, "")
		// 按顺序第一条出错的是 missing，和逐条解码时一样。
		chs := slices.Concat(ordinary[:3], []*cdcv1.RowChange{change(cdcv1.Op_OP_UPDATE, missing), change(cdcv1.Op_OP_UPDATE, badStatus)})
		_, wantErr := decodeInOrder(chs)
		_, gotErr := decodeChanges(chs)
		if !errors.Is(gotErr, ErrSchema) || gotErr.Error() != wantErr.Error() {
			t.Errorf("got error %v, want %v", gotErr, wantErr)
		}
	})
}

func TestRowDirectPanicsOnClosedUnpaid(t *testing.T) {
	t.Parallel()
	for _, status := range []int64{4, 5} { // CANCELLED、CLOSED：都没有付款
		t.Run(strconv.FormatInt(status, 10), func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recover() == nil {
					t.Errorf("status %d with NULL paid_at: rowDirect did not panic", status)
				}
			}()
			_, _, _ = rowDirect(orderImage(3, 900, 4, status, ""))
		})
	}
}

const childEnv = "ORDERINDEX_FANOUT_CHILD"

// TestFanoutChildProcess 只在 TestFanoutCrashesProcess 起的子进程里真正执行。
func TestFanoutChildProcess(t *testing.T) {
	if os.Getenv(childEnv) != "1" {
		t.Skip("runs only as the child of TestFanoutCrashesProcess")
	}
	setFanout(t, true)
	_, _ = decodeChanges([]*cdcv1.RowChange{closedUnpaid()})
	t.Fatal("decodeChanges returned; the process should have died")
}

// 开着时一条 CLOSED、没付款的订单让整个进程退出：panic 发生在 decodeChanges 起的 goroutine 里，
// kafkax 在 handler 外面的 recover 接不到。
func TestFanoutCrashesProcess(t *testing.T) {
	t.Parallel()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestFanoutChildProcess$", "-test.count=1")
	cmd.Env = append(os.Environ(), childEnv+"=1")
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stderr, &stderr
	err := cmd.Run()
	if _, ok := errors.AsType[*exec.ExitError](err); !ok {
		t.Fatalf("child: err = %v, want a non-zero exit; output:\n%s", err, stderr.String())
	}
	out := stderr.String()
	for _, want := range []string{"panic: interface conversion", "rowDirect"} {
		if !strings.Contains(out, want) {
			t.Errorf("child output has no %q:\n%s", want, out)
		}
	}
}
