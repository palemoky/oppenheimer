package ledger

import (
	"bufio"
	"bytes"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// AcceptedStatus 是订单清单导入成账本行时填的订单状态：202 只表示受理，订单还在落库，
// 和 run 在响应里缺状态时按 202 推断的值相同。
const AcceptedStatus = "creating"

// OrderLine 是 k6 发压时输出的订单清单（orders.jsonl）的一行：每个被受理（HTTP 202）的订单一行。
// 64 位的号都是十进制字符串。
type OrderLine struct {
	Run            string `json:"run"`
	UserID         string `json:"user_id"`
	OrderID        string `json:"order_id"`
	IdempotencyKey string `json:"idempotency_key"`
	SKUID          string `json:"sku_id"`
	Quantity       int32  `json:"quantity"`
	AcceptedAt     string `json:"accepted_at"`
}

// parsedOrder 是校验过的一行。
type parsedOrder struct {
	line       int
	run        string
	userID     int64
	orderID    int64
	key        string
	skuID      int64
	quantity   int32
	acceptedAt time.Time
}

// sameOrder 比较两行除受理时间以外的内容。k6 用同一个幂等键重试时，每次 202 都会输出一行，
// 订单号相同，受理时间不同，所以受理时间不参与比较（保留最早的）。
func (p parsedOrder) sameOrder(q parsedOrder) bool {
	return p.run == q.run && p.userID == q.userID && p.orderID == q.orderID && p.key == q.key &&
		p.skuID == q.skuID && p.quantity == q.quantity
}

func parsePositive(field, s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%s is missing", field)
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s %q is not a positive decimal integer", field, s)
	}
	return v, nil
}

func parseOrderLine(raw []byte, lineNo int) (parsedOrder, error) {
	var l OrderLine
	if err := json.Unmarshal(raw, &l); err != nil {
		return parsedOrder{}, err
	}
	p := parsedOrder{line: lineNo, run: l.Run, key: l.IdempotencyKey, quantity: l.Quantity}
	var errs []error
	if l.Run == "" {
		errs = append(errs, errors.New("run is missing"))
	}
	if l.IdempotencyKey == "" {
		errs = append(errs, errors.New("idempotency_key is missing"))
	}
	if l.Quantity <= 0 {
		errs = append(errs, fmt.Errorf("quantity %d is not positive", l.Quantity))
	}
	var err error
	if p.userID, err = parsePositive("user_id", l.UserID); err != nil {
		errs = append(errs, err)
	}
	if p.orderID, err = parsePositive("order_id", l.OrderID); err != nil {
		errs = append(errs, err)
	}
	if p.skuID, err = parsePositive("sku_id", l.SKUID); err != nil {
		errs = append(errs, err)
	}
	if l.AcceptedAt == "" {
		errs = append(errs, errors.New("accepted_at is missing"))
	} else if p.acceptedAt, err = time.Parse(time.RFC3339Nano, l.AcceptedAt); err != nil {
		errs = append(errs, fmt.Errorf("accepted_at %q is not RFC 3339", l.AcceptedAt))
	}
	return p, errors.Join(errs...)
}

// ReadOrderList 读 k6 的订单清单，把每个被受理的订单转成一条 submit 账本行
// （Outcome ok、HTTP 202、Attempt 1、订单状态 AcceptedStatus），按第一次出现的顺序返回。
//
// 格式不对的行直接报错并指出行号（清单不是被强杀的进程写的，坏行说明清单本身有问题，不跳过）。
// 同一个订单号出现多次时只留一条（k6 重试同一个幂等键会重复输出）；除受理时间外内容不一致时报错。
func ReadOrderList(r io.Reader) ([]Entry, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxLine)
	byID := map[int64]int{} // 订单号 → out 的下标
	var (
		out    []Entry
		parsed []parsedOrder
	)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		raw := bytes.TrimSpace(sc.Bytes())
		if len(raw) == 0 {
			continue
		}
		p, err := parseOrderLine(raw, lineNo)
		if err != nil {
			return nil, fmt.Errorf("order list line %d: %w", lineNo, err)
		}
		if i, dup := byID[p.orderID]; dup {
			first := parsed[i]
			if !first.sameOrder(p) {
				return nil, fmt.Errorf("order list line %d: order %d differs from line %d", lineNo, p.orderID, first.line)
			}
			if p.acceptedAt.Before(first.acceptedAt) {
				parsed[i].acceptedAt, out[i].Time = p.acceptedAt, p.acceptedAt
			}
			continue
		}
		byID[p.orderID] = len(out)
		parsed = append(parsed, p)
		out = append(out, Entry{
			Time:           p.acceptedAt,
			Op:             OpSubmit,
			OrderID:        strconv.FormatInt(p.orderID, 10),
			UserID:         p.userID,
			SKUID:          p.skuID,
			Quantity:       p.quantity,
			IdempotencyKey: p.key,
			Attempt:        1,
			HTTPStatus:     http.StatusAccepted,
			OrderStatus:    AcceptedStatus,
			Outcome:        OutcomeOK,
		})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read order list: %w", err)
	}
	return out, nil
}

// ReadOrderListFile 是读文件版的 ReadOrderList。
func ReadOrderListFile(path string) ([]Entry, error) {
	f, err := os.Open(path) //nolint:gosec // 路径来自命令行参数，由操作者指定
	if err != nil {
		return nil, fmt.Errorf("open order list %s: %w", path, err)
	}
	defer f.Close()
	return ReadOrderList(f)
}
