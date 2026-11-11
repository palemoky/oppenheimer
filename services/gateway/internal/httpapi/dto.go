package httpapi

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
)

// id 是 64 位 ID 的 JSON 表示：输出一律是字符串，输入接受字符串或整数。
// JavaScript 的数字是双精度浮点，超过 2^53 的整数会丢精度，所以对外用字符串。
type id int64

func (v id) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatInt(int64(v), 10) + `"`), nil
}

func (v *id) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) >= 2 && b[0] == '"' && b[len(b)-1] == '"' {
		b = b[1 : len(b)-1]
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid id %s", b)
	}
	*v = id(n)
	return nil
}

// parseID 解析路径参数里的 ID，要求为正整数。
func parseID(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil && n > 0
}

type moneyDTO struct {
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

func toMoney(m *commonv1.Money) moneyDTO {
	return moneyDTO{Amount: m.GetAmount(), Currency: m.GetCurrency()}
}

// ---- 请求 ----

type createOrderRequest struct {
	Items []createOrderItem `json:"items"`
}

type createOrderItem struct {
	SKUID    id    `json:"sku_id"`
	Quantity int32 `json:"quantity"`
}

type payOrderRequest struct {
	Outcome string `json:"outcome"`
}

// ---- 响应 ----

type orderDTO struct {
	OrderID    id             `json:"order_id"`
	UserID     id             `json:"user_id"`
	Status     string         `json:"status"`
	Total      moneyDTO       `json:"total"`
	Items      []orderItemDTO `json:"items"`
	CreateTime time.Time      `json:"create_time"`
	ExpireTime time.Time      `json:"expire_time"`
	PayTime    *time.Time     `json:"pay_time,omitzero"`
}

type orderItemDTO struct {
	SKUID     id       `json:"sku_id"`
	Quantity  int32    `json:"quantity"`
	UnitPrice moneyDTO `json:"unit_price"`
}

func toOrder(o *orderv1.Order) orderDTO {
	d := orderDTO{
		OrderID:    id(o.GetOrderId()),
		UserID:     id(o.GetUserId()),
		Status:     enumName(o.GetStatus().String(), "ORDER_STATUS_"),
		Total:      toMoney(o.GetTotal()),
		Items:      make([]orderItemDTO, 0, len(o.GetItems())),
		CreateTime: utc(o.GetCreateTime()),
		ExpireTime: utc(o.GetExpireTime()),
	}
	for _, it := range o.GetItems() {
		d.Items = append(d.Items, orderItemDTO{
			SKUID:     id(it.GetSkuId()),
			Quantity:  it.GetQuantity(),
			UnitPrice: toMoney(it.GetUnitPrice()),
		})
	}
	if o.GetPayTime() != nil {
		t := utc(o.GetPayTime())
		d.PayTime = &t
	}
	return d
}

type paymentDTO struct {
	PaymentID  id        `json:"payment_id"`
	OrderID    id        `json:"order_id"`
	UserID     id        `json:"user_id"`
	Status     string    `json:"status"`
	Amount     moneyDTO  `json:"amount"`
	CreateTime time.Time `json:"create_time"`
	Notified   bool      `json:"notified"`
}

func toPayment(p *paymentv1.Payment) paymentDTO {
	return paymentDTO{
		PaymentID:  id(p.GetPaymentId()),
		OrderID:    id(p.GetOrderId()),
		UserID:     id(p.GetUserId()),
		Status:     enumName(p.GetStatus().String(), "PAYMENT_STATUS_"),
		Amount:     toMoney(p.GetAmount()),
		CreateTime: utc(p.GetCreateTime()),
		Notified:   p.GetNotified(),
	}
}

type stockDTO struct {
	SKUID     id       `json:"sku_id"`
	Available int64    `json:"available"`
	Reserved  int64    `json:"reserved"`
	Sold      int64    `json:"sold"`
	UnitPrice moneyDTO `json:"unit_price"`
}

func toStock(s *inventoryv1.GetStockResponse) stockDTO {
	return stockDTO{
		SKUID:     id(s.GetSkuId()),
		Available: s.GetAvailable(),
		Reserved:  s.GetReserved(),
		Sold:      s.GetSold(),
		UnitPrice: toMoney(s.GetUnitPrice()),
	}
}

// enumName 把 proto 枚举名转成对外的小写形式：ORDER_STATUS_PAID -> paid。
func enumName(name, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}

func utc(t *timestamppb.Timestamp) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.AsTime().UTC()
}
