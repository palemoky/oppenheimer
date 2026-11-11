// 本文件把下游的 proto 消息转换成对外的类型。
// 对外类型由 api/openapi/gateway.yaml 生成（oas 包），这里只保留转换逻辑。

package httpapi

import (
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/common/v1"
	inventoryv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/inventory/v1"
	orderv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/order/v1"
	paymentv1 "github.com/palemoky/Oppenheimer/api/gen/oppenheimer/payment/v1"
	"github.com/palemoky/Oppenheimer/services/gateway/internal/httpapi/oas"
)

// parseID 解析路径参数里的 ID，要求为正整数。
func parseID(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil && n > 0
}

func toMoney(m *commonv1.Money) oas.Money {
	return oas.Money{Amount: m.GetAmount(), Currency: m.GetCurrency()}
}

func toOrder(o *orderv1.Order) oas.Order {
	d := oas.Order{
		OrderId:    oas.ID(o.GetOrderId()),
		UserId:     oas.ID(o.GetUserId()),
		Status:     oas.OrderStatus(enumName(o.GetStatus().String(), "ORDER_STATUS_")),
		Total:      toMoney(o.GetTotal()),
		Items:      make([]oas.OrderItem, 0, len(o.GetItems())),
		CreateTime: utc(o.GetCreateTime()),
		ExpireTime: utc(o.GetExpireTime()),
	}
	for _, it := range o.GetItems() {
		d.Items = append(d.Items, oas.OrderItem{
			SkuId:     oas.ID(it.GetSkuId()),
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

func toPayment(p *paymentv1.Payment) oas.Payment {
	return oas.Payment{
		PaymentId:  oas.ID(p.GetPaymentId()),
		OrderId:    oas.ID(p.GetOrderId()),
		UserId:     oas.ID(p.GetUserId()),
		Status:     oas.PaymentStatus(enumName(p.GetStatus().String(), "PAYMENT_STATUS_")),
		Amount:     toMoney(p.GetAmount()),
		CreateTime: utc(p.GetCreateTime()),
		Notified:   p.GetNotified(),
	}
}

func toStock(s *inventoryv1.GetStockResponse) oas.Stock {
	return oas.Stock{
		SkuId:     oas.ID(s.GetSkuId()),
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
