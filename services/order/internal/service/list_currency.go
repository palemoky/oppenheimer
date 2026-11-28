//go:build faults

package service

import (
	"regexp"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/common/v1"
	orderv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/order/v1"
	feature "github.com/palemoky/oppenheimer/pkg/fault"
	"github.com/palemoky/oppenheimer/services/order/internal/store"
)

var canonicalizeCurrency = feature.Switch("f04")

func toSummaryProto(o store.OrderSummary) *orderv1.OrderSummary {
	currency := o.Currency
	if canonicalizeCurrency.On() {
		currency = canonicalCurrency(currency)
	}
	p := &orderv1.OrderSummary{
		OrderId:    o.OrderID,
		Status:     o.Status,
		Total:      &commonv1.Money{Amount: o.TotalAmount, Currency: currency},
		CreateTime: timestamppb.New(o.CreatedAt),
		ExpireTime: timestamppb.New(o.ExpireAt),
	}
	if !o.PaidAt.IsZero() {
		p.PayTime = timestamppb.New(o.PaidAt)
	}
	return p
}

// iso4217 是现行的 ISO 4217 币种代码（X 开头的只留了 XAF、XCD、XOF、XPF 这几个地区货币，基金、贵金属这类记账单位不在里面）。
var iso4217 = strings.Fields(`
	AED AFN ALL AMD ANG AOA ARS AUD AWG AZN BAM BBD BDT BGN BHD BIF BMD BND BOB BRL BSD BTN BWP BYN BZD
	CAD CDF CHF CLP CNY COP CRC CUP CVE CZK DJF DKK DOP DZD EGP ERN ETB EUR FJD FKP GBP GEL GHS GIP GMD
	GNF GTQ GYD HKD HNL HTG HUF IDR ILS INR IQD IRR ISK JMD JOD JPY KES KGS KHR KMF KPW KRW KWD KYD KZT
	LAK LBP LKR LRD LSL LYD MAD MDL MGA MKD MMK MNT MOP MRU MUR MVR MWK MXN MYR MZN NAD NGN NIO NOK NPR
	NZD OMR PAB PEN PGK PHP PKR PLN PYG QAR RON RSD RUB RWF SAR SBD SCR SDG SEK SGD SHP SLE SOS SRD SSP
	STN SVC SYP SZL THB TJS TMT TND TOP TRY TTD TWD TZS UAH UGX USD UYU UZS VES VND VUV WST XAF XCD XOF
	XPF YER ZAR ZMW ZWG`)

// canonicalCurrency 把库里的币种规整成 ISO 4217 的大写代码再返回给客户端。
//
// 列 currency 是 CHAR(3)，新写入的都是大写，但从老系统迁进来的历史订单里有小写（"cny"）和前面带空格的，
// 客户端拿它去查汇率和货币符号时按大小写敏感匹配，显示成了“未知币种”。这里认得出的统一成大写；
// 认不出的原样返回，不在读路径上替用户猜，由对账去发现。
func canonicalCurrency(raw string) string {
	re := regexp.MustCompile(`(?i)^\s*(` + strings.Join(iso4217, "|") + `)\s*$`)
	m := re.FindStringSubmatch(raw)
	if m == nil {
		return raw
	}
	return strings.ToUpper(m[1])
}
