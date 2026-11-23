package prom

import (
	"cmp"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
)

var nan = math.NaN()

// fmtVal 保留 4 位有效数字。NaN、±Inf 照 Prometheus 的写法。
func fmtVal(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	// 先按 4 位有效数字舍入；整数部分不太大时写成普通小数（24030 而不是 2.403e+04），模型读起来不费劲。
	r, _ := strconv.ParseFloat(strconv.FormatFloat(v, 'g', 4, 64), 64)
	if a := math.Abs(r); a >= 1 && a < 1e15 {
		return strconv.FormatFloat(r, 'f', -1, 64)
	}
	return strconv.FormatFloat(r, 'g', -1, 64)
}

// sortDesc 按 key 从大到小稳定排序，NaN 排最后。
func sortDesc[T any](xs []T, key func(T) float64) {
	slices.SortStableFunc(xs, func(a, b T) int {
		ka, kb := key(a), key(b)
		switch na, nb := math.IsNaN(ka), math.IsNaN(kb); {
		case na && nb:
			return 0
		case na:
			return 1
		case nb:
			return -1
		}
		return cmp.Compare(kb, ka)
	})
}

// commonLabels 返回所有序列都有、而且值都相同的标签。少于两条序列时不提取：一条序列直接写全。
func commonLabels(ss []series) map[string]string {
	if len(ss) < 2 {
		return nil
	}
	common := maps.Clone(ss[0].Metric)
	for _, s := range ss[1:] {
		maps.DeleteFunc(common, func(k, v string) bool {
			got, ok := s.Metric[k]
			return !ok || got != v
		})
	}
	return common
}

// writeCommon 把共有的标签写一行，返回它们，每条序列的那一行就不再重复。
func writeCommon(b *strings.Builder, ss []series) map[string]string {
	common := commonLabels(ss)
	if len(common) > 0 {
		b.WriteString("labels common to all shown series: " + fmtLabels(common, nil) + "\n")
	}
	return common
}

// fmtLabels 写成 PromQL 的样子：name{k="v", ...}，跳过 skip 里的标签。
func fmtLabels(m map[string]string, skip map[string]string) string {
	var b strings.Builder
	if _, skipped := skip["__name__"]; !skipped {
		b.WriteString(m["__name__"])
	}
	keys := slices.Sorted(maps.Keys(m))
	n := 0
	for _, k := range keys {
		if k == "__name__" {
			continue
		}
		if _, skipped := skip[k]; skipped {
			continue
		}
		if n == 0 {
			b.WriteString("{")
		} else {
			b.WriteString(", ")
		}
		b.WriteString(k + "=" + strconv.Quote(m[k]))
		n++
	}
	if n > 0 {
		b.WriteString("}")
	} else if b.Len() == 0 {
		b.WriteString("{}")
	}
	return b.String()
}
