// Package capture 实现 CDC 的流水线：读一个源库的 binlog（必要时先做全量），把匹配规则的表的行变更
// 转成 cdc.v1.RowChange 发到 Kafka，并把位点存在 Kafka 的压缩 topic 里。
//
// 本包不认识任何业务表：表名、主键列、版本列都来自 Rule。设计见 docs/design/1c-sharding.md 第 4 节。
package capture

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

// Rule 描述一组要捕获的物理表，以及它们合成的逻辑表。
type Rule struct {
	// Pattern 匹配物理表名，必须整体匹配，并且第一个捕获组是 slot 的十进制数，例如 `^orders_(\d+)$`。
	Pattern *regexp.Regexp
	// Table 是逻辑表名，写进消息的 table 字段。
	Table string
	// Topic 是这组表的变更发往的 topic（已带环境前缀）。
	Topic string
	// KeyColumn 是主键列，必须是整数列。它的十进制字符串是消息的 key，全量按它分批。
	KeyColumn string
	// VersionColumn 是行版本列，用来检测“更新了行却没有递增版本”。为空表示不检测。
	VersionColumn string
}

func (r Rule) validate() error {
	switch {
	case r.Pattern == nil:
		return errors.New("rule: pattern is required")
	case r.Pattern.NumSubexp() < 1:
		return fmt.Errorf("rule %s: pattern %q has no capture group for slot", r.Table, r.Pattern)
	case r.Table == "", r.Topic == "", r.KeyColumn == "":
		return fmt.Errorf("rule %q: table, topic and key column are required", r.Table)
	}
	return nil
}

// match 判断物理表名是否属于这条规则，并取出 slot。
func (r Rule) match(table string) (slot int32, ok bool) {
	m := r.Pattern.FindStringSubmatch(table)
	if m == nil || m[0] != table {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(n), true
}

// matchRule 返回第一条匹配表名的规则。
func matchRule(rules []Rule, table string) (*Rule, int32, bool) {
	for i := range rules {
		if slot, ok := rules[i].match(table); ok {
			return &rules[i], slot, true
		}
	}
	return nil, 0, false
}
