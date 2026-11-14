package capture

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	cdcv1 "github.com/palemoky/oppenheimer/api/gen/oppenheimer/cdc/v1"
)

// 本文件把两条路径上的列值映射成 cdcv1.Value：binlog 的行事件（go-mysql 解码后的 Go 值）和全量读表的结果集（文本协议的字节）。
// 同一行从两条路径出来的映像必须相同，否则下游比较版本时会把同一个版本当成两个不同的内容。
// 两边对不齐的地方都在这里补齐，并由测试钉住。

// binaryCollation 是 binary 字符集的排序规则号。BINARY、VARBINARY、BLOB 在 TABLE_MAP 里就是这个排序规则。
const binaryCollation = 63

// tableColumns 是一个 TABLE_MAP 事件里与取值相关的列信息。
type tableColumns struct {
	names []string
	// binary[i] 表示第 i 列是二进制列（值进 Value.raw）。
	binary []bool
	// padTo[i] 大于 0 表示第 i 列是 BINARY(n)：binlog 去掉了尾部的 0x00，要补回 n 字节。
	padTo []int
}

// errNoColumnNames 说明源没有开 binlog_row_metadata=FULL。
const errNoColumnNames = "binlog_row_metadata is not FULL on the source"

// columnsOf 从 TABLE_MAP 事件取列名和列的二进制属性。
// 取不到列名（或缺字符集信息）说明源没有开 binlog_row_metadata=FULL，这时不能猜，直接报错。
func columnsOf(t *replication.TableMapEvent) (tableColumns, error) {
	n := len(t.ColumnType) // 等于 ColumnCount
	names := t.ColumnNameString()
	if len(names) != n {
		return tableColumns{}, fmt.Errorf("table %s.%s: got %d column names for %d columns: %s",
			t.Schema, t.Table, len(names), n, errNoColumnNames)
	}
	cols := tableColumns{names: names, binary: make([]bool, n), padTo: make([]int, n)}
	collations := t.CollationMap()
	for i := range n {
		if !t.IsCharacterColumn(i) {
			continue
		}
		coll, ok := collations[i]
		if !ok {
			return tableColumns{}, fmt.Errorf("table %s.%s: no charset for column %s: %s",
				t.Schema, t.Table, names[i], errNoColumnNames)
		}
		if coll != binaryCollation {
			continue
		}
		cols.binary[i] = true
		if t.ColumnType[i] == mysql.MYSQL_TYPE_STRING {
			cols.padTo[i] = fixedStringLength(t.ColumnMeta[i])
		}
	}
	return cols, nil
}

// fixedStringLength 从 MYSQL_TYPE_STRING 列的元数据里取出定义的字节长度。
// 编码方式见 MySQL 的 Field_string::do_save_field_metadata，解法与 go-mysql 的 decodeValue 相同。
func fixedStringLength(meta uint16) int {
	if meta < 256 {
		return int(meta)
	}
	b0, b1 := uint8(meta>>8), uint8(meta&0xFF)
	if b0&0x30 != 0x30 {
		return int(uint16(b1) | uint16((b0&0x30)^0x30)<<4)
	}
	return int(b1)
}

// binlogRow 把行事件里的一行转成列名到值的映射。
func binlogRow(cols tableColumns, row []any) (map[string]*cdcv1.Value, error) {
	if len(row) != len(cols.names) {
		return nil, fmt.Errorf("row has %d values for %d columns", len(row), len(cols.names))
	}
	out := make(map[string]*cdcv1.Value, len(row))
	for i, v := range row {
		val, err := binlogValue(v, cols.binary[i], cols.padTo[i])
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", cols.names[i], err)
		}
		out[cols.names[i]] = val
	}
	return out, nil
}

// binlogValue 映射 go-mysql 解码出的一个值。
//
// 对齐全量路径要注意的两处：
//   - BINARY(n)：binlog 里去掉了尾部的 0x00（MySQL 的 Field_string::pack 按填充字符截尾），
//     而 SELECT 返回补满 n 字节的值，所以这里补回去。CHAR 的尾部空格两边都去掉了，不用处理。
//   - FLOAT：binlog 里是 float32，直接转 float64 会多出 0.10000000149 这样的尾巴，
//     而 SELECT 返回的是 float32 的最短十进制表示。先按 float32 的最短表示格式化再解析，两边一致。
func binlogValue(v any, binary bool, padTo int) (*cdcv1.Value, error) {
	switch x := v.(type) {
	case nil:
		return &cdcv1.Value{}, nil
	case int8:
		return intValue(int64(x)), nil
	case int16:
		return intValue(int64(x)), nil
	case int32:
		return intValue(int64(x)), nil
	case int64:
		return intValue(x), nil
	case int:
		return intValue(int64(x)), nil
	case uint8:
		return uintValue(uint64(x)), nil
	case uint16:
		return uintValue(uint64(x)), nil
	case uint32:
		return uintValue(uint64(x)), nil
	case uint64:
		return uintValue(x), nil
	case float32:
		f, err := strconv.ParseFloat(strconv.FormatFloat(float64(x), 'g', -1, 32), 64)
		if err != nil {
			return nil, fmt.Errorf("float32 %v: %w", x, err)
		}
		return &cdcv1.Value{Kind: &cdcv1.Value_Float{Float: f}}, nil
	case float64:
		return &cdcv1.Value{Kind: &cdcv1.Value_Float{Float: x}}, nil
	case string:
		if binary {
			return rawValue(padBinary([]byte(x), padTo)), nil
		}
		return strValue(x), nil
	case []byte:
		// BLOB、TEXT、JSON 解码出来是 []byte，指向事件的缓冲区，要复制。
		if binary {
			return rawValue(padBinary(bytes.Clone(x), padTo)), nil
		}
		return strValue(string(x)), nil
	default:
		// DECIMAL 在不开 UseDecimal 时是字符串，走上面的分支；其他少见的类型按文本传递。
		return strValue(fmt.Sprint(x)), nil
	}
}

func padBinary(b []byte, n int) []byte {
	if len(b) >= n {
		return b
	}
	return append(b, make([]byte, n-len(b))...)
}

// sqlValue 映射全量读表时一列的值。b 是文本协议返回的字节，nil 表示 NULL；typeName 是 go-sql-driver 给出的列类型名。
func sqlValue(typeName string, b []byte) (*cdcv1.Value, error) {
	if b == nil {
		return &cdcv1.Value{}, nil
	}
	s := string(b)
	switch typeName {
	case "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "YEAR":
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %s %q: %w", typeName, s, err)
		}
		return intValue(n), nil
	case "UNSIGNED TINYINT", "UNSIGNED SMALLINT", "UNSIGNED MEDIUMINT", "UNSIGNED INT", "UNSIGNED BIGINT":
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %s %q: %w", typeName, s, err)
		}
		return uintValue(n), nil
	case "FLOAT", "DOUBLE":
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("parse %s %q: %w", typeName, s, err)
		}
		return &cdcv1.Value{Kind: &cdcv1.Value_Float{Float: f}}, nil
	}
	if strings.HasSuffix(typeName, "BLOB") || strings.HasSuffix(typeName, "BINARY") {
		return rawValue(bytes.Clone(b)), nil
	}
	return strValue(s), nil
}

func intValue(n int64) *cdcv1.Value   { return &cdcv1.Value{Kind: &cdcv1.Value_Int{Int: n}} }
func uintValue(n uint64) *cdcv1.Value { return &cdcv1.Value{Kind: &cdcv1.Value_Uint{Uint: n}} }
func strValue(s string) *cdcv1.Value  { return &cdcv1.Value{Kind: &cdcv1.Value_Str{Str: s}} }
func rawValue(b []byte) *cdcv1.Value  { return &cdcv1.Value{Kind: &cdcv1.Value_Raw{Raw: b}} }

// keyString 把主键列的值转成消息 key（十进制字符串），同时给出它作为全量游标的 int64 值。
func keyString(v *cdcv1.Value) (string, int64, error) {
	switch k := v.GetKind().(type) {
	case *cdcv1.Value_Int:
		return strconv.FormatInt(k.Int, 10), k.Int, nil
	case *cdcv1.Value_Uint:
		// 全量的游标是 int64，超出范围的无符号主键不支持。
		if k.Uint > math.MaxInt64 {
			return "", 0, fmt.Errorf("unsigned key %d overflows int64", k.Uint)
		}
		return strconv.FormatUint(k.Uint, 10), int64(k.Uint), nil
	default:
		return "", 0, fmt.Errorf("key column is not an integer: %v", v)
	}
}
