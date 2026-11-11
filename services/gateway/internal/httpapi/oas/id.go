// Package oas 是 gateway 对外 HTTP 接口的类型和服务端接口。
//
// oas.gen.go 由 api/openapi/gateway.yaml 生成（make openapi），不要手改。
// 本文件是规范通过 x-go-type 引用的手写类型。
package oas

import (
	"bytes"
	"fmt"
	"strconv"
)

// ID 是 64 位 ID 的 JSON 表示，对应规范里的 Id：输出一律是字符串，输入接受字符串或整数。
// JavaScript 的数字是双精度浮点，超过 2^53 的整数会丢精度，所以对外用字符串。
//
// 规范用 x-go-type 让生成代码引用这个手写类型，而不是生成 string。
// 这样 handler 拿到的直接是整数，不用在每个用到 ID 的地方再解析一遍。
type ID int64

// MarshalJSON 输出十进制字符串。
func (v ID) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatInt(int64(v), 10) + `"`), nil
}

// UnmarshalJSON 接受十进制字符串或 JSON 整数。
func (v *ID) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) >= 2 && b[0] == '"' && b[len(b)-1] == '"' {
		b = b[1 : len(b)-1]
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid id %s", b)
	}
	*v = ID(n)
	return nil
}
