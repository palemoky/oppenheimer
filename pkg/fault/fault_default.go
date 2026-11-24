//go:build !faults

package fault

import "net/http"

// Compiled 报告这个二进制里有没有故障代码。
const Compiled = false

// Register 在不带 faults 标签的构建里什么都不做。
func Register(*http.ServeMux) {}
