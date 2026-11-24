//go:build !faults

package fault

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// 平时的构建：没有故障代码，管理端口上也没有 /fault。
func TestDefaultBuildHasNoSwitches(t *testing.T) {
	if Compiled {
		t.Fatal("Compiled must be false without the faults tag")
	}
	mux := http.NewServeMux()
	Register(mux)
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, "/fault/f01?mode=on", nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s /fault/f01 = %d, want 404", method, rec.Code)
		}
	}
}
