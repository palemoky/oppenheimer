//go:build faults

package fault

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// 每个用例用自己的开关名：登记表是包级的，名字不能重复。
func TestSwitchTurnsOffByItself(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sw := Switch("f90")
		if sw.On() {
			t.Fatal("a new switch must be off")
		}
		if err := Set("f90", true, time.Minute); err != nil {
			t.Fatal(err)
		}
		time.Sleep(59 * time.Second)
		if !sw.On() {
			t.Fatal("off before the ttl")
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if sw.On() {
			t.Fatal("still on after the ttl")
		}

		// 重新打开会换掉旧的定时器：旧的到点不能把新的关掉。
		if err := Set("f90", true, time.Minute); err != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Second)
		if err := Set("f90", true, time.Hour); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Second)
		synctest.Wait()
		if !sw.On() {
			t.Fatal("the replaced timer turned the switch off")
		}
		// 手工关掉之后定时器不再起作用，也能再打开。
		if err := Set("f90", false, 0); err != nil {
			t.Fatal(err)
		}
		if sw.On() {
			t.Fatal("still on after mode=off")
		}
		time.Sleep(2 * time.Hour)
		synctest.Wait()
		for _, v := range Snapshot() {
			if v.Name == "f90" && (v.On || !v.Until.IsZero()) {
				t.Errorf("snapshot after off = %+v", v)
			}
		}
	})
}

func TestSetValidates(t *testing.T) {
	Switch("f91")
	if err := Set("f00", true, 0); err == nil {
		t.Error("unknown switch: want an error")
	}
	for _, ttl := range []time.Duration{-time.Second, MaxTTL + time.Second} {
		if err := Set("f91", true, ttl); err == nil {
			t.Errorf("ttl %s: want an error", ttl)
		}
	}
	for _, bad := range []string{"leak", "f1", "F01", "f001", ""} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Switch(%q) must panic", bad)
				}
			}()
			Switch(bad)
		}()
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("registering f91 twice must panic")
			}
		}()
		Switch("f91")
	}()
	_ = Set("f91", false, 0)
}

func TestHTTP(t *testing.T) {
	sw := Switch("f92")
	mux := http.NewServeMux()
	Register(mux)
	do := func(method, target string) (int, string) {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), method, target, nil))
		return rec.Code, rec.Body.String()
	}
	if code, body := do(http.MethodPut, "/fault/f92?mode=on&ttl=10m"); code != http.StatusOK || !strings.Contains(body, `"name":"f92","on":true,"until":`) {
		t.Errorf("on = %d %s", code, body)
	}
	if !sw.On() {
		t.Error("switch is not on")
	}
	if code, body := do(http.MethodGet, "/fault"); code != http.StatusOK || !strings.Contains(body, `"f92"`) {
		t.Errorf("list = %d %s", code, body)
	}
	if code, body := do(http.MethodPut, "/fault/f92?mode=off"); code != http.StatusOK || !strings.Contains(body, `"name":"f92","on":false}`) {
		t.Errorf("off = %d %s", code, body)
	}
	for target, want := range map[string]int{
		"/fault/f92?mode=maybe":        http.StatusBadRequest,
		"/fault/f92?mode=on&ttl=soon":  http.StatusBadRequest,
		"/fault/f92?mode=on&ttl=1000h": http.StatusBadRequest,
		"/fault/f55?mode=on":           http.StatusNotFound,
	} {
		if code, _ := do(http.MethodPut, target); code != want {
			t.Errorf("PUT %s = %d, want %d", target, code, want)
		}
	}
}

// On 在请求的路径上被并发读，同时有人在开关：-race 下不能有竞争。
func TestConcurrentUse(_ *testing.T) {
	sw := Switch("f93")
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 500 {
				_ = sw.On()
				_ = Snapshot()
			}
		})
	}
	wg.Go(func() {
		for i := range 200 {
			_ = Set("f93", i%2 == 0, time.Hour)
		}
	})
	wg.Wait()
	_ = Set("f93", false, 0)
}
