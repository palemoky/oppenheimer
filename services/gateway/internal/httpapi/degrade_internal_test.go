package httpapi

import (
	"strings"
	"testing"

	"github.com/palemoky/oppenheimer/pkg/errx"
)

// TestFeaturesMatchSpec 保证 features 表里的接口都在 operations 表里（也就在规范里，见 TestOperationsMatchSpec），
// 开关名不重复。operationId 写错时开关登记不上，关掉它没有任何效果。
func TestFeaturesMatchSpec(t *testing.T) {
	ids := map[string]bool{}
	for _, op := range operations {
		ids[op.id] = true
	}
	names := map[string]bool{}
	for id, f := range features {
		if !ids[id] {
			t.Errorf("features has %s, which is not an operation", id)
		}
		if names[f.switchName] {
			t.Errorf("switch %s is used twice", f.switchName)
		}
		names[f.switchName] = true
	}
	// 核心接口没有开关（设计文档 1d 的 6.9）。
	for _, core := range []string{"createOrder", "getOrder", "cancelOrder", "payOrder"} {
		if _, ok := features[core]; ok {
			t.Errorf("core operation %s has a switch", core)
		}
	}
}

// TestEveryOperationDocuments503 保证规范里每个接口都声明了 503，ServiceUnavailable 带 Retry-After 头，
// 并且列出了全部降级原因码。
func TestEveryOperationDocuments503(t *testing.T) {
	specOps, responses := loadSpec(t)
	for _, so := range specOps {
		if got := so.responses["503"]; got != "ServiceUnavailable" {
			t.Errorf("%s: 503 response = %q, want a reference to ServiceUnavailable", so.id, got)
		}
	}
	node, ok := responses["ServiceUnavailable"]
	if !ok {
		t.Fatal("components.responses.ServiceUnavailable is missing")
	}
	var resp struct {
		Description string `yaml:"description"`
		Headers     map[string]struct {
			Ref string `yaml:"$ref"`
		} `yaml:"headers"`
	}
	if err := node.Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Headers["Retry-After"].Ref == "" {
		t.Error("ServiceUnavailable has no Retry-After header")
	}
	for _, code := range []string{
		"UNAVAILABLE",
		errx.ReasonStockGateUnavailable,
		errx.ReasonOrderLandingUnavailable,
		errx.ReasonInventoryUnavailable,
		errx.ReasonOrderQueueUnavailable,
		errx.ReasonOrderUnavailable,
		errx.ReasonArchiveUnavailable,
		errx.ReasonFeatureDisabled,
	} {
		if !strings.Contains(resp.Description, "`"+code+"`") {
			t.Errorf("ServiceUnavailable does not describe %s", code)
		}
		if code != "UNAVAILABLE" && !errx.IsDegraded(errx.Degraded(code, "x")) {
			t.Errorf("%s is not a degraded reason in errx", code)
		}
	}
}
