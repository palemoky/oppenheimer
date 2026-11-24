package agent

import (
	"bytes"
	"os"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// 只读的边界划在 deploy/cluster/monitoring/agent.yaml 的 ClusterRole 里：这里核对它没有被放宽。
func TestClusterRoleIsReadOnly(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/cluster/monitoring/agent.yaml")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}

	var role *rbacv1.ClusterRole
	for i, doc := range bytes.Split(raw, []byte("\n---")) {
		var kind struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal(doc, &kind); err != nil {
			t.Fatalf("document %d: %v", i, err)
		}
		if kind.Kind != "ClusterRole" {
			continue
		}
		if role != nil {
			t.Fatal("more than one ClusterRole in the manifest")
		}
		role = &rbacv1.ClusterRole{}
		if err := yaml.UnmarshalStrict(doc, role); err != nil {
			t.Fatalf("parse ClusterRole: %v", err)
		}
	}
	if role == nil {
		t.Fatal("no ClusterRole in the manifest")
	}
	if len(role.Rules) == 0 {
		t.Fatal("ClusterRole has no rules")
	}

	allowed := []string{"get", "list", "watch"}
	forbiddenResources := []string{"secrets", "configmaps", "pods/exec", "*"}
	sawLog := false
	for i, r := range role.Rules {
		if len(r.NonResourceURLs) > 0 || len(r.ResourceNames) > 0 {
			t.Errorf("rule %d: unexpected nonResourceURLs or resourceNames", i)
		}
		if len(r.Verbs) == 0 {
			t.Errorf("rule %d: no verbs", i)
		}
		for _, v := range r.Verbs {
			if !slices.Contains(allowed, v) {
				t.Errorf("rule %d: verb %q is not read-only", i, v)
			}
		}
		if slices.Contains(r.APIGroups, "*") {
			t.Errorf("rule %d: wildcard apiGroup", i)
		}
		for _, res := range r.Resources {
			if slices.Contains(forbiddenResources, res) {
				t.Errorf("rule %d: resource %q must not be granted", i, res)
			}
			if res == "pods/log" {
				sawLog = true
				if !slices.Equal(r.Verbs, []string{"get"}) {
					t.Errorf("rule %d: pods/log verbs = %v, want [get]", i, r.Verbs)
				}
			}
		}
	}
	if !sawLog {
		t.Error("pods/log is not granted at all (the log tool needs it)")
	}
}
