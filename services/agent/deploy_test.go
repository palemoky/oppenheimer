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

// pprof_capture 的端口转发权限只在 oppenheimer 命名空间的 Role 里（不是 ClusterRole），并且只有
// pods/portforward 的 create 和 pods 的 get。这里核对它没有被放宽、也没有挪到别的命名空间。
func TestPprofRoleIsNarrow(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/cluster/monitoring/agent.yaml")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var roles []rbacv1.Role
	var bindings []rbacv1.RoleBinding
	for i, doc := range bytes.Split(raw, []byte("\n---")) {
		var kind struct {
			Kind string `json:"kind"`
		}
		if err := yaml.Unmarshal(doc, &kind); err != nil {
			t.Fatalf("document %d: %v", i, err)
		}
		switch kind.Kind {
		case "Role":
			var r rbacv1.Role
			if err := yaml.UnmarshalStrict(doc, &r); err != nil {
				t.Fatalf("parse Role: %v", err)
			}
			roles = append(roles, r)
		case "RoleBinding":
			var b rbacv1.RoleBinding
			if err := yaml.UnmarshalStrict(doc, &b); err != nil {
				t.Fatalf("parse RoleBinding: %v", err)
			}
			bindings = append(bindings, b)
		}
	}
	if len(roles) != 1 || len(bindings) != 1 {
		t.Fatalf("got %d Roles and %d RoleBindings, want exactly one of each", len(roles), len(bindings))
	}
	role, binding := roles[0], bindings[0]
	if role.Namespace != "oppenheimer" || binding.Namespace != "oppenheimer" {
		t.Errorf("Role in %q, RoleBinding in %q; both must be in oppenheimer", role.Namespace, binding.Namespace)
	}
	want := map[string][]string{"pods/portforward": {"create"}, "pods": {"get"}}
	got := map[string][]string{}
	for i, r := range role.Rules {
		if !slices.Equal(r.APIGroups, []string{""}) || len(r.NonResourceURLs) > 0 || len(r.ResourceNames) > 0 {
			t.Errorf("rule %d: want the core API group only, no nonResourceURLs or resourceNames", i)
		}
		for _, res := range r.Resources {
			got[res] = append(got[res], r.Verbs...)
		}
	}
	if len(got) != len(want) {
		t.Errorf("Role grants %v, want exactly %v", got, want)
	}
	for res, verbs := range want {
		if !slices.Equal(got[res], verbs) {
			t.Errorf("Role grants %s %v, want %v", res, got[res], verbs)
		}
	}
	if binding.RoleRef.Kind != "Role" || binding.RoleRef.Name != role.Name {
		t.Errorf("RoleBinding refers to %s %q, want Role %q", binding.RoleRef.Kind, binding.RoleRef.Name, role.Name)
	}
	if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != "ServiceAccount" ||
		binding.Subjects[0].Name != "agent" || binding.Subjects[0].Namespace != "monitoring" {
		t.Errorf("RoleBinding subjects = %+v, want only ServiceAccount monitoring/agent", binding.Subjects)
	}
}
