package agent

import (
	"bytes"
	"os"
	"slices"
	"strings"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"

	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
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

// 对模型隐藏的命名空间（tool.Hidden 的默认名单）有两处：Agent 的工具把它当成不存在，日志采集不收它的日志。
// 只做前一处的话 Loki 里仍然有那些日志，聚合掉命名空间标签的查询读得到；这里核对后一处没有漏。
func TestHiddenNamespacesAreNotCollected(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/cluster/monitoring/otel-agent.yaml")
	if err != nil {
		t.Fatalf("read collector values: %v", err)
	}
	var values struct {
		Config struct {
			Processors map[string]struct {
				Logs struct {
					LogRecord []string `json:"log_record"`
				} `json:"logs"`
			} `json:"processors"`
			Service struct {
				Pipelines struct {
					Logs struct {
						Processors []string `json:"processors"`
					} `json:"logs"`
				} `json:"pipelines"`
			} `json:"service"`
		} `json:"config"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatalf("parse collector values: %v", err)
	}
	const name = "filter/hidden_namespaces"
	if !slices.Contains(values.Config.Service.Pipelines.Logs.Processors, name) {
		t.Errorf("processor %s is not in the logs pipeline", name)
	}
	conditions := values.Config.Processors[name].Logs.LogRecord
	for ns := range strings.SplitSeq(tool.DefaultHiddenNamespaces, ",") {
		want := `resource.attributes["k8s.namespace.name"] == "` + ns + `"`
		if !slices.Contains(conditions, want) {
			t.Errorf("processor %s does not drop namespace %q: want the condition %s, have %q", name, ns, want, conditions)
		}
	}
}
