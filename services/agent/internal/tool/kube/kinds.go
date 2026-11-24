package kube

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	typedappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	typedautoscalingv2 "k8s.io/client-go/kubernetes/typed/autoscaling/v2"
	typedbatchv1 "k8s.io/client-go/kubernetes/typed/batch/v1"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	typeddiscoveryv1 "k8s.io/client-go/kubernetes/typed/discovery/v1"
)

// object 是白名单里各种资源的共同面：有 metadata，是一个 API 对象。
type object interface {
	metav1.Object
	runtime.Object
}

// page 是一次列表的结果。只取一页（listPageLimit），more 表示 apiserver 那边还有。
type page struct {
	items     []object
	more      bool
	remaining *int64
}

// spec 是白名单里的一种资源：怎么取、怎么列、列表里一行给什么、describe 给什么。
// 只能通过 define 建，读取都走 client-go 的强类型客户端，没有按资源名反射的通用路径。
type spec struct {
	kind       string
	namespaced bool
	aliases    []string
	header     []string
	get        func(ctx context.Context, c kubernetes.Interface, ns, name string) (object, error)
	list       func(ctx context.Context, c kubernetes.Interface, ns string, opts metav1.ListOptions) (page, error)
	row        func(o object, now time.Time) []string
	describe   func(b *strings.Builder, o object, now time.Time)
	// order 给列表排序，nil 时按命名空间、名字。
	order func(a, b object) int
}

// getter 是 client-go 强类型客户端里这里用到的两个方法（都是读）。
type getter[P, L any] interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (P, error)
	List(ctx context.Context, opts metav1.ListOptions) (L, error)
}

func define[T any, P interface {
	*T
	object
}, L metav1.ListInterface, G getter[P, L]](
	kind string, namespaced bool, aliases, header []string,
	client func(c kubernetes.Interface, ns string) G,
	items func(L) []T,
	row func(P, time.Time) []string,
	describe func(*strings.Builder, P, time.Time),
) *spec {
	return &spec{
		kind:       kind,
		namespaced: namespaced,
		aliases:    aliases,
		header:     header,
		get: func(ctx context.Context, c kubernetes.Interface, ns, name string) (object, error) {
			o, err := client(c, ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			return o, nil
		},
		list: func(ctx context.Context, c kubernetes.Interface, ns string, opts metav1.ListOptions) (page, error) {
			l, err := client(c, ns).List(ctx, opts)
			if err != nil {
				return page{}, err
			}
			its := items(l)
			objs := make([]object, len(its))
			for i := range its {
				objs[i] = P(&its[i])
			}
			return page{items: objs, more: l.GetContinue() != "", remaining: l.GetRemainingItemCount()}, nil
		},
		row:      func(o object, now time.Time) []string { return row(o.(P), now) },
		describe: func(b *strings.Builder, o object, now time.Time) { describe(b, o.(P), now) },
	}
}

// specs 是 kind 的白名单。Secret、ConfigMap 故意不在里面（设计 3.3）。
var specs = []*spec{
	withOrder(define("Pod", true, []string{"pod", "pods", "po"},
		[]string{"NAME", "READY", "STATUS", "RESTARTS", "AGE", "NODE", "IP"},
		func(c kubernetes.Interface, ns string) typedcorev1.PodInterface { return c.CoreV1().Pods(ns) },
		func(l *corev1.PodList) []corev1.Pod { return l.Items },
		podRow, describePod), podOrder),
	define("Node", false, []string{"node", "nodes", "no"},
		[]string{"NAME", "STATUS", "ROLES", "AGE", "VERSION", "INTERNAL-IP"},
		func(c kubernetes.Interface, _ string) typedcorev1.NodeInterface { return c.CoreV1().Nodes() },
		func(l *corev1.NodeList) []corev1.Node { return l.Items },
		nodeRow, describeNode),
	define("Deployment", true, []string{"deployment", "deployments", "deploy"},
		[]string{"NAME", "READY", "UP-TO-DATE", "AVAILABLE", "AGE", "IMAGES"},
		func(c kubernetes.Interface, ns string) typedappsv1.DeploymentInterface {
			return c.AppsV1().Deployments(ns)
		},
		func(l *appsv1.DeploymentList) []appsv1.Deployment { return l.Items },
		deploymentRow, describeDeployment),
	define("StatefulSet", true, []string{"statefulset", "statefulsets", "sts"},
		[]string{"NAME", "READY", "UP-TO-DATE", "AGE", "IMAGES"},
		func(c kubernetes.Interface, ns string) typedappsv1.StatefulSetInterface {
			return c.AppsV1().StatefulSets(ns)
		},
		func(l *appsv1.StatefulSetList) []appsv1.StatefulSet { return l.Items },
		statefulSetRow, describeStatefulSet),
	define("DaemonSet", true, []string{"daemonset", "daemonsets", "ds"},
		[]string{"NAME", "DESIRED", "CURRENT", "READY", "UP-TO-DATE", "AVAILABLE", "AGE", "IMAGES"},
		func(c kubernetes.Interface, ns string) typedappsv1.DaemonSetInterface {
			return c.AppsV1().DaemonSets(ns)
		},
		func(l *appsv1.DaemonSetList) []appsv1.DaemonSet { return l.Items },
		daemonSetRow, describeDaemonSet),
	define("ReplicaSet", true, []string{"replicaset", "replicasets", "rs"},
		[]string{"NAME", "DESIRED", "CURRENT", "READY", "AGE", "OWNER"},
		func(c kubernetes.Interface, ns string) typedappsv1.ReplicaSetInterface {
			return c.AppsV1().ReplicaSets(ns)
		},
		func(l *appsv1.ReplicaSetList) []appsv1.ReplicaSet { return l.Items },
		replicaSetRow, describeReplicaSet),
	define("Job", true, []string{"job", "jobs"},
		[]string{"NAME", "STATUS", "COMPLETIONS", "ACTIVE", "FAILED", "AGE"},
		func(c kubernetes.Interface, ns string) typedbatchv1.JobInterface { return c.BatchV1().Jobs(ns) },
		func(l *batchv1.JobList) []batchv1.Job { return l.Items },
		jobRow, describeJob),
	define("CronJob", true, []string{"cronjob", "cronjobs", "cj"},
		[]string{"NAME", "SCHEDULE", "SUSPEND", "ACTIVE", "LAST-SCHEDULE", "AGE"},
		func(c kubernetes.Interface, ns string) typedbatchv1.CronJobInterface { return c.BatchV1().CronJobs(ns) },
		func(l *batchv1.CronJobList) []batchv1.CronJob { return l.Items },
		cronJobRow, describeCronJob),
	define("Service", true, []string{"service", "services", "svc"},
		[]string{"NAME", "TYPE", "CLUSTER-IP", "PORTS", "SELECTOR", "AGE"},
		func(c kubernetes.Interface, ns string) typedcorev1.ServiceInterface { return c.CoreV1().Services(ns) },
		func(l *corev1.ServiceList) []corev1.Service { return l.Items },
		serviceRow, describeService),
	define("EndpointSlice", true, []string{"endpointslice", "endpointslices"},
		[]string{"NAME", "SERVICE", "ADDRESSTYPE", "PORTS", "ENDPOINTS", "AGE"},
		func(c kubernetes.Interface, ns string) typeddiscoveryv1.EndpointSliceInterface {
			return c.DiscoveryV1().EndpointSlices(ns)
		},
		func(l *discoveryv1.EndpointSliceList) []discoveryv1.EndpointSlice { return l.Items },
		endpointSliceRow, describeEndpointSlice),
	define("HorizontalPodAutoscaler", true, []string{"horizontalpodautoscaler", "horizontalpodautoscalers", "hpa"},
		[]string{"NAME", "REFERENCE", "TARGETS", "MIN", "MAX", "REPLICAS", "AGE"},
		func(c kubernetes.Interface, ns string) typedautoscalingv2.HorizontalPodAutoscalerInterface {
			return c.AutoscalingV2().HorizontalPodAutoscalers(ns)
		},
		func(l *autoscalingv2.HorizontalPodAutoscalerList) []autoscalingv2.HorizontalPodAutoscaler {
			return l.Items
		},
		hpaRow, describeHPA),
	define("PersistentVolumeClaim", true, []string{"persistentvolumeclaim", "persistentvolumeclaims", "pvc"},
		[]string{"NAME", "STATUS", "VOLUME", "CAPACITY", "ACCESS", "STORAGECLASS", "AGE"},
		func(c kubernetes.Interface, ns string) typedcorev1.PersistentVolumeClaimInterface {
			return c.CoreV1().PersistentVolumeClaims(ns)
		},
		func(l *corev1.PersistentVolumeClaimList) []corev1.PersistentVolumeClaim { return l.Items },
		pvcRow, describePVC),
	withOrder(define("Event", true, []string{"event", "events", "ev"},
		[]string{"LAST", "TYPE", "REASON", "OBJECT", "COUNT", "MESSAGE"},
		func(c kubernetes.Interface, ns string) typedcorev1.EventInterface { return c.CoreV1().Events(ns) },
		func(l *corev1.EventList) []corev1.Event { return l.Items },
		eventRow, describeEvent), eventOrder),
}

func withOrder(s *spec, order func(a, b object) int) *spec {
	s.order = order
	return s
}

// byAlias 是小写的名字（单数、复数、简写）到 spec。
var byAlias = func() map[string]*spec {
	m := make(map[string]*spec)
	for _, s := range specs {
		for _, a := range s.aliases {
			if _, dup := m[a]; dup {
				panic("kube: duplicate kind alias " + a)
			}
			m[a] = s
		}
	}
	return m
}()

// allowedKinds 是写进工具说明和错误信息里的白名单。
var allowedKinds = func() string {
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.kind
	}
	return strings.Join(names, ", ")
}()

// lookupKind 按白名单解析 kind，大小写不敏感。名单外的（包括 Secret、ConfigMap）一律拒绝。
func lookupKind(kind string) (*spec, error) {
	k := strings.ToLower(strings.TrimSpace(kind))
	if k == "" {
		return nil, fmt.Errorf("kind is required; allowed kinds: %s", allowedKinds)
	}
	if s, ok := byAlias[k]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("kind %q is not allowed: this agent can only read %s", kind, allowedKinds)
}

// byNamespaceName 是默认的列表顺序。
func byNamespaceName(a, b object) int {
	if c := strings.Compare(a.GetNamespace(), b.GetNamespace()); c != 0 {
		return c
	}
	return strings.Compare(a.GetName(), b.GetName())
}

func (s *spec) sort(items []object) {
	order := s.order
	if order == nil {
		order = byNamespaceName
	}
	slices.SortStableFunc(items, order)
}
