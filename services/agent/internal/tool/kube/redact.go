package kube

import (
	"regexp"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// redacted 替换掉不能落盘的值。
const redacted = "<redacted>"

// lastApplied 是 kubectl apply 留下的注解，里面是上一次提交的整个对象，环境变量的值也在里面。
const lastApplied = "kubectl.kubernetes.io/last-applied-configuration"

// podSpecOf 返回对象里的 Pod 规格（Pod 本身的，或者工作负载的模板里的），没有就返回 nil。
// 返回的是指向对象内部的指针，改它就是改对象。
func podSpecOf(o object) *corev1.PodSpec {
	switch v := o.(type) {
	case *corev1.Pod:
		return &v.Spec
	case *appsv1.Deployment:
		return &v.Spec.Template.Spec
	case *appsv1.StatefulSet:
		return &v.Spec.Template.Spec
	case *appsv1.DaemonSet:
		return &v.Spec.Template.Spec
	case *appsv1.ReplicaSet:
		return &v.Spec.Template.Spec
	case *batchv1.Job:
		return &v.Spec.Template.Spec
	case *batchv1.CronJob:
		return &v.Spec.JobTemplate.Spec.Template.Spec
	}
	return nil
}

// redact 就地抹掉对象里不能出现在 Raw 里的值：环境变量的值（可能是带密码的 DSN），
// 以及 last-applied-configuration 注解（里面有一份完整的旧对象，环境变量的值也在里面）。
// 对象是这次调用自己解码出来的，就地改不影响别人。
func redact(o object) {
	if ann := o.GetAnnotations(); ann != nil {
		if _, ok := ann[lastApplied]; ok {
			ann[lastApplied] = redacted
		}
	}
	ps := podSpecOf(o)
	if ps == nil {
		return
	}
	redactEnv := func(env []corev1.EnvVar) {
		for i := range env {
			if env[i].Value != "" {
				env[i].Value = redacted
			}
		}
	}
	for i := range ps.InitContainers {
		c := &ps.InitContainers[i]
		redactEnv(c.Env)
		redactFlags(c.Command)
		redactFlags(c.Args)
	}
	for i := range ps.Containers {
		c := &ps.Containers[i]
		redactEnv(c.Env)
		redactFlags(c.Command)
		redactFlags(c.Args)
	}
	for i := range ps.EphemeralContainers {
		c := &ps.EphemeralContainers[i]
		redactEnv(c.Env)
		redactFlags(c.Command)
		redactFlags(c.Args)
	}
}

// secretFlag 认出命令行里“名字像密钥”的参数：--password=x、-dsn=x、TOKEN=x，或者值里带用户名密码的地址（scheme://user:pass@host）。
var secretFlag = regexp.MustCompile(`(?i)^(.*(?:pass(?:word|wd)?|secret|token|dsn|credential|api[-_]?key)[^=]*=).+$|^(.*://[^/@:]+:)[^@/]+(@.*)$`)

// redactFlags 就地抹掉命令和参数里像密钥的值。命令行本身留着（排查时有用），只换掉值。
// 这是尽力而为：“--password x”这种分成两个参数的写法认不出来，所以密钥不该走命令行传。
func redactFlags(args []string) {
	for i, a := range args {
		m := secretFlag.FindStringSubmatch(a)
		switch {
		case m == nil:
		case m[1] != "":
			args[i] = m[1] + redacted
		default:
			args[i] = m[2] + redacted + m[3]
		}
	}
}
