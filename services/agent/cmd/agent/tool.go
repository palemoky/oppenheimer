package main

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/services/agent/internal/loop"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool/pprof"
)

// toolCmd 手工调一次工具，打印模型会看到的那份（裁剪后的）结果。不调模型，不花钱。
// 用来看裁剪的效果、对着真的后端核对工具；不带工具名时列出登记了的工具。
func toolCmd(args []string, stdout, stderr io.Writer) int {
	var cfg serveConfig
	var rawOut string
	fs := flag.NewFlagSet("tool", flag.ContinueOnError)
	fs.SetOutput(stderr)
	toolFlags(fs, &cfg)
	fs.StringVar(&rawOut, "raw-out", "", "also write the raw result to this file (for pprof_capture: a .pb.gz for go tool pprof)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	// 工具自己的留痕（pprof_capture 每次抓取记一行）打到 stderr，和结果分开。
	logger := logx.New(serviceName, stderr, logx.ParseLevel(os.Getenv(logx.LevelEnv)))
	reg, err := buildTools(cfg, logger)
	if err != nil {
		fmt.Fprintln(stderr, "tool:", err)
		return 1
	}
	if fs.NArg() == 0 {
		for _, d := range reg.Defs() {
			fmt.Fprintf(stdout, "%s\n    %s\n", d.Name, d.Description)
		}
		return 0
	}
	t, ok := reg.Get(fs.Arg(0))
	if !ok {
		fmt.Fprintf(stderr, "tool: %q is not registered (is its backend URL set?)\n", fs.Arg(0))
		return 2
	}
	input := "{}"
	if fs.NArg() > 1 {
		input = fs.Arg(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	// 和循环里一样的时限，看到的超时行为才是真的。
	ctx, cancel := context.WithTimeout(ctx, tool.TimeoutOf(t, loop.DefaultLimits().ToolTimeout))
	defer cancel()
	began := time.Now()
	res, err := t.Call(ctx, jsontext.Value(input))
	if err != nil {
		fmt.Fprintln(stdout, "Error:", err)
		return 1
	}
	text := tool.Cap(res.Text)
	fmt.Fprintln(stdout, text)
	if rawOut != "" && len(res.Raw) > 0 {
		if err := os.WriteFile(rawOut, res.Raw, 0o600); err != nil {
			fmt.Fprintln(stderr, "tool: write raw result:", err)
			return 1
		}
	}
	fmt.Fprintf(stderr, "-- %d bytes shown to the model, %d bytes raw, %s\n", len(text), len(res.Raw), time.Since(began).Round(time.Millisecond))
	return 0
}

// toolFlags 是 serve 和 tool 共用的、工具后端的地址。
func toolFlags(fs *flag.FlagSet, cfg *serveConfig) {
	fs.StringVar(&cfg.prometheusURL, "prometheus-url", os.Getenv("PROMETHEUS_URL"), "Prometheus base URL (default $PROMETHEUS_URL); empty disables the prom_* tools")
	fs.StringVar(&cfg.lokiURL, "loki-url", os.Getenv("LOKI_URL"), "Loki base URL (default $LOKI_URL); empty disables the loki_* tools")
	fs.StringVar(&cfg.kubeconfig, "kubeconfig", "", "kubeconfig path for the kube_* and pprof_capture tools (local use)")
	fs.BoolVar(&cfg.inCluster, "in-cluster", false, "use the pod's ServiceAccount for the kube_* and pprof_capture tools")
	fs.StringVar(&cfg.pprofNamespace, "pprof-namespace", pprof.DefaultNamespace, "the only namespace pprof_capture may port-forward into")
	fs.StringVar(&cfg.hiddenNamespaces, "hidden-namespaces", tool.DefaultHiddenNamespaces,
		"comma-separated namespaces every tool treats as nonexistent (the fault injection tooling); empty hides none")
	fs.StringVar(&cfg.hiddenAPIGroups, "hidden-api-groups", tool.DefaultHiddenAPIGroups,
		"comma-separated API groups whose objects' events kube tools drop; empty hides none")
	fs.IntVar(&cfg.pprofPort, "pprof-port", pprof.DefaultPort, "debug (pprof) port on the pod's loopback, as in the services' DEBUG_ADDR")
}
