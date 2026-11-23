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

	"github.com/palemoky/oppenheimer/services/agent/internal/loop"
	"github.com/palemoky/oppenheimer/services/agent/internal/tool"
)

// toolCmd 手工调一次工具，打印模型会看到的那份（裁剪后的）结果。不调模型，不花钱。
// 用来看裁剪的效果、对着真的后端核对工具；不带工具名时列出登记了的工具。
func toolCmd(args []string, stdout, stderr io.Writer) int {
	var cfg serveConfig
	fs := flag.NewFlagSet("tool", flag.ContinueOnError)
	fs.SetOutput(stderr)
	toolFlags(fs, &cfg)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	reg, err := buildTools(cfg)
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
	ctx, cancel := context.WithTimeout(ctx, loop.DefaultLimits().ToolTimeout)
	defer cancel()
	began := time.Now()
	res, err := t.Call(ctx, jsontext.Value(input))
	if err != nil {
		fmt.Fprintln(stdout, "Error:", err)
		return 1
	}
	text := tool.Cap(res.Text)
	fmt.Fprintln(stdout, text)
	fmt.Fprintf(stderr, "-- %d bytes shown to the model, %d bytes raw, %s\n", len(text), len(res.Raw), time.Since(began).Round(time.Millisecond))
	return 0
}

// toolFlags 是 serve 和 tool 共用的、工具后端的地址。
func toolFlags(fs *flag.FlagSet, cfg *serveConfig) {
	fs.StringVar(&cfg.prometheusURL, "prometheus-url", os.Getenv("PROMETHEUS_URL"), "Prometheus base URL (default $PROMETHEUS_URL); empty disables the prom_* tools")
	fs.StringVar(&cfg.lokiURL, "loki-url", os.Getenv("LOKI_URL"), "Loki base URL (default $LOKI_URL); empty disables the loki_* tools")
	fs.StringVar(&cfg.kubeconfig, "kubeconfig", "", "kubeconfig path for the kube_* tools (local use)")
	fs.BoolVar(&cfg.inCluster, "in-cluster", false, "use the pod's ServiceAccount for the kube_* tools")
}
