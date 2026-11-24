package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/palemoky/oppenheimer/services/agent/internal/trigger"
)

// askTimeout 比一次运行最坏的总时长（见 loop.Limits 的说明）再宽一些。
const askTimeout = 20 * time.Minute

// askCmd 向常驻的 agent 提一个问题并打印报告。诊断在 agent 那一侧跑，记录也留在那里。
func askCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("ask", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var addr string
	var force bool
	fs.BoolVar(&force, "force", false, "run now even during the model's peak pricing hours")
	fs.StringVar(&addr, "addr", "http://127.0.0.1:8080", "base URL of a running agent (kubectl port-forward in the cluster case)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	question := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if question == "" {
		fmt.Fprintln(stderr, `usage: agent ask [-addr URL] [-force] "question"`)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, askTimeout)
	defer cancel()

	reply, err := ask(ctx, addr, question, force)
	if err != nil {
		fmt.Fprintln(stderr, "ask failed:", err)
		return 1
	}
	fmt.Fprintln(stdout, reply.Report)
	fmt.Fprintf(stderr, "run %s finished: %s\n", reply.RunID, reply.Outcome)
	return 0
}

func ask(ctx context.Context, addr, question string, force bool) (trigger.Reply, error) {
	var reply trigger.Reply
	body, err := json.Marshal(map[string]any{"question": question, "force": force})
	if err != nil {
		return reply, fmt.Errorf("encode question: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(addr, "/")+"/ask", strings.NewReader(string(body)))
	if err != nil {
		return reply, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// 提问会花钱，而且不是幂等的：不重试，失败了由人决定要不要再问一次。
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return reply, fmt.Errorf("post /ask: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return reply, fmt.Errorf("agent answered %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}
	if err := json.UnmarshalRead(resp.Body, &reply); err != nil {
		return reply, fmt.Errorf("decode reply: %w", err)
	}
	return reply, nil
}
