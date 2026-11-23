package main

import (
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/palemoky/oppenheimer/services/agent/internal/runlog"
)

// replayCmd 把一次运行的记录按顺序打印出来：模型说了什么、调了什么、看到了什么。
// 只读磁盘，不调 API，不碰集群。不带运行号时列出全部运行。
func replayCmd(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var dir string
	var full bool
	fs.StringVar(&dir, "runs-dir", "runs", "directory with run records")
	fs.BoolVar(&full, "full", false, "print tool results in full instead of the first lines")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	store, err := runlog.Open(dir, slog.New(slog.DiscardHandler))
	if err != nil {
		fmt.Fprintln(stderr, "replay:", err)
		return 1
	}
	defer func() { _ = store.Close() }()

	if fs.NArg() == 0 {
		runs, err := store.List()
		if err != nil {
			fmt.Fprintln(stderr, "replay:", err)
			return 1
		}
		for _, r := range runs {
			fmt.Fprintf(stdout, "%s  %-5s  %-18s  turns=%-2d  $%.4f\n", r.ID, r.Trigger, r.Outcome, r.Turns, r.CostUSD)
		}
		return 0
	}
	events, err := store.Events(fs.Arg(0))
	if err != nil {
		fmt.Fprintln(stderr, "replay:", err)
		return 1
	}
	for _, ev := range events {
		printEvent(stdout, ev, full)
	}
	return 0
}

// replayEvent 是各种事件里回放要用到的字段的并集（事件的定义在 internal/loop）。
type replayEvent struct {
	Turn     int      `json:"turn"`
	Text     string   `json:"text"`
	Thinking []string `json:"thinking"`
	Stop     string   `json:"stop_reason"`
	Model    string   `json:"model"`
	Usage    struct {
		Input, Output         int64
		CacheWrite, CacheRead int64
	} `json:"usage"`
	CostUSD float64 `json:"cost_usd"`
	Calls   []struct {
		Name  string `json:"Name"`
		Input any    `json:"Input"`
	} `json:"calls"`
	Step       string `json:"step"`
	Tool       string `json:"tool"`
	Input      any    `json:"input"`
	Result     string `json:"result"`
	DurationMS int64  `json:"duration_ms"`
	Limit      string `json:"limit"`
	Reason     string `json:"reason"`
	Outcome    string `json:"outcome"`
	Error      string `json:"error"`
}

const replayExcerptLines = 12

func printEvent(w io.Writer, ev runlog.Event, full bool) {
	var d replayEvent
	if err := json.Unmarshal(ev.Data, &d, json.MatchCaseInsensitiveNames(true)); err != nil {
		fmt.Fprintf(w, "%s  %s  (unreadable: %v)\n", ev.Time.Format("15:04:05"), ev.Kind, err)
		return
	}
	stamp := ev.Time.Format("15:04:05")
	switch ev.Kind {
	case "model_request":
		if d.Text != "" {
			fmt.Fprintf(w, "\n%s  ── turn %d ── to model:\n%s\n", stamp, d.Turn, indent(d.Text))
		} else {
			fmt.Fprintf(w, "\n%s  ── turn %d ── tool results sent to model\n", stamp, d.Turn)
		}
	case "model_response":
		fmt.Fprintf(w, "%s  model (%s, stop=%s, in=%d out=%d cache_read=%d cache_write=%d, $%.4f, %dms)\n",
			stamp, d.Model, d.Stop, d.Usage.Input, d.Usage.Output, d.Usage.CacheRead, d.Usage.CacheWrite, d.CostUSD, d.DurationMS)
		for _, t := range d.Thinking {
			fmt.Fprintf(w, "  [thinking]\n%s\n", indent(t))
		}
		if d.Text != "" {
			fmt.Fprintln(w, indent(d.Text))
		}
		for _, c := range d.Calls {
			in, _ := json.Marshal(c.Input)
			fmt.Fprintf(w, "  → %s %s\n", c.Name, in)
		}
	case "tool_call":
		in, _ := json.Marshal(d.Input)
		fmt.Fprintf(w, "%s  [%s] %s %s → %s (%dms)\n", stamp, d.Step, d.Tool, in, d.Result, d.DurationMS)
		fmt.Fprintln(w, indent(excerpt(d.Text, full)))
	case "limit_hit":
		fmt.Fprintf(w, "%s  !! limit hit: %s\n", stamp, d.Limit)
	case "report_rejected":
		fmt.Fprintf(w, "%s  !! report rejected: %s\n", stamp, d.Reason)
	case "end":
		fmt.Fprintf(w, "\n%s  end: %s", stamp, d.Outcome)
		if d.Error != "" {
			fmt.Fprintf(w, " (%s)", d.Error)
		}
		fmt.Fprintln(w)
	default:
		fmt.Fprintf(w, "%s  %s %s\n", stamp, ev.Kind, ev.Data)
	}
}

func excerpt(s string, full bool) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if full || len(lines) <= replayExcerptLines {
		return strings.Join(lines, "\n")
	}
	return strings.Join(lines[:replayExcerptLines], "\n") + fmt.Sprintf("\n… %d more lines (use -full)", len(lines)-replayExcerptLines)
}

func indent(s string) string {
	return "    " + strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n    ")
}
