// agent 是最小诊断 Agent：告警触发或者手动提问时，用只读工具查一轮，交出带证据的报告，全过程落盘。
// 设计见 docs/design/3d-diagnosis-agent.md。
//
//	agent serve    常驻：收 Alertmanager 的 webhook 和手动提问，跑诊断
//	agent ask      向常驻的 agent 提一个问题，打印报告
//	agent replay   按原样回放一次运行的记录（不调 API，不碰集群）
package main

import (
	"fmt"
	"io"
	"os"
)

const usage = `usage: agent <command> [flags]

commands:
  serve    run the webhook receiver and diagnosis worker
  ask      ask a running agent a question and print its report
  replay   print a recorded run step by step
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "serve":
		return serveCmd(args[1:], stderr)
	case "ask":
		return askCmd(args[1:], stdout, stderr)
	case "replay":
		return replayCmd(args[1:], stdout, stderr)
	case "-h", "--help", "help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
}
