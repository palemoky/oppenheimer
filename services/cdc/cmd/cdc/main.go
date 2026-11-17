// Command cdc 是 CDC 服务的入口：每个源库一条流水线，读 binlog 把订单表的行变更发到 Kafka。
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/palemoky/oppenheimer/api/eventx"
	"github.com/palemoky/oppenheimer/pkg/app"
	"github.com/palemoky/oppenheimer/pkg/kafkax"
	"github.com/palemoky/oppenheimer/pkg/logx"
	"github.com/palemoky/oppenheimer/pkg/otelx"
	"github.com/palemoky/oppenheimer/services/cdc/internal/capture"
)

const serviceName = "cdc"

// kafkaStartTimeout 是启动时连接 Kafka、建 topic、写墓碑的时限。
const kafkaStartTimeout = 30 * time.Second

// recordDeliveryTimeout 是一条消息从进入缓冲到被确认的最长时间，超过即视为最终失败，流水线退出。
// 要短于 capture 里等待 Flush 的时限，失败才会以发送错误的形式出现，而不是 Flush 超时。
const recordDeliveryTimeout = time.Minute

type config struct {
	httpAddr           string
	names              []string
	dsns               []string
	serverIDs          []uint32
	kafkaBrokers       []string
	kafkaPrefix        string
	checkpointInterval time.Duration
	snapshotBatch      int
}

// rules 是捕获规则。目前只有订单表；内部的包不认识这些名字，都从这里传进去。
func rules(prefix string) []capture.Rule {
	return []capture.Rule{{
		Pattern:       regexp.MustCompile(`^orders_(\d+)$`),
		Table:         "orders",
		Topic:         eventx.Topic(prefix, eventx.CDCOrders),
		KeyColumn:     "order_id",
		VersionColumn: "version",
	}}
}

func main() {
	resetName := flag.String("reset-checkpoint", "", "给这条流水线的位点写墓碑后退出，下次启动时重做全量")
	printName := flag.String("print-checkpoint", "",
		"把这条流水线已确认的位点（GTID 集合）打印到标准输出后退出；没有位点或全量还没做完时报错。日志改写到标准错误")
	flag.Parse()

	// 打印位点时标准输出只留那一行 GTID 集合，方便脚本直接取用（scripts/expand.sh 用它判断 CDC 是否越过围栏）。
	logOut := os.Stdout
	if *printName != "" {
		logOut = os.Stderr
	}
	logger := logx.New(serviceName, logOut, logx.LevelFromEnv())
	slog.SetDefault(logger)

	if err := run(context.Background(), logger, *resetName, *printName); err != nil {
		logger.Error("service exited with error", "error", err)
		os.Exit(1)
	}
}

func splitList(s string) []string {
	return strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' })
}

func loadConfig() (config, error) {
	cfg := config{
		httpAddr:     cmp.Or(os.Getenv("HTTP_ADDR"), ":8085"),
		names:        splitList(os.Getenv("CDC_SOURCE_NAMES")),
		dsns:         splitList(os.Getenv("CDC_SOURCE_DSNS")),
		kafkaBrokers: splitList(cmp.Or(os.Getenv("KAFKA_BROKERS"), "127.0.0.1:9092")),
		kafkaPrefix:  os.Getenv("KAFKA_PREFIX"),
	}
	for _, s := range splitList(os.Getenv("CDC_SERVER_IDS")) {
		id, err := strconv.ParseUint(s, 10, 32)
		if err != nil || id == 0 {
			return config{}, fmt.Errorf("invalid server id %q in CDC_SERVER_IDS", s)
		}
		cfg.serverIDs = append(cfg.serverIDs, uint32(id))
	}
	if len(cfg.names) == 0 {
		return config{}, errors.New("CDC_SOURCE_NAMES is empty")
	}
	if len(cfg.dsns) != len(cfg.names) || len(cfg.serverIDs) != len(cfg.names) {
		return config{}, fmt.Errorf("CDC_SOURCE_NAMES (%d), CDC_SOURCE_DSNS (%d) and CDC_SERVER_IDS (%d) must have the same length",
			len(cfg.names), len(cfg.dsns), len(cfg.serverIDs))
	}

	cfg.checkpointInterval = capture.DefaultCheckpointInterval
	if v := os.Getenv("CDC_CHECKPOINT_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return config{}, fmt.Errorf("invalid CDC_CHECKPOINT_INTERVAL %q", v)
		}
		cfg.checkpointInterval = d
	}
	cfg.snapshotBatch = capture.DefaultSnapshotBatch
	if v := os.Getenv("CDC_SNAPSHOT_BATCH"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return config{}, fmt.Errorf("invalid CDC_SNAPSHOT_BATCH %q", v)
		}
		cfg.snapshotBatch = n
	}
	return cfg, nil
}

// producerOpts 是发送行变更和位点用的选项：acks=all，幂等（franz-go 默认开启，依赖 acks=all），
// 投递有时限，超时视为最终失败。
func producerOpts() []kgo.Opt {
	return []kgo.Opt{
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordDeliveryTimeout(recordDeliveryTimeout),
	}
}

func run(ctx context.Context, logger *slog.Logger, resetName, printName string) error {
	// 链路和指标（设计文档 1d 第 8 节）。最先装配、最后关闭：其他组件停止时产生的 span 也要发出去。
	// 没设 OTEL_EXPORTER_OTLP_ENDPOINT 时不导出，只让日志带上 trace id。
	shutdownTelemetry, err := otelx.Setup(ctx, serviceName, logger)
	if err != nil {
		return err
	}
	defer otelx.Shutdown(ctx, shutdownTelemetry, logger)

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	kafkaCfg := kafkax.Config{Brokers: cfg.kafkaBrokers, ClientID: serviceName}
	checkpointTopic := eventx.Topic(cfg.kafkaPrefix, eventx.CDCCheckpoint)

	startCtx, cancelStart := context.WithTimeout(ctx, kafkaStartTimeout)
	defer cancelStart()
	writer, err := kafkax.NewClient(startCtx, kafkaCfg, producerOpts()...)
	if err != nil {
		return fmt.Errorf("connect kafka: %w", err)
	}
	defer writer.Close()
	if err := kafkax.EnsureTopics(startCtx, writer,
		kafkax.TopicSpec{
			Name: eventx.Topic(cfg.kafkaPrefix, eventx.CDCOrders), Partitions: eventx.Partitions,
			Configs: map[string]string{"cleanup.policy": "compact"},
		},
		kafkax.TopicSpec{
			Name: checkpointTopic, Partitions: 1,
			Configs: map[string]string{"cleanup.policy": "compact"},
		},
	); err != nil {
		return fmt.Errorf("ensure kafka topics: %w", err)
	}
	checkpoints := capture.NewCheckpointStore(kafkaCfg, checkpointTopic, writer)

	if resetName != "" {
		if err := checkpoints.Reset(startCtx, resetName); err != nil {
			return err
		}
		logger.InfoContext(ctx, "checkpoint reset; the pipeline will redo the snapshot on next start", "pipeline", resetName)
		return nil
	}

	if printName != "" {
		cp, err := checkpoints.Load(startCtx, printName)
		if err != nil {
			return err
		}
		switch {
		case cp == nil:
			return fmt.Errorf("pipeline %s has no checkpoint", printName)
		case cp.GetSnapshot() != nil:
			// 全量期间位点停在全量开始前的集合，不代表增量已经读到那里之后的任何位置。
			return fmt.Errorf("pipeline %s is still in its snapshot (at table %s)", printName, cp.GetSnapshot().GetTable())
		}
		// MySQL 输出的 GTID 集合在多个来源之间带换行，压成一行。
		fmt.Println(strings.Join(strings.Fields(cp.GetGtidSet()), ""))
		return nil
	}

	// 每条流水线独占一个生产者：推进位点前的 Flush 只等自己的消息，两条流水线互不拖累。
	components := make([]app.Component, 0, len(cfg.names)+1)
	for i, name := range cfg.names {
		producer, err := kafkax.NewClient(startCtx, kafkaCfg, producerOpts()...)
		if err != nil {
			return fmt.Errorf("connect kafka for %s: %w", name, err)
		}
		defer producer.Close()
		p, err := capture.New(capture.Config{
			Name:               name,
			DSN:                cfg.dsns[i],
			ServerID:           cfg.serverIDs[i],
			Rules:              rules(cfg.kafkaPrefix),
			Producer:           producer,
			Checkpoints:        checkpoints,
			CheckpointInterval: cfg.checkpointInterval,
			SnapshotBatch:      cfg.snapshotBatch,
			Logger:             logger,
		})
		if err != nil {
			return err
		}
		components = append(components, p)
	}

	httpServer, err := app.ListenHTTP(ctx, "http", cfg.httpAddr, app.HealthHandler(serviceName))
	if err != nil {
		return err
	}
	// 调试端口（pprof）：DEBUG_ADDR 没设就不开，集群里只在回环上监听，经 kubectl port-forward 访问。
	debugServer, err := app.ListenDebug(ctx, logger)
	if err != nil {
		return err
	}
	logger.InfoContext(ctx, "listening", "http_addr", httpServer.Addr().String(), "pipelines", cfg.names)

	// app.Run 按相反顺序停止：健康检查先停，然后各流水线停止读取、落最后一次位点，调试端口（开了的话）最后停。
	return app.Run(ctx, logger, app.WithDebug(debugServer, append(components, httpServer)...)...)
}
