package main

import (
	"slices"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	t.Setenv("MYSQL_DSNS", "")
	t.Setenv("MYSQL_REPLICA_DSNS", "")
	t.Setenv("ARCHIVE_DSN", "")
	if _, err := loadConfig(); err == nil {
		t.Error("loadConfig without MYSQL_DSNS succeeded")
	}

	t.Setenv("MYSQL_DSNS", "a, b")
	if _, err := loadConfig(); err == nil {
		t.Error("loadConfig without ARCHIVE_DSN succeeded")
	}

	t.Setenv("ARCHIVE_DSN", "c")
	cfg, err := loadConfig()
	if err != nil || !slices.Equal(cfg.dsns, []string{"a", "b"}) || cfg.replicaDSNs != nil || cfg.archiveDSN != "c" {
		t.Fatalf("loadConfig = %+v, %v", cfg, err)
	}

	t.Setenv("MYSQL_REPLICA_DSNS", "ra")
	if _, err := loadConfig(); err == nil {
		t.Error("loadConfig with fewer replicas than databases succeeded")
	}
	t.Setenv("MYSQL_DSNS", "a,,b")
	if _, err := loadConfig(); err == nil {
		t.Error("loadConfig with an empty DSN succeeded")
	}
}

func TestRunRejectsDryRunWithReclaim(t *testing.T) {
	if err := run(t.Context(), nil, options{dryRun: true, reclaim: true}); err == nil {
		t.Error("run with -dry-run and -reclaim succeeded")
	}
}
