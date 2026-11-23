package kube

import (
	"slices"
	"testing"
)

func TestRedactFlags(t *testing.T) {
	args := []string{
		"--listen=:8080",
		"--db-password=hunter2",
		"-dsn=app:hunter2@tcp(mysql:3306)/orders",
		"API_KEY=sk-123",
		"--redis-url=redis://default:hunter2@redis-gate:6379/0",
		"--log-level=debug",
		"serve",
	}
	redactFlags(args)
	want := []string{
		"--listen=:8080",
		"--db-password=<redacted>",
		"-dsn=<redacted>",
		"API_KEY=<redacted>",
		"--redis-url=redis://default:<redacted>@redis-gate:6379/0",
		"--log-level=debug",
		"serve",
	}
	if !slices.Equal(args, want) {
		t.Errorf("args = %q\nwant   %q", args, want)
	}
}
