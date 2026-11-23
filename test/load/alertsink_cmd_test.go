package main

import (
	"bytes"
	"testing"
)

func TestAlertsinkCmdFlags(t *testing.T) {
	var stderr bytes.Buffer
	if code := alertsinkCmd([]string{"-h"}, &stderr); code != 0 {
		t.Errorf("-h: exit %d, want 0", code)
	}
	if code := alertsinkCmd([]string{"-no-such-flag"}, &stderr); code != 2 {
		t.Errorf("bad flag: exit %d, want 2", code)
	}
}
