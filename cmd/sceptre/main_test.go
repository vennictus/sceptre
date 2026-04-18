package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunPrintsUsageWithoutArgs(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := run(nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run() exit code = %d, want 0", code)
	}
	if stderr.Len() != 0 {
		t.Fatalf("run() wrote stderr = %q, want empty", stderr.String())
	}
	if !strings.Contains(stdout.String(), "repository scaffold only") {
		t.Fatalf("run() stdout = %q, want scaffold message", stdout.String())
	}
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := run([]string{"unknown"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("run() exit code = %d, want 2", code)
	}
	if stdout.Len() != 0 {
		t.Fatalf("run() wrote stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), `unknown command "unknown"`) {
		t.Fatalf("run() stderr = %q, want unknown command message", stderr.String())
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Fatalf("run() stderr = %q, want usage text", stderr.String())
	}
}
