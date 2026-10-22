package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/vennictus/sceptre/internal/bench"
)

func TestCLIEmitsJSONAndKeepsProgressSeparate(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-rows", "4", "-payloads", "8", "-operations", "5", "-batch", "2", "-dir", t.TempDir()}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit=%d stderr=%s", code, &stderr)
	}
	var report bench.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != 9 || !bytes.Contains(stderr.Bytes(), []byte("complete")) {
		t.Fatalf("missing results/progress")
	}
}

func TestCLIRejectsMalformedDatasetFlags(t *testing.T) {
	for _, args := range [][]string{{"-rows", "1,1"}, {"-payloads", "bad"}, {"unexpected"}, {"-operations", "0"}} {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code == 0 || stdout.Len() != 0 {
			t.Fatalf("args=%v code=%d stdout=%s", args, code, &stdout)
		}
	}
}
