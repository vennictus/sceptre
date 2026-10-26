package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunUpgradeConvertsV1Database(t *testing.T) {
	original, err := os.ReadFile(filepath.Join("..", "..", "internal", "upgrade", "testdata", "v1.db"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "app.db")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	if code := run([]string{"sql", path, "select * from users limit 1"}, &stdout, &stderr); code == 0 || !strings.Contains(stderr.String(), "sceptre upgrade") {
		t.Fatalf("v1 file before upgrade: exit %d, stderr %q", code, stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"upgrade", path}, &stdout, &stderr); code != 0 {
		t.Fatalf("upgrade exit %d, stderr %q", code, stderr.String())
	}
	for _, want := range []string{"format=v1->v3", "keys=", "backup=" + path + ".v1.bak", "verified="} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("upgrade output missing %q:\n%s", want, stdout.String())
		}
	}

	stdout.Reset()
	if code := run([]string{"check", path}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "status=ok") {
		t.Fatalf("check after upgrade: exit %d\n%s%s", code, stdout.String(), stderr.String())
	}

	stderr.Reset()
	if code := run([]string{"upgrade", path}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "already uses the storage engine format") {
		t.Fatalf("second upgrade: exit %d, stderr %q", code, stderr.String())
	}
	if code := run([]string{"upgrade"}, &stdout, &stderr); code != 2 {
		t.Fatalf("upgrade without a path: exit %d", code)
	}
}
