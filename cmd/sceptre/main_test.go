package main

import (
	"bytes"
	"path/filepath"
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
	if !strings.Contains(stdout.String(), "sceptre sql <db-path>") {
		t.Fatalf("run() stdout = %q, want sql usage", stdout.String())
	}
	if !strings.Contains(stdout.String(), "sceptre explain <db-path>") {
		t.Fatalf("run() stdout = %q, want explain usage", stdout.String())
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

func TestRunSQLExecutesStatements(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	runOK(t, []string{"sql", path, "create table users (id int64, name bytes, primary key (id))"})
	runOK(t, []string{"sql", path, "insert into users (id, name) values (1, 'Ada')"})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"sql", path, "select id, name from users"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run(sql select) exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "id\tname") {
		t.Fatalf("run(sql select) stdout = %q, want header", stdout.String())
	}
	if !strings.Contains(stdout.String(), "1\tAda") {
		t.Fatalf("run(sql select) stdout = %q, want row", stdout.String())
	}
}

func TestRunInspectMetaAndTree(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	runOK(t, []string{"sql", path, "create table users (id int64, name bytes, primary key (id))"})
	runOK(t, []string{"sql", path, "insert into users (id, name) values (1, 'Ada')"})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"inspect", "meta", path}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run(inspect meta) exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "page_size=") || !strings.Contains(stdout.String(), "root_page=") {
		t.Fatalf("run(inspect meta) stdout = %q, want meta fields", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"inspect", "tree", path}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run(inspect tree) exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "entries=") {
		t.Fatalf("run(inspect tree) stdout = %q, want entries", stdout.String())
	}
}

func TestRunExplainPrintsPlan(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	runOK(t, []string{"sql", path, "create table users (id int64, name bytes, age int64, primary key (id))"})
	runOK(t, []string{"sql", path, "create index users_age on users (age)"})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := run([]string{"explain", path, "select * from users where age = 31 and name = 'Ada'"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run(explain) exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "access=secondary_index_lookup") {
		t.Fatalf("run(explain) stdout = %q, want access path", stdout.String())
	}
	if !strings.Contains(stdout.String(), "index=users_age") {
		t.Fatalf("run(explain) stdout = %q, want index name", stdout.String())
	}
	if !strings.Contains(stdout.String(), "lookup=age = 31") {
		t.Fatalf("run(explain) stdout = %q, want lookup", stdout.String())
	}
	if !strings.Contains(stdout.String(), "residual=name = 'Ada'") {
		t.Fatalf("run(explain) stdout = %q, want residual", stdout.String())
	}
}

func runOK(t *testing.T, args []string) {
	t.Helper()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if code := run(args, &stdout, &stderr); code != 0 {
		t.Fatalf("run(%v) exit code = %d, stderr = %q", args, code, stderr.String())
	}
}
