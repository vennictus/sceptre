package main

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vennictus/sceptre/internal/sql"
	"github.com/vennictus/sceptre/internal/table"
)

// queryAfterReopen opens the database fresh, so it sees only what committed.
func queryAfterReopen(t *testing.T, path, query string) sql.Result {
	t.Helper()
	db, err := table.Open(path, table.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r, err := sql.Execute(db, query)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestSQLFailedUpsertLeavesNoRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	runOK(t, []string{"sql", path, "create table t (id int64 primary key, email text unique)"})
	runOK(t, []string{"sql", path, "insert into t values (1, 'taken')"})
	var out, stderr bytes.Buffer
	if run([]string{"sql", path, "insert into t values (2, 'free'), (3, 'taken') on conflict (id) do nothing"}, &out, &stderr) == 0 {
		t.Fatal("upsert with a unique violation succeeded")
	}
	if r := queryAfterReopen(t, path, "select id from t"); len(r.Rows) != 1 {
		t.Fatalf("failed upsert left rows behind: %v", r.Rows)
	}
}

func TestSQLFailedAddUniqueColumnLeavesNoColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	runOK(t, []string{"sql", path, "create table t (id int64 primary key, v int64)"})
	runOK(t, []string{"sql", path, "insert into t values (1, 5), (2, 6)"})
	var out, stderr bytes.Buffer
	if run([]string{"sql", path, "alter table t add w int64 not null default 0 unique"}, &out, &stderr) == 0 {
		t.Fatal("adding a unique column over duplicate defaults succeeded")
	}
	r := queryAfterReopen(t, path, "select * from t")
	if len(r.Columns) != 2 {
		t.Fatalf("failed ADD COLUMN UNIQUE left the column: %v", r.Columns)
	}
}

func TestShellFailedStatementLeavesNoRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	input := strings.NewReader(strings.Join([]string{
		"create table t (id int64 primary key, email text unique);",
		"insert into t values (1, 'taken');",
		"insert into t values (2, 'free'), (3, 'taken') on conflict (id) do nothing;",
		"insert into t values (4, 'ok');",
	}, "\n"))
	var stdout, stderr bytes.Buffer
	if code := runShell([]string{path}, input, &stdout, &stderr); code != 0 {
		t.Fatalf("shell exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "unique") {
		t.Fatalf("shell did not report the violation: %q", stderr.String())
	}
	r := queryAfterReopen(t, path, "select id from t")
	if len(r.Rows) != 2 || r.Rows[0][0].I64 != 1 || r.Rows[1][0].I64 != 4 {
		t.Fatalf("shell rows after a failed statement = %v, want ids 1 and 4", r.Rows)
	}
}
