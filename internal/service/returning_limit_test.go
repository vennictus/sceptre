package service

import (
	"context"
	"strings"
	"testing"
)

// A write whose RETURNING result is too large to send must not be kept: the
// client sees a limit error and would retry, so a committed write would be
// applied twice.
func TestOversizedReturningRollsBackAutocommit(t *testing.T) {
	f := newFixture(t, Options{MaxResponseBytes: 1024})
	f.mustSQL("CREATE TABLE t (id int64 PRIMARY KEY AUTOINCREMENT, body text)")
	big := strings.Repeat("x", 1500)
	r := f.sql("/api/execute", "INSERT INTO t (body) VALUES ('"+big+"') RETURNING *", nil)
	if r.Status != 413 || r.Body.Error == nil || r.Body.Error.Code != "limit" || !strings.Contains(r.Body.Error.Message, "rolled back") {
		t.Fatalf("oversized INSERT RETURNING: %d %+v", r.Status, r.Body.Error)
	}
	if n := f.count("t"); n != 0 {
		t.Fatalf("oversized INSERT RETURNING kept %d row(s)", n)
	}

	// Without RETURNING the same insert fits and commits.
	if r := f.sql("/api/execute", "INSERT INTO t (body) VALUES ('"+big+"')", nil); r.Status != 200 {
		t.Fatalf("insert without RETURNING: %d %+v", r.Status, r.Body.Error)
	}
	r = f.sql("/api/execute", "DELETE FROM t RETURNING *", nil)
	if r.Status != 413 || r.Body.Error.Code != "limit" {
		t.Fatalf("oversized DELETE RETURNING: %d %+v", r.Status, r.Body.Error)
	}
	got, err := f.db.Execute(context.Background(), "SELECT id FROM t")
	if err != nil || len(got.Rows) != 1 {
		t.Fatalf("oversized DELETE RETURNING removed the row: %v %v", got.Rows, err)
	}

	// A RETURNING result that fits is kept and sent.
	if r := f.sql("/api/execute", "INSERT INTO t (body) VALUES ('small') RETURNING id", nil); r.Status != 200 || len(r.Body.Result.Rows) != 1 {
		t.Fatalf("small INSERT RETURNING: %d %+v", r.Status, r.Body.Error)
	}
}

func TestOversizedReturningRollsBackOnlyTheStatement(t *testing.T) {
	f := newFixture(t, Options{MaxResponseBytes: 1024})
	f.mustSQL("CREATE TABLE t (id int64 PRIMARY KEY, body text)")
	id := f.begin()
	if r := f.sql("/api/tx/execute", "INSERT INTO t VALUES (1, 'kept')", withTx(id)); r.Status != 200 {
		t.Fatalf("first insert: %d %+v", r.Status, r.Body.Error)
	}
	r := f.sql("/api/tx/execute", "INSERT INTO t VALUES (2, '"+strings.Repeat("x", 1500)+"') RETURNING *", withTx(id))
	if r.Status != 413 || r.Body.Error.Code != "limit" || r.Body.Transaction == nil || r.Body.Transaction.ID != id {
		t.Fatalf("oversized RETURNING in a transaction: %d %+v %+v", r.Status, r.Body.Error, r.Body.Transaction)
	}
	if r := f.call("/api/tx/commit", "{}", withTx(id)); r.Status != 200 {
		t.Fatalf("commit: %d %+v", r.Status, r.Body.Error)
	}
	r = f.sql("/api/execute", "SELECT id FROM t", nil)
	if len(r.Body.Result.Rows) != 1 || r.Body.Result.Rows[0][0].Value != "1" {
		t.Fatalf("rows after commit = %+v, want only id 1", r.Body.Result.Rows)
	}
}
