package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

// Walking a RETURNING result across the response limit: every write that is
// kept fits in its response, every write after the first rejected one is
// rejected too and leaves no row, and the limit leaves no more room unused
// than the widest duration field could need.
func TestReturningLimitBoundaryIsExact(t *testing.T) {
	f := newFixture(t, Options{MaxResponseBytes: 1024})
	f.mustSQL("CREATE TABLE t (id int64 PRIMARY KEY AUTOINCREMENT, body text)")
	kept, keptLen, rejected := 0, 0, false
	for n := 700; n <= 1100; n++ {
		body, _ := json.Marshal(query{SQL: "INSERT INTO t (body) VALUES ('" + strings.Repeat("x", n) + "') RETURNING body"})
		req := httptest.NewRequest(http.MethodPost, "http://"+testHost+"/api/execute", strings.NewReader(string(body)))
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		f.h.ServeHTTP(rec, req)
		switch {
		case rec.Code == 200 && !rejected:
			if rec.Body.Len() > 1024 {
				t.Fatalf("n=%d: a kept write sent %d bytes, over the 1024 limit", n, rec.Body.Len())
			}
			kept, keptLen = kept+1, rec.Body.Len()
		case rec.Code == 413:
			rejected = true
		default:
			t.Fatalf("n=%d: status %d after rejected=%v: %s", n, rec.Code, rejected, rec.Body.String())
		}
	}
	if kept == 0 || !rejected {
		t.Fatalf("kept %d writes, rejected=%v: the walk never crossed the limit", kept, rejected)
	}
	// The size check assumes the widest duration (24 characters); the real
	// one is shorter, so a little room goes unused, never more than that.
	if unused := 1024 - keptLen; unused > 24 {
		t.Fatalf("largest kept response is %d bytes: %d bytes of the limit unused", keptLen, unused)
	}
	got, err := f.db.Execute(context.Background(), "SELECT id FROM t")
	if err != nil || len(got.Rows) != kept {
		t.Fatalf("rows = %d (%v), want %d: one per kept write, none for rejected ones", len(got.Rows), err, kept)
	}
}
