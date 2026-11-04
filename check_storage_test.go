package sceptre

import (
	"testing"
	"time"
)

func reopen(t *testing.T, db *DB, path string) *DB {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func wantCode(t *testing.T, db *DB, code Code, q string, args ...Value) {
	t.Helper()
	if _, err := db.Execute(background, q, args...); codeOf(err) != code {
		t.Fatalf("%s: %v, want %s", q, err, code)
	}
}

// A CHECK keeps the exact values and types it was written with, across
// close and reopen.
func TestCheckValuesSurviveReopen(t *testing.T) {
	t.Parallel()
	db, path := openTestDB(t, Options{})
	execOK(t, db, "create table b (id int64 primary key, body bytes check (body != ?))", Bytes([]byte{0xff}))
	execOK(t, db, "create table c (id int64 primary key, n int64, check (n != CAST(? AS int64)))", Timestamp(time.UnixMicro(7)))
	execOK(t, db, "create table f (id int64 primary key, x float64 check (x != ?))", Float64(2))
	db = reopen(t, db, path)

	wantCode(t, db, CodeConstraint, "insert into b values (1, ?)", Bytes([]byte{0xff}))
	execOK(t, db, "insert into b values (1, ?)", Bytes([]byte("�")))
	wantCode(t, db, CodeConstraint, "insert into c values (1, 7)")
	execOK(t, db, "insert into c values (2, 8)")
	wantCode(t, db, CodeConstraint, "insert into f values (1, 2.0)")
	execOK(t, db, "insert into f values (2, 2.5)")
}

// Renaming a column to a word the parser treats as a keyword keeps the
// CHECK on that column working, before and after reopen.
func TestRenameColumnToKeywordKeepsCheck(t *testing.T) {
	t.Parallel()
	db, path := openTestDB(t, Options{})
	execOK(t, db, "create table t (id int64 primary key, score int64 check (score > 0))")
	execOK(t, db, "alter table t rename score to true")
	execOK(t, db, "insert into t values (1, 2)")
	wantCode(t, db, CodeConstraint, "insert into t values (2, -2)")
	db = reopen(t, db, path)
	execOK(t, db, "insert into t values (3, 4)")
	wantCode(t, db, CodeConstraint, `insert into t ("true", id) values (-1, 4)`)
	if r := execOK(t, db, `select id from t where "true" > 1`); len(r.Rows) != 2 {
		t.Fatalf("rows = %+v", r.Rows)
	}
}
