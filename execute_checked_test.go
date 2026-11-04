package sceptre

import (
	"errors"
	"testing"
)

var errReject = errors.New("rejected by caller")

func reject(Result) error { return errReject }

func rowCount(t *testing.T, db *DB, query string) int {
	t.Helper()
	res, err := db.Execute(background, query)
	if err != nil {
		t.Fatal(err)
	}
	return len(res.Rows)
}

func TestExecuteCheckedRejectUndoesTheStatement(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, "create table t (id int64 primary key autoincrement, v int64)")

	// Autocommit: a rejected write is never committed.
	if _, err := db.ExecuteChecked(background, reject, "insert into t (v) values (1) returning id"); !errors.Is(err, errReject) {
		t.Fatalf("DB.ExecuteChecked error = %v", err)
	}
	if n := rowCount(t, db, "select id from t"); n != 0 {
		t.Fatalf("rejected autocommit insert kept %d row(s)", n)
	}

	// Accept sees the result the caller gets.
	var seen Result
	res, err := db.ExecuteChecked(background, func(r Result) error { seen = r; return nil }, "insert into t (v) values (2) returning id")
	if err != nil || len(res.Rows) != 1 || len(seen.Rows) != 1 || seen.Rows[0][0].I64 != res.Rows[0][0].I64 {
		t.Fatalf("accepted insert: %+v seen %+v err %v", res, seen, err)
	}

	// Transaction: only the rejected statement is undone.
	tx, err := db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Execute(background, "insert into t (v) values (3)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecuteChecked(background, reject, "delete from t returning *"); !errors.Is(err, errReject) {
		t.Fatalf("Tx.ExecuteChecked error = %v", err)
	}
	if err := tx.Commit(background); err != nil {
		t.Fatal(err)
	}
	if n := rowCount(t, db, "select id from t"); n != 2 {
		t.Fatalf("rows after a rejected delete = %d, want 2", n)
	}

	// Session, in and out of a transaction.
	s := db.Session()
	defer s.Close()
	if _, err := s.ExecuteChecked(background, reject, "insert into t (v) values (4)"); !errors.Is(err, errReject) {
		t.Fatalf("Session.ExecuteChecked error = %v", err)
	}
	if _, err := s.Execute(background, "begin"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ExecuteChecked(background, reject, "insert into t (v) values (5)"); !errors.Is(err, errReject) {
		t.Fatalf("Session.ExecuteChecked in a transaction error = %v", err)
	}
	if _, err := s.Execute(background, "commit"); err != nil {
		t.Fatal(err)
	}
	if n := rowCount(t, db, "select id from t"); n != 2 {
		t.Fatalf("rows after rejected session writes = %d, want 2", n)
	}
}
