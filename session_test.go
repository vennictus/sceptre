package sceptre

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func sessionOK(t *testing.T, s *Session, q string, args ...Value) Result {
	t.Helper()
	r, err := s.Execute(background, q, args...)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return r
}

func postIDs(t *testing.T, run func(string, ...Value) (Result, error)) []int64 {
	t.Helper()
	r, err := run("select id from posts")
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, len(r.Rows))
	for i, row := range r.Rows {
		ids[i] = row[0].I64
	}
	return ids
}

func sameIDs(t *testing.T, got []int64, want ...int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("ids = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ids = %v, want %v", got, want)
		}
	}
}

func insertPost(id int) string {
	return fmt.Sprintf("insert into posts (id, body, state) values (%d, 'b', 0)", id)
}

func TestSessionBeginCommitAndRollback(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	s := db.Session()
	defer s.Close()
	dbRead := func(q string, a ...Value) (Result, error) { return db.Execute(background, q, a...) }
	inSession := func(q string, a ...Value) (Result, error) { return s.Execute(background, q, a...) }

	sessionOK(t, s, "BEGIN")
	if !s.InTransaction() {
		t.Fatal("not in a transaction after BEGIN")
	}
	sessionOK(t, s, insertPost(1))
	sameIDs(t, postIDs(t, inSession), 1)
	sameIDs(t, postIDs(t, dbRead)) // uncommitted work stays private
	sessionOK(t, s, "COMMIT")
	if s.InTransaction() {
		t.Fatal("still in a transaction after COMMIT")
	}
	sameIDs(t, postIDs(t, dbRead), 1)

	sessionOK(t, s, "begin transaction")
	sessionOK(t, s, insertPost(2))
	sessionOK(t, s, "rollback")
	sameIDs(t, postIDs(t, dbRead), 1)

	// Outside BEGIN, statements autocommit.
	sessionOK(t, s, insertPost(3))
	sameIDs(t, postIDs(t, dbRead), 1, 3)
}

func TestSessionSavepoints(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	s := db.Session()
	defer s.Close()
	inSession := func(q string, a ...Value) (Result, error) { return s.Execute(background, q, a...) }

	sessionOK(t, s, "BEGIN")
	sessionOK(t, s, insertPost(1))
	sessionOK(t, s, "SAVEPOINT a")
	sessionOK(t, s, insertPost(2))
	sessionOK(t, s, "SAVEPOINT b")
	sessionOK(t, s, insertPost(3))
	sessionOK(t, s, "ROLLBACK TO a")
	sameIDs(t, postIDs(t, inSession), 1)
	if _, err := s.Execute(background, "RELEASE b"); codeOf(err) != CodeNotFound {
		t.Fatalf("RELEASE of a savepoint closed by ROLLBACK TO: %v", err)
	}
	// ROLLBACK TO keeps the savepoint open.
	sessionOK(t, s, insertPost(5))
	sessionOK(t, s, "rollback to savepoint a")
	sameIDs(t, postIDs(t, inSession), 1)
	sessionOK(t, s, insertPost(4))
	sessionOK(t, s, "RELEASE SAVEPOINT a")
	if _, err := s.Execute(background, "ROLLBACK TO a"); codeOf(err) != CodeNotFound {
		t.Fatalf("ROLLBACK TO a released savepoint: %v", err)
	}
	sessionOK(t, s, "COMMIT")
	sameIDs(t, postIDs(t, func(q string, a ...Value) (Result, error) { return db.Execute(background, q, a...) }), 1, 4)
}

func TestSessionSavepointUndoesSchemaChanges(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	s := db.Session()
	defer s.Close()
	sessionOK(t, s, "BEGIN")
	sessionOK(t, s, "SAVEPOINT s")
	sessionOK(t, s, createPosts)
	sessionOK(t, s, insertPost(1))
	sessionOK(t, s, "ROLLBACK TO s")
	sessionOK(t, s, "COMMIT")
	if defs, err := db.Tables(background); err != nil || len(defs) != 0 {
		t.Fatalf("tables after rolled-back CREATE = %v, %v", defs, err)
	}
}

func TestSessionTransactionStateErrors(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	s := db.Session()
	defer s.Close()
	for _, q := range []string{"COMMIT", "ROLLBACK", "SAVEPOINT a", "RELEASE a", "ROLLBACK TO a"} {
		if _, err := s.Execute(background, q); codeOf(err) != CodeTransactionState {
			t.Fatalf("%s outside a transaction: %v", q, err)
		}
	}
	for _, q := range []string{"BEGIN", "COMMIT", "SAVEPOINT a"} {
		if _, err := db.Execute(background, q); codeOf(err) != CodeTransactionState {
			t.Fatalf("DB.Execute(%s): %v", q, err)
		}
	}
	sessionOK(t, s, "BEGIN")
	if _, err := s.Execute(background, "BEGIN"); codeOf(err) != CodeTransactionState {
		t.Fatalf("nested BEGIN: %v", err)
	}
	if !s.InTransaction() {
		t.Fatal("nested BEGIN ended the transaction")
	}
	if _, err := s.Execute(background, "RELEASE nope"); codeOf(err) != CodeNotFound {
		t.Fatalf("RELEASE unknown savepoint: %v", err)
	}
}

func TestTxSavepointsThroughGoAPI(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	tx, err := db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	run := func(q string, a ...Value) (Result, error) { return tx.Execute(background, q, a...) }
	for _, q := range []string{insertPost(1), "SAVEPOINT x", insertPost(2), "ROLLBACK TO SAVEPOINT x"} {
		if _, err := run(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	sameIDs(t, postIDs(t, run), 1)
	for _, q := range []string{"BEGIN", "COMMIT", "ROLLBACK"} {
		if _, err := run(q); codeOf(err) != CodeTransactionState {
			t.Fatalf("Tx.Execute(%s): %v", q, err)
		}
	}
	if err := tx.Commit(background); err != nil {
		t.Fatal(err)
	}
}

func TestSessionCloseRollsBackAndFreesExecution(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	s := db.Session()
	sessionOK(t, s, "BEGIN")
	sessionOK(t, s, insertPost(1))
	s.Close()
	ctx, cancel := context.WithTimeout(background, 2*time.Second)
	defer cancel()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin after Session.Close: %v", err)
	}
	tx.Rollback()
	sameIDs(t, postIDs(t, func(q string, a ...Value) (Result, error) { return db.Execute(background, q, a...) }))
}
