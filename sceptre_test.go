package sceptre

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

var background = context.Background()

const createPosts = "create table posts (id int64, body bytes, state int64, primary key (id))"

func openTestDB(t *testing.T, opts Options) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, path
}
func execOK(t *testing.T, db *DB, q string, args ...Value) Result {
	t.Helper()
	r, err := db.Execute(background, q, args...)
	if err != nil {
		t.Fatal(q, err)
	}
	return r
}
func txOK(t *testing.T, tx *Tx, q string, args ...Value) Result {
	t.Helper()
	r, err := tx.Execute(background, q, args...)
	if err != nil {
		t.Fatal(q, err)
	}
	return r
}
func assertCode(t *testing.T, err error, code Code) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Code != code {
		t.Fatalf("got %v, want code %s", err, code)
	}
}

func TestTransactionSchemaRowsIndexesCommitAndRollback(t *testing.T) {
	t.Parallel()
	db, path := openTestDB(t, Options{})
	baseline := db.store.Pager().Meta().Generation
	tx, err := db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	txOK(t, tx, createPosts)
	txOK(t, tx, "insert into posts (id, body, state) values (?, ?, ?)", Int64(1), Bytes([]byte("draft")), Int64(0))
	txOK(t, tx, "create index state_idx on posts (state)")
	txOK(t, tx, "update posts set body = ?, state = ? where id = ?", Bytes([]byte("published")), Int64(1), Int64(1))
	r := txOK(t, tx, "select body from posts where state = ?", Int64(1))
	if len(r.Rows) != 1 || string(r.Rows[0][0].Bytes) != "published" {
		t.Fatal(r)
	}
	defs, err := tx.Tables(background)
	if err != nil || len(defs) != 1 || len(defs[0].Indexes) != 1 {
		t.Fatal(defs, err)
	}
	if db.store.Pager().Meta().Generation != baseline {
		t.Fatal("buffered statements wrote durable data")
	}
	if err := tx.Commit(background); err != nil {
		t.Fatal(err)
	}
	if db.store.Pager().Meta().Generation != baseline+1 {
		t.Fatal("expected exactly one commit")
	}
	tx, err = db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	txOK(t, tx, "delete from posts where state = 1")
	txOK(t, tx, "create table abandoned (id int64, primary key (id))")
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	r = execOK(t, db, "select body from posts where state = 1")
	if len(r.Rows) != 1 || string(r.Rows[0][0].Bytes) != "published" {
		t.Fatal(r)
	}
	defs, err = db.Tables(background)
	if err != nil || len(defs) != 1 {
		t.Fatal(defs, err)
	}
	check, err := db.Check(background)
	if err != nil || !check.OK() {
		t.Fatal(check, err)
	}
}

func TestFailedStatementKeepsTransactionUsable(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{PageSize: 512})
	execOK(t, db, createPosts)
	tx, err := db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	txOK(t, tx, "insert into posts (id, body, state) values (1, 'kept', 0)")
	for _, tc := range []struct {
		q    string
		args []Value
		code Code
	}{
		{"insert into posts (id, body, state) values (1, 'duplicate', 0)", nil, CodeConstraint},
		{"update posts set nonexistent = 1", nil, CodeInvalid},
		{"update posts set body = ?", []Value{Int64(1)}, CodeInvalid},
		{"select * from posts where id = ?", nil, CodeParameter},
		{"select * from posts LIMIT -1", nil, CodeParse},
		{"nonsense", nil, CodeParse},
	} {
		_, err := tx.Execute(background, tc.q, tc.args...)
		assertCode(t, err, tc.code)
	}
	// The oversized row fails before pending changes are published.
	_, err = tx.Execute(background, "update posts set body = ?", Bytes(bytes.Repeat([]byte("x"), 2000)))
	assertCode(t, err, CodeLimit)
	r := txOK(t, tx, "select body from posts where id = 1")
	if string(r.Rows[0][0].Bytes) != "kept" {
		t.Fatal(r)
	}
	txOK(t, tx, "insert into posts (id, body, state) values (2, 'also kept', 0)")
	if err := tx.Commit(background); err != nil {
		t.Fatal(err)
	}
	r = execOK(t, db, "select * from posts")
	if len(r.Rows) != 2 {
		t.Fatal(r)
	}
}

func TestTypedParametersPreserveBytesAndPreventSQLInjection(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{MaxRows: 2})
	execOK(t, db, createPosts)
	body := []byte{'\x00', '\xff', '\'', '?', ';'}
	for _, id := range []int64{-9223372036854775808, 0, 9223372036854775807} {
		execOK(t, db, "insert into posts (id, body, state) values (?, ?, 0)", Int64(id), Bytes(body))
	}
	r := execOK(t, db, "select body from posts where id = ?", Int64(-9223372036854775808))
	if !bytes.Equal(r.Rows[0][0].Bytes, body) {
		t.Fatal(r)
	}
	injection := Bytes([]byte("' OR id = 0; delete from posts"))
	r = execOK(t, db, "select id from posts where body = ?", injection)
	if len(r.Rows) != 0 {
		t.Fatal(r)
	}
	r = execOK(t, db, "select id from posts LIMIT ? OFFSET ?", Int64(100), Int64(1))
	if len(r.Rows) != 2 {
		t.Fatal(r)
	}
	r = execOK(t, db, "select id from posts")
	if len(r.Rows) != 2 {
		t.Fatal("missing implicit result cap", r)
	}
	for _, tc := range []struct {
		q    string
		args []Value
		code Code
	}{
		{"select * from posts where body = '?'", []Value{Int64(1)}, CodeParameter},
		{"select * from posts where id = ?", []Value{{Type: 99}}, CodeParameter},
		{"select * from posts LIMIT ?", []Value{Bytes([]byte("2"))}, CodeParse},
		{"select * from ?", []Value{Bytes([]byte("posts"))}, CodeParse},
	} {
		_, err := db.Execute(background, tc.q, tc.args...)
		assertCode(t, err, tc.code)
	}
	p, err := db.Explain(background, "select * from posts where id = ?", Int64(0))
	if err != nil {
		t.Fatal(p, err)
	}
}

func TestExecutionOwnershipCancellationAndClose(t *testing.T) {
	t.Parallel()
	db, path := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	tx, err := db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	txOK(t, tx, "insert into posts (id, body, state) values (1, 'not committed', 0)")
	ctx, cancel := context.WithTimeout(background, 30*time.Millisecond)
	defer cancel()
	_, err = db.Execute(ctx, "select * from posts")
	assertCode(t, err, CodeCanceled)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	waiting := make(chan error, 1)
	started := make(chan struct{})
	go func() { close(started); _, err := db.Begin(background); waiting <- err }()
	<-started
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waiting:
		assertCode(t, err, CodeClosed)
	case <-time.After(3 * time.Second):
		t.Fatal("waiting request did not wake")
	}
	_, err = tx.Execute(background, "select * from posts")
	assertCode(t, err, CodeTransactionClosed)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if r := execOK(t, db, "select * from posts"); len(r.Rows) != 0 {
		t.Fatal(r)
	}
}

func TestConcurrentReadModifyWriteTransactions(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	execOK(t, db, "insert into posts (id, body, state) values (1, 'counter', 0)")
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := db.Begin(background)
			if err != nil {
				errs <- err
				return
			}
			defer tx.Rollback()
			r, err := tx.Execute(background, "select state from posts where id = 1")
			if err != nil {
				errs <- err
				return
			}
			_, err = tx.Execute(background, "update posts set state = ? where id = 1", Int64(r.Rows[0][0].I64+1))
			if err != nil {
				errs <- err
				return
			}
			errs <- tx.Commit(background)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	r := execOK(t, db, "select state from posts where id = 1")
	if r.Rows[0][0].I64 != 16 {
		t.Fatal(r)
	}
}

func TestEmptyCanceledAndClosedTransactions(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	baseline := db.store.Pager().Meta().Generation
	tx, err := db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(background); err != nil {
		t.Fatal(err)
	}
	if db.store.Pager().Meta().Generation != baseline {
		t.Fatal("empty commit wrote data")
	}
	assertCode(t, tx.Commit(background), CodeTransactionClosed)
	tx, err = db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	txOK(t, tx, createPosts)
	ctx, cancel := context.WithCancel(background)
	cancel()
	assertCode(t, tx.Commit(ctx), CodeCanceled)
	defs, err := db.Tables(background)
	if err != nil || len(defs) != 0 {
		t.Fatal(defs, err)
	}
	_, err = db.Begin(ctx)
	assertCode(t, err, CodeCanceled)
	_, err = db.Begin(nil)
	assertCode(t, err, CodeInvalid)
	_, err = db.Execute(background, string(bytes.Repeat([]byte(" "), 65537)))
	assertCode(t, err, CodeLimit)
	for _, n := range []int{-1, 100001} {
		_, err := Open(filepath.Join(t.TempDir(), fmt.Sprint(n)), Options{MaxRows: n})
		assertCode(t, err, CodeInvalid)
	}
}

func TestDuplicatePrimaryKeyUnderConcurrentRequests(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := db.Execute(background, "insert into posts (id, body, state) values (1, 'contended', 0)")
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	succeeded := 0
	for err := range errs {
		if err == nil {
			succeeded++
		} else {
			assertCode(t, err, CodeConstraint)
		}
	}
	if succeeded != 1 {
		t.Fatal("duplicate key admitted", succeeded)
	}
	if r := execOK(t, db, "select * from posts"); len(r.Rows) != 1 {
		t.Fatal(r)
	}
}
