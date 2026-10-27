package sceptre

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestReadTxSeesOneCommittedState(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	execOK(t, db, "insert into posts (id, body, state) values (1, 'first', 0)")

	r, err := db.BeginRead(background)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	execOK(t, db, "insert into posts (id, body, state) values (2, 'later', 0)")

	res, err := r.Execute(background, "select id from posts")
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("read transaction sees a later commit: %v %v", res.Rows, err)
	}
	defs, err := r.Tables(background)
	if err != nil || len(defs) != 1 {
		t.Fatalf("Tables = %v %v", defs, err)
	}
	if _, err := r.Explain(background, "select * from posts where id = 1"); err != nil {
		t.Fatal(err)
	}
}

func TestReadTxRejectsWritesAndStaysUsable(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	r, err := db.BeginRead(background)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for _, q := range []string{
		"insert into posts (id, body, state) values (1, 'x', 0)",
		"update posts set state = 1",
		"delete from posts",
		"create table other (id int64, primary key (id))",
	} {
		_, err := r.Execute(background, q)
		assertCode(t, err, CodeReadOnly)
	}
	if _, err := r.Execute(background, "select * from posts"); err != nil {
		t.Fatalf("read transaction unusable after a rejected write: %v", err)
	}
	if r2, err := db.Execute(background, "select * from posts"); err != nil || len(r2.Rows) != 0 {
		t.Fatalf("a rejected write changed the database: %v %v", r2.Rows, err)
	}
}

func TestReadTxCloseIsFinal(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	r, err := db.BeginRead(background)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, err = r.Execute(background, "select * from posts")
	assertCode(t, err, CodeTransactionClosed)
	if _, err := db.BeginRead(nil); err == nil { // a nil context is rejected, not dereferenced
		t.Fatal("BeginRead accepted a nil context")
	}
}

// Reads racing with Close either finish normally or fail with closed; none
// sees the file disappear underneath it.
func TestCloseWaitsForInFlightReads(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, createPosts)
	for i := 0; i < 200; i++ {
		execOK(t, db, "insert into posts (id, body, state) values (?, 'row', 0)", Int64(int64(i)))
	}
	held, err := db.BeginRead(background)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_, err := db.Execute(background, "select * from posts")
				if err == nil {
					continue
				}
				var e *Error
				if !errors.As(err, &e) || e.Code != CodeClosed {
					errs <- err
				}
				return
			}
		}()
	}
	time.Sleep(time.Millisecond)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("read during Close failed with something other than closed: %v", err)
	}
	_, err = held.Execute(background, "select * from posts")
	assertCode(t, err, CodeClosed)
	_, err = db.BeginRead(background)
	assertCode(t, err, CodeClosed)
}
