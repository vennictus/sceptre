package sceptre

import (
	"errors"
	"fmt"
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

// A writer moves balance between accounts in multi-statement transactions
// while readers scan every account. Each read must see a whole committed
// state: the total never changes, and no reader waits for the writer.
func TestConcurrentReadsSeeOnlyWholeCommits(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, "create table accounts (id int64, balance int64, primary key (id))")
	const accounts, start = 10, 100
	for i := 0; i < accounts; i++ {
		execOK(t, db, "insert into accounts (id, balance) values (?, ?)", Int64(int64(i)), Int64(start))
	}
	total := func(rows [][]Value) int64 {
		var sum int64
		for _, r := range rows {
			sum += r[0].I64
		}
		return sum
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	reads := make([]int, 4)
	for g := range reads {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := db.Execute(background, "select balance from accounts")
				if err != nil {
					errs <- err
					return
				}
				if len(res.Rows) != accounts || total(res.Rows) != accounts*start {
					errs <- fmt.Errorf("read saw %d rows totalling %d", len(res.Rows), total(res.Rows))
					return
				}
				reads[g]++
			}
		}(g)
	}

	for i := 0; i < 100; i++ {
		tx, err := db.Begin(background)
		if err != nil {
			t.Fatal(err)
		}
		from, to := int64(i%accounts), int64((i+3)%accounts)
		// Between these statements the database is mid-transfer; readers must not see it.
		txOK(t, tx, "update accounts set balance = ? where id = ?", Int64(start-7), Int64(from))
		txOK(t, tx, "update accounts set balance = ? where id = ?", Int64(start+7), Int64(to))
		if i%2 == 0 {
			if err := tx.Commit(background); err != nil {
				t.Fatal(err)
			}
			// Restore so every committed state keeps the same total.
			tx, err = db.Begin(background)
			if err != nil {
				t.Fatal(err)
			}
			txOK(t, tx, "update accounts set balance = ? where id = ?", Int64(start), Int64(from))
			txOK(t, tx, "update accounts set balance = ? where id = ?", Int64(start), Int64(to))
			if err := tx.Commit(background); err != nil {
				t.Fatal(err)
			}
		} else if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for g, n := range reads {
		if n == 0 {
			t.Fatalf("reader %d never completed a read while the writer ran", g)
		}
	}
}
