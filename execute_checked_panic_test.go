package sceptre

import "testing"

// A panic in accept undoes the statement, as an error would, and leaves the
// transaction usable.
func TestExecuteCheckedPanicUndoesTheStatement(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, "create table t (id int64 primary key)")
	tx, err := db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("accept's panic was swallowed")
			}
		}()
		tx.ExecuteChecked(background, func(Result) error { panic("boom") }, "insert into t values (1)")
	}()
	r, err := tx.Execute(background, "select id from t")
	if err != nil || len(r.Rows) != 0 {
		t.Fatalf("rows after a panicking accept = %+v, %v; want none", r.Rows, err)
	}
	if _, err := tx.Execute(background, "insert into t values (2)"); err != nil {
		t.Fatalf("transaction unusable after a panicking accept: %v", err)
	}
	if err := tx.Commit(background); err != nil {
		t.Fatal(err)
	}

	// Autocommit: the automatic transaction is rolled back and released.
	func() {
		defer func() { recover() }()
		db.ExecuteChecked(background, func(Result) error { panic("boom") }, "insert into t values (3)")
	}()
	if n := rowCount(t, db, "select id from t"); n != 1 {
		t.Fatalf("rows after a panicking autocommit accept = %d, want 1", n)
	}
}
