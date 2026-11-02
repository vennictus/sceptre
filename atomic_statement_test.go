package sceptre

import "testing"

// A statement that fails partway leaves none of its writes in the
// transaction, even when it writes row by row.
func TestFailedStatementLeavesNoPartialWrites(t *testing.T) {
	t.Parallel()
	db, _ := openTestDB(t, Options{})
	execOK(t, db, "create table t (id int64 primary key, email text unique)")
	execOK(t, db, "insert into t values (1, 'a')")
	tx, err := db.Begin(background)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Row 2 inserts, then row 3 hits the email of row 1 with a different
	// conflict target and fails the statement.
	if _, err := tx.Execute(background, "insert into t values (2, 'b'), (3, 'a') on conflict (id) do nothing"); codeOf(err) != CodeConstraint {
		t.Fatalf("upsert error = %v", err)
	}
	res, err := tx.Execute(background, "select id from t")
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("rows after the failed statement = %+v %v", res.Rows, err)
	}
	if _, err := tx.Execute(background, "insert into t values (2, 'b')"); err != nil {
		t.Fatalf("transaction unusable after a failed statement: %v", err)
	}
	if err := tx.Commit(background); err != nil {
		t.Fatal(err)
	}
}
