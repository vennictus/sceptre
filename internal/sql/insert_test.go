package sql

import (
	"errors"
	"testing"

	"github.com/vennictus/sceptre/internal/table"
)

func TestMultiRowInsert(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (id int64 primary key autoincrement, title text not null, views int64 default 0)")
	r := mustExec(t, db, "insert into posts (title) values ('a'), ('b'), (?)", table.TextValue("c"))
	if r.RowsAffected != 3 {
		t.Fatalf("RowsAffected = %d", r.RowsAffected)
	}
	wantIDs(t, db, "select id from posts where views = 0", 1, 2, 3)
	mustExec(t, db, "insert into posts values (10, 'all columns', 4), (11, 'x', 5)")
	wantIDs(t, db, "select id from posts where views >= 4", 10, 11)
	// One bad row fails the whole statement.
	if err := execErr(t, db, "insert into posts (id, title) values (20, 'ok'), (1, 'duplicate key')"); !errors.Is(err, table.ErrRowExists) {
		t.Fatalf("duplicate in a multi-row insert: %v", err)
	}
	wantIDs(t, db, "select id from posts where id = 20")
	for _, q := range []string{
		"insert into posts (title) values ('a'), ('b', 1)",
		"insert into posts values (1, 'too few')",
	} {
		if err := execErr(t, db, q); err == nil {
			t.Fatalf("%s accepted", q)
		}
	}
}

func TestInsertSelect(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table drafts (id int64 primary key, title text, score int64)")
	mustExec(t, db, "create table posts (id int64 primary key, title text, score float64)")
	mustExec(t, db, "insert into drafts values (1, 'a', 3), (2, 'b', NULL), (3, 'c', 9)")
	r := mustExec(t, db, "insert into posts (id, title, score) select id, title, score from drafts where id >= 2")
	if r.RowsAffected != 2 {
		t.Fatalf("RowsAffected = %d", r.RowsAffected)
	}
	wantIDs(t, db, "select id from posts where score is null", 2)
	wantIDs(t, db, "select id from posts where score = 9.0", 3) // int64 widened to float64
	if r := mustExec(t, db, "insert into posts select * from drafts where id = 99"); r.RowsAffected != 0 {
		t.Fatalf("empty INSERT ... SELECT affected %d", r.RowsAffected)
	}
	for _, q := range []string{
		"insert into posts (id, title) select id from drafts",
		"insert into drafts (id, title, score) select id, title, score from posts", // float64 into int64
	} {
		if err := execErr(t, db, q); !errors.Is(err, ErrExec) {
			t.Fatalf("%s: %v", q, err)
		}
	}
}
