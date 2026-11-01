package sql

import (
	"errors"
	"testing"

	"github.com/vennictus/sceptre/internal/table"
)

func execErr(t *testing.T, db *table.DB, q string) error {
	t.Helper()
	stmt, err := Parse(q)
	if err != nil {
		return err
	}
	_, err = ExecuteStatement(db, stmt)
	return err
}

func TestSchemaChangesThroughSQL(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (id int64, title text not null, views int64, primary key (id))")
	mustExec(t, db, "create table if not exists posts (id int64, primary key (id))") // no-op
	mustExec(t, db, "create index posts_views on posts (views)")
	mustExec(t, db, "create index if not exists posts_views on posts (title)") // no-op
	mustExec(t, db, "insert into posts (id, title, views) values (1, 'a', 5)")

	mustExec(t, db, "alter table posts add column summary text")
	mustExec(t, db, "alter table posts add slug text unique")
	wantIDs(t, db, "select id from posts where summary is null and slug is null", 1)
	mustExec(t, db, "update posts set slug = 'a' where id = 1")
	mustExec(t, db, "insert into posts (id, title, slug) values (2, 'b', 'b')")
	if err := execErr(t, db, "update posts set slug = 'a' where id = 2"); !errors.Is(err, table.ErrUniqueViolation) {
		t.Fatalf("added UNIQUE column: %v", err)
	}
	if err := execErr(t, db, "alter table posts add column body text not null"); !errors.Is(err, ErrExec) {
		t.Fatalf("ADD COLUMN NOT NULL: %v", err)
	}

	mustExec(t, db, "alter table posts rename column views to hits")
	wantIDs(t, db, "select id from posts where hits = 5", 1)
	mustExec(t, db, "alter table posts rename to articles")
	wantIDs(t, db, "select id from articles", 1, 2)
	if err := execErr(t, db, "select id from posts"); !errors.Is(err, table.ErrTableNotFound) {
		t.Fatalf("old table name: %v", err)
	}

	if err := execErr(t, db, "alter table articles drop column hits"); !errors.Is(err, table.ErrInvalidTableDef) {
		t.Fatalf("DROP COLUMN of an indexed column: %v", err)
	}
	mustExec(t, db, "drop index posts_views")
	mustExec(t, db, "drop index if exists posts_views")
	mustExec(t, db, "alter table articles drop hits")
	if r := mustExec(t, db, "select * from articles where id = 1"); len(r.Columns) != 4 {
		t.Fatalf("columns after DROP COLUMN = %v", r.Columns)
	}
	if err := execErr(t, db, "drop index posts_views"); !errors.Is(err, table.ErrIndexNotFound) {
		t.Fatalf("DROP INDEX of a dropped index: %v", err)
	}

	mustExec(t, db, "drop table articles")
	mustExec(t, db, "drop table if exists articles")
	if err := execErr(t, db, "drop table articles"); !errors.Is(err, table.ErrTableNotFound) {
		t.Fatalf("DROP TABLE of a dropped table: %v", err)
	}
	if rep, err := db.Check(); err != nil || !rep.OK() {
		t.Fatalf("check: %+v %v", rep, err)
	}
}

func TestDropIndexNeedsATableWhenAmbiguous(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	for _, name := range []string{"a", "b"} {
		mustExec(t, db, "create table "+name+" (id int64, v int64, primary key (id))")
		mustExec(t, db, "create index v_idx on "+name+" (v)")
	}
	if err := execErr(t, db, "drop index v_idx"); !errors.Is(err, ErrExec) {
		t.Fatalf("ambiguous DROP INDEX: %v", err)
	}
	mustExec(t, db, "drop index v_idx on a")
	mustExec(t, db, "drop index v_idx")
}

func TestParseSchemaChangeErrors(t *testing.T) {
	t.Parallel()
	for _, q := range []string{
		"drop view v",
		"drop table",
		"alter table t",
		"alter table t rename column a b",
		"alter table t add column x",
		"create table if exists t (id int64, primary key (id))",
	} {
		if _, err := Parse(q); err == nil {
			t.Fatalf("Parse(%q) accepted", q)
		}
	}
}
