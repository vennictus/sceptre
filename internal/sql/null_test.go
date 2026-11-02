package sql

import (
	"errors"
	"testing"

	"github.com/vennictus/sceptre/internal/table"
)

func ids(t *testing.T, r Result) []int64 {
	t.Helper()
	out := make([]int64, len(r.Rows))
	for i, row := range r.Rows {
		out[i] = row[0].I64
	}
	return out
}

func wantIDs(t *testing.T, db *table.DB, q string, want ...int64) {
	t.Helper()
	got := ids(t, mustExec(t, db, q))
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", q, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", q, got, want)
		}
	}
}

func TestNullSemantics(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (id int64, title text not null, summary text, views int64 null, primary key (id))")
	mustExec(t, db, "create index posts_views on posts (views)")
	mustExec(t, db, "insert into posts (id, title, summary, views) values (1, 'a', 'one', 10)")
	mustExec(t, db, "insert into posts (id, title, summary, views) values (2, 'b', NULL, 20)")
	mustExec(t, db, "insert into posts (id, title) values (3, 'c')") // omitted columns are NULL
	mustExec(t, db, "insert into posts (id, title, summary, views) values (?, 'd', ?, ?)", table.Int64Value(4), table.NullValue(), table.Int64Value(10))

	def, _, _ := db.Table("posts")
	for _, c := range def.Columns {
		want := c.Name == "summary" || c.Name == "views"
		if c.Nullable != want {
			t.Fatalf("column %s nullable = %v, want %v", c.Name, c.Nullable, want)
		}
	}

	wantIDs(t, db, "select id from posts where summary is null", 2, 3, 4)
	wantIDs(t, db, "select id from posts where summary is not null", 1)
	// Comparisons with NULL are unknown, so they never match.
	wantIDs(t, db, "select id from posts where summary = NULL")
	wantIDs(t, db, "select id from posts where summary != 'one'")
	wantIDs(t, db, "select id from posts where views = 10", 1, 4)
	wantIDs(t, db, "select id from posts where views >= 0", 1, 2, 4)
	// Three-valued AND / OR.
	wantIDs(t, db, "select id from posts where views = 10 or summary = 'x'", 1, 4)
	wantIDs(t, db, "select id from posts where views is null or summary = 'one'", 1, 3)
	wantIDs(t, db, "select id from posts where views = 20 and summary is null", 2)

	r := mustExec(t, db, "select summary, views from posts where id = 3")
	if !r.Rows[0][0].IsNull() || !r.Rows[0][1].IsNull() {
		t.Fatalf("NULL projection = %+v", r.Rows)
	}
	mustExec(t, db, "update posts set summary = 'filled', views = NULL where id = 2")
	wantIDs(t, db, "select id from posts where views is null", 2, 3)
	wantIDs(t, db, "select id from posts where summary = 'filled'", 2)
	mustExec(t, db, "delete from posts where summary is null")
	wantIDs(t, db, "select id from posts", 1, 2)

	if plan, err := ExplainStatement(db, mustParse(t, "select * from posts where views = NULL")); err != nil || plan.Access != AccessTableScan {
		t.Fatalf("plan for = NULL: %+v %v", plan, err)
	}
	if rep, err := db.Check(); err != nil || !rep.OK() {
		t.Fatalf("check: %+v %v", rep, err)
	}
}

func mustParse(t *testing.T, q string) Statement {
	t.Helper()
	stmt, err := Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	return stmt
}

func TestNullRejections(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table posts (id int64, title text not null, primary key (id))")
	for _, q := range []string{
		"insert into posts (id) values (1)",
		"insert into posts (id, title) values (1, NULL)",
		"insert into posts (id, title) values (NULL, 't')",
		"select id from posts where (title = 'a') is null",
	} {
		stmt, err := Parse(q)
		if err == nil {
			_, err = ExecuteStatement(db, stmt)
		}
		if err == nil {
			t.Fatalf("%s accepted", q)
		}
		if !errors.Is(err, ErrExec) && !errors.Is(err, table.ErrInvalidValue) && !errors.Is(err, table.ErrInvalidRecord) {
			t.Fatalf("%s: unexpected error %v", q, err)
		}
	}
	mustExec(t, db, "insert into posts (id, title) values (1, 't')")
	if _, err := ExecuteStatement(db, mustParse(t, "update posts set title = NULL where id = 1")); !errors.Is(err, table.ErrInvalidValue) {
		t.Fatalf("UPDATE to NULL in a NOT NULL column: %v", err)
	}
}

func TestParseNullForms(t *testing.T) {
	t.Parallel()
	stmt := mustParse(t, "create table t (id int64, a text not null, b text null, c int64, primary key (id))")
	cols := stmt.(*CreateTableStmt).Columns
	if cols[1].NotNull != true || cols[2].NotNull || cols[3].NotNull {
		t.Fatalf("column constraints = %+v", cols)
	}
	where := mustParse(t, "select * from t where a is not null and b IS NULL").(*SelectStmt).Where
	if got := FormatExpr(where); got != "(a IS NOT NULL AND b IS NULL)" {
		t.Fatalf("FormatExpr = %q", got)
	}
	if _, err := Parse("select * from t where a is 1"); err == nil {
		t.Fatal("IS 1 accepted")
	}
}
