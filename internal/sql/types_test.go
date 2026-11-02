package sql

import (
	"testing"
	"time"

	"github.com/vennictus/sceptre/internal/table"
)

func TestFloatBoolTimestampColumns(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table events (id int64, score float64, done bool, at timestamp, primary key (id))")
	mustExec(t, db, "create index events_at on events (at)")
	mustExec(t, db, "create index events_score on events (score)")
	mustExec(t, db, "insert into events (id, score, done, at) values (1, 1.5, true, '2026-11-02T10:00:00Z')")
	mustExec(t, db, "insert into events (id, score, done, at) values (2, -2, false, '2026-11-01')") // int literal into float64
	mustExec(t, db, "insert into events (id, score, done, at) values (3, 2.5e1, TRUE, '2026-11-02 12:30:00')")
	at := time.Date(2026, 10, 1, 8, 0, 0, 123456789, time.UTC)
	mustExec(t, db, "insert into events (id, score, done, at) values (?, ?, ?, ?)", table.Int64Value(4), table.Float64Value(-0.25), table.BoolValue(false), table.TimestampValue(at))

	wantIDs(t, db, "select id from events where done = true", 1, 3)
	wantIDs(t, db, "select id from events where done = false", 2, 4)
	wantIDs(t, db, "select id from events where score > 1", 1, 3) // int literal against float64
	wantIDs(t, db, "select id from events where score = -2", 2)   // index lookup with a converted literal
	wantIDs(t, db, "select id from events where score <= -0.25", 2, 4)
	wantIDs(t, db, "select id from events where at >= '2026-11-02'", 1, 3)
	wantIDs(t, db, "select id from events where at = '2026-11-02T10:00:00Z'", 1)

	r := mustExec(t, db, "select score, done, at from events where id = ?", table.Int64Value(4))
	row := r.Rows[0]
	if row[0].F64 != -0.25 || row[1].Bool || !row[2].Time().Equal(at.Truncate(time.Microsecond)) {
		t.Fatalf("typed round trip = %+v", row)
	}
	r = mustExec(t, db, "select id from events where at < ?", table.TimestampValue(time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)))
	if len(r.Rows) != 1 || r.Rows[0][0].I64 != 4 {
		t.Fatalf("timestamp binding comparison = %+v", r.Rows)
	}
	mustExec(t, db, "update events set score = 0, done = true where id = 2")
	wantIDs(t, db, "select id from events where score = 0 and done = true", 2)
	if rep, err := db.Check(); err != nil || !rep.OK() {
		t.Fatalf("check: %+v %v", rep, err)
	}
}

func TestTypedLiteralMismatches(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table t (id int64, f float64, b bool, ts timestamp, s text, primary key (id))")
	for _, q := range []string{
		"insert into t (id, f) values (1.5, 1)", // float into int64
		"insert into t (id, b) values (1, 1)",   // int into bool
		"insert into t (id, ts) values (1, 5)",  // int into timestamp
		"insert into t (id, ts) values (1, 'soon')",
		"insert into t (id, s) values (1, true)",
		"select id from t where b = 'true'",
		"select id from t where s = 1.5",
		"select id from t where f = b",
	} {
		if err := execErr(t, db, q); err == nil {
			t.Fatalf("%s accepted", q)
		}
	}
	for _, q := range []string{"select 1. from t", "select * from t where f = 1e"} {
		if _, err := Parse(q); err == nil {
			t.Fatalf("Parse(%q) accepted", q)
		}
	}
}

func TestParseNumbersAndBooleans(t *testing.T) {
	t.Parallel()
	where := mustParse(t, "select * from t where a = -1.25e-3 and b = false and c = 7").(*SelectStmt).Where
	if got := FormatExpr(where); got != "((a = -0.00125 AND b = FALSE) AND c = 7)" {
		t.Fatalf("FormatExpr = %q", got)
	}
	cols := mustParse(t, "create table t (id integer, a double, b boolean, c timestamp, d real, primary key (id))").(*CreateTableStmt).Columns
	for i, want := range []table.Type{table.TypeInt64, table.TypeFloat64, table.TypeBool, table.TypeTimestamp, table.TypeFloat64} {
		if got, err := sqlType(cols[i].Type); err != nil || got != want {
			t.Fatalf("type %s = %v %v, want %v", cols[i].Type, got, err, want)
		}
	}
}
