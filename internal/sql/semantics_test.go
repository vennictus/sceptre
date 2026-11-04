package sql

import (
	"errors"
	"math"
	"strconv"
	"testing"

	"github.com/vennictus/sceptre/internal/table"
)

// int64 -> float64 is exact or fails, wherever it happens: CAST, a literal
// stored in or compared with a float column, and INSERT ... SELECT.
func TestIntToFloatIsExactOrFails(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table i (id int64 primary key)")
	mustExec(t, db, "create table f (id int64 primary key, x float64)")
	const two53 = int64(1) << 53
	for _, n := range []int64{0, -1, two53, -two53, math.MinInt64} {
		mustExec(t, db, "insert into i values (?)", table.Int64Value(n))
	}
	r := mustExec(t, db, "select id from i where CAST(CAST(id AS float64) AS int64) = id")
	if len(r.Rows) != 5 {
		t.Fatalf("exact conversions: %d rows, want 5", len(r.Rows))
	}
	mustExec(t, db, "insert into f select id, id from i")

	for _, n := range []int64{two53 + 1, -two53 - 1, math.MaxInt64} {
		db2 := mustOpenSQLTable(t)
		mustExec(t, db2, "create table i (id int64 primary key)")
		mustExec(t, db2, "create table f (id int64 primary key, x float64)")
		mustExec(t, db2, "insert into i values (?)", table.Int64Value(n))
		lit := strconv.FormatInt(n, 10)
		for _, q := range []string{
			"select id from i where CAST(id AS float64) > 0.5",
			"insert into f values (1, " + lit + ")",
			"insert into f select id, id from i",
		} {
			if _, err := Execute(db2, q); !errors.Is(err, table.ErrInvalidValue) {
				t.Errorf("%d: %s: %v, want an invalid-value error", n, q, err)
			}
		}
		db2.Close()
	}
}

// Comparing two literals gives the same answer whichever side each is on.
func TestLiteralComparisonsAreSymmetric(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table t (id int64 primary key)")
	mustExec(t, db, "insert into t values (1)")
	flip := map[string]string{"=": "=", "!=": "!=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}
	pairs := [][2]string{{"1", "1.5"}, {"2", "2.0"}, {"-3", "-2.5"}, {"1.5", "1.5"}}
	for op, rev := range flip {
		for _, p := range pairs {
			a := len(mustExec(t, db, "select id from t where "+p[0]+" "+op+" "+p[1]).Rows)
			b := len(mustExec(t, db, "select id from t where "+p[1]+" "+rev+" "+p[0]).Rows)
			if a != b {
				t.Errorf("%s %s %s gave %d row(s), %s %s %s gave %d", p[0], op, p[1], a, p[1], rev, p[0], b)
			}
		}
	}
	if r := mustExec(t, db, "select id from t where 1 < 1.5 and 1.5 > 1 and 2 = 2.0"); len(r.Rows) != 1 {
		t.Fatalf("mixed literal comparisons = %d rows, want 1", len(r.Rows))
	}
}

// A conflict target names each column once.
func TestConflictTargetRejectsRepeatedColumns(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table t (a int64, b int64, v int64, email text unique, primary key (a, b))")
	mustExec(t, db, "insert into t values (1, 2, 0, 'x')")
	for _, q := range []string{
		"insert into t values (1, 2, 1, 'y') on conflict (a, a) do update set v = excluded.v",
		"insert into t values (1, 3, 1, 'x') on conflict (email, email) do nothing",
	} {
		if err := execErr(t, db, q); !errors.Is(err, ErrExec) {
			t.Errorf("%s: %v", q, err)
		}
	}
	if r := mustExec(t, db, "select v from t where a = 1 and b = 2"); r.Rows[0][0].I64 != 0 {
		t.Fatalf("rejected upsert changed the row: %+v", r.Rows)
	}
	mustExec(t, db, "insert into t values (1, 2, 1, 'y') on conflict (b, a) do update set v = excluded.v")
}

// An INSERT ... SELECT is checked the same way whether or not the SELECT
// returns rows, and an empty one still has its RETURNING columns.
func TestEmptyInsertSelectIsCheckedAndShaped(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table t (id int64 primary key, v int64)")
	mustExec(t, db, "create table src (id text primary key)")
	mustExec(t, db, "create table ints (id int64 primary key, n int64)")
	mustExec(t, db, "create table dst (id int64 primary key, x float64)")

	r := mustExec(t, db, "insert into t select id, v from t returning id")
	if len(r.Columns) != 1 || r.Columns[0] != "id" || len(r.Rows) != 0 {
		t.Fatalf("empty INSERT SELECT RETURNING = %+v", r)
	}
	r = mustExec(t, db, "insert into t select * from t on conflict do nothing returning *")
	if len(r.Columns) != 2 {
		t.Fatalf("empty upsert RETURNING columns = %v", r.Columns)
	}
	for _, q := range []string{
		"insert into t select * from t on conflict (v) do nothing",
		"insert into t select * from t on conflict (id) do update set nope = 1",
		"insert into dst (id) select id from src",
		"insert into t select id from t",
	} {
		if err := execErr(t, db, q); !errors.Is(err, ErrExec) {
			t.Errorf("%s on empty tables: %v, want an execution error", q, err)
		}
	}
	mustExec(t, db, "insert into dst select id, n from ints") // int64 widens into float64
}

// Whether a predicate is valid never depends on the rows or the index.
func TestPredicateValidityIndependentOfIndexAndRows(t *testing.T) {
	t.Parallel()
	bad := []struct {
		q    string
		args []table.Value
	}{
		{"select * from t where created = 'not-a-time'", nil},
		{"select * from t where CAST('abc' AS int64) = id", nil},
		{"select * from t where body = ?", []table.Value{table.BytesValue([]byte{0xff})}},
		{"select * from t where x = 9007199254740993", nil},
	}
	for _, indexed := range []bool{false, true} {
		db := mustOpenSQLTable(t)
		mustExec(t, db, "create table t (id int64 primary key, created timestamp, body text, x float64)")
		if indexed {
			mustExec(t, db, "create index t_created on t (created)")
			mustExec(t, db, "create index t_body on t (body)")
			mustExec(t, db, "create index t_x on t (x)")
		}
		for _, c := range bad {
			stmt, err := ParseParameters(c.q, c.args)
			if err == nil {
				_, err = ExecuteStatement(db, stmt)
			}
			if !errors.Is(err, table.ErrInvalidValue) {
				t.Errorf("indexed=%v, empty table: %s: %v, want an invalid-value error", indexed, c.q, err)
			}
		}
		db.Close()
	}
}
