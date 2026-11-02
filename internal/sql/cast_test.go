package sql

import (
	"math"
	"testing"
	"time"

	"github.com/vennictus/sceptre/internal/table"
)

func TestCastValue(t *testing.T) {
	t.Parallel()
	ts := table.TimestampValue(time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC))
	for _, c := range []struct {
		in   table.Value
		to   table.Type
		want string
	}{
		{table.Int64Value(3), table.TypeFloat64, "3"},
		{table.Float64Value(4), table.TypeInt64, "4"},
		{table.TextValue(" 42 "), table.TypeInt64, "42"},
		{table.TextValue("2.5"), table.TypeFloat64, "2.5"},
		{table.TextValue("TRUE"), table.TypeBool, "true"},
		{table.Int64Value(0), table.TypeBool, "false"},
		{table.BoolValue(true), table.TypeInt64, "1"},
		{table.TextValue("2026-11-02"), table.TypeTimestamp, "2026-11-02T00:00:00Z"},
		{ts, table.TypeText, "2026-11-02T00:00:00Z"},
		{table.Float64Value(0.5), table.TypeText, "0.5"},
		{table.BytesValue([]byte("hi")), table.TypeText, "hi"},
		{table.TextValue("hi"), table.TypeBytes, "hi"},
		{table.NullValue(), table.TypeInt64, "NULL"},
	} {
		got, err := castValue(c.in, c.to)
		if err != nil || table.FormatValue(got) != c.want || (!got.IsNull() && got.Type != c.to) {
			t.Fatalf("CAST(%s AS %s) = %s (%v), %v; want %s", table.FormatValue(c.in), c.to, table.FormatValue(got), got.Type, err, c.want)
		}
	}
	for _, c := range []struct {
		in table.Value
		to table.Type
	}{
		{table.Float64Value(1.5), table.TypeInt64},
		{table.Float64Value(math.MaxFloat64), table.TypeInt64},
		{table.TextValue("x"), table.TypeInt64},
		{table.Int64Value(2), table.TypeBool},
		{table.BytesValue([]byte{0xff}), table.TypeText},
		{table.BoolValue(true), table.TypeTimestamp},
		{table.TextValue("NaN"), table.TypeFloat64},
	} {
		if got, err := castValue(c.in, c.to); err == nil {
			t.Fatalf("CAST(%s AS %s) = %s, want an error", table.FormatValue(c.in), c.to, table.FormatValue(got))
		}
	}
}

func TestCastInWhere(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	mustExec(t, db, "create table t (id int64, code text, score float64, primary key (id))")
	mustExec(t, db, "insert into t (id, code, score) values (1, '10', 1.0)")
	mustExec(t, db, "insert into t (id, code, score) values (2, '9', 2.5)")
	wantIDs(t, db, "select id from t where cast(code as int64) > 9", 1)
	wantIDs(t, db, "select id from t where cast(score as text) = '2.5'", 2)
	wantIDs(t, db, "select id from t where id = cast('2' as int64)", 2)
	if err := execErr(t, db, "select id from t where cast(score as int64) = 1"); err == nil {
		t.Fatal("cast of 2.5 to int64 succeeded")
	}
	if err := execErr(t, db, "select id from t where cast(code as money) = 1"); err == nil {
		t.Fatal("unknown cast type accepted")
	}
	if got := FormatExpr(mustParse(t, "select * from t where CAST(code AS int64) = 1").(*SelectStmt).Where); got != "CAST(code AS int64) = 1" {
		t.Fatalf("FormatExpr = %q", got)
	}
	// A column named cast still works.
	mustExec(t, db, "create table c (id int64, cast int64, primary key (id))")
	mustExec(t, db, "insert into c (id, cast) values (1, 5)")
	wantIDs(t, db, "select id from c where cast = 5", 1)
}
