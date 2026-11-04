package sql

import (
	"errors"
	"reflect"
	"testing"
)

func FuzzParse(f *testing.F) {
	// One seed per grammar family, so the fuzzer starts from every form the
	// parser accepts.
	for _, input := range []string{
		"select * from t where id >= -9223372036854775808 limit 0",
		"insert into t (id,body) values (1,'a''b')",
		"update t set body = 'x' where id = 2 or (id = 1 and body != 'y')",
		"create table t (id int64,body bytes,primary key (id))",
		"create index body_idx on t (body)",
		"delete from t", "", "select (", "select * from t limit -1",
		`create table if not exists "t t" (id int64 primary key autoincrement, s text not null unique default 'x' check (s != ''), f float64, b bool default false, at timestamp default current_timestamp, check (f is null or f > 0.5))`,
		"create unique index if not exists i on t (s, f)",
		"insert into t values (1, X'ff00', 1.5e3, true, timestamp '2026-11-05 10:00:00'), (2, NULL, -0.0, FALSE, '2026-11-05')",
		"insert into t select * from u on conflict (id) do update set s = excluded.s, f = 1 returning *",
		"insert into t (id) values (1) on conflict do nothing returning id",
		`select "select", s from t where CAST(f AS int64) = 3 and at is not null and "true" <> 'x' limit 5 offset 2`,
		"update t set s = NULL where id = 1 returning s", "delete from t where id < 3 returning *",
		"drop table if exists t", "drop index if exists i on t",
		"alter table t add column n int64 not null default 0 check (n >= 0)", "alter table t drop column n",
		"alter table t rename column a to b", "alter table t rename to u",
		"begin", "commit", "rollback", "savepoint s", "release savepoint s", "rollback to savepoint s",
	} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 8192 {
			return
		}
		stmt, err := Parse(input)
		if err != nil {
			if stmt != nil || !errors.Is(err, ErrParse) {
				t.Fatalf("inconsistent parse failure: %T %v", stmt, err)
			}
			return
		}
		var exprs []Expr
		switch s := stmt.(type) {
		case *SelectStmt:
			exprs = append(exprs, s.Where)
		case *UpdateStmt:
			exprs = append(exprs, s.Where)
		case *DeleteStmt:
			exprs = append(exprs, s.Where)
		case *CreateTableStmt:
			exprs = append(exprs, s.Checks...)
			for _, c := range s.Columns {
				exprs = append(exprs, c.Checks...)
			}
		case *AlterTableStmt:
			if s.Add != nil {
				exprs = append(exprs, s.Add.Checks...)
			}
		case *CreateIndexStmt, *InsertStmt, *DropTableStmt, *DropIndexStmt,
			*BeginStmt, *CommitStmt, *RollbackStmt, *SavepointStmt, *ReleaseStmt:
		default:
			t.Fatalf("successful parse returned unexpected statement %T", stmt)
		}
		// Every parsed expression survives the stored CHECK form exactly.
		for _, expr := range exprs {
			if expr == nil {
				continue
			}
			text := StoreExpr(expr)
			back, err := ParseExpr(text)
			if err != nil {
				t.Fatalf("stored form %q of %q does not parse: %v", text, input, err)
			}
			if !reflect.DeepEqual(normalizeStored(expr), back) {
				t.Fatalf("stored form %q of %q parses to a different expression", text, input)
			}
		}
	})
}

// normalizeStored returns expr as StoreExpr writes it: a timestamp literal
// outside four-digit years becomes an exact CAST of its microseconds.
func normalizeStored(expr Expr) Expr {
	switch e := expr.(type) {
	case *Literal:
		if e.Kind == LiteralTimestamp && storeLiteral(*e)[0] == 'C' {
			return &CastExpr{Expr: &Literal{Kind: LiteralInt, Int: e.Int}, Type: "timestamp"}
		}
		c := *e
		return &c
	case *BinaryExpr:
		return &BinaryExpr{Left: normalizeStored(e.Left), Op: e.Op, Right: normalizeStored(e.Right)}
	case *CastExpr:
		return &CastExpr{Expr: normalizeStored(e.Expr), Type: e.Type}
	case *IsNullExpr:
		return &IsNullExpr{Expr: normalizeStored(e.Expr), Not: e.Not}
	case *IdentExpr:
		c := *e
		return &c
	}
	return expr
}
