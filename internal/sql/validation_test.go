package sql

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/vennictus/sceptre/internal/table"
)

func TestSQLRejectsPrimaryKeyAssignmentsWithoutChangingRows(t *testing.T) {
	t.Parallel()
	for _, query := range []string{
		"update users set id = 2 where id = 1",
		"update users set id = 3 where id = 1",
		"update users set id = 1 where id = 1",
		"update users set name = 'changed', id = 2 where id = 1",
		"update users set id = 2 where id = 99",
	} {
		t.Run(query, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			db, err := table.Open(path, table.Options{PageSize: 1024})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			for _, statement := range []string{
				"create table users (id int64, name bytes, primary key (id))",
				"create index users_name on users (name)",
				"insert into users (id,name) values (1,'original')",
				"insert into users (id,name) values (2,'victim')",
			} {
				if _, err := Execute(db, statement); err != nil {
					t.Fatal(err)
				}
			}
			before, err := Execute(db, "select * from users")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Execute(db, query); !errors.Is(err, ErrExec) {
				t.Fatalf("Execute error=%v; want ErrExec", err)
			}
			if _, err := Explain(db, query); !errors.Is(err, ErrExec) {
				t.Fatalf("Explain error=%v; want ErrExec", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = table.Open(path, table.Options{})
			if err != nil {
				t.Fatal(err)
			}
			after, err := Execute(db, "select * from users")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("rows changed: before=%+v after=%+v", before, after)
			}
			report, err := db.Check()
			if err != nil || !report.OK() {
				t.Fatalf("integrity: report=%+v error=%v", report, err)
			}
		})
	}
}

func TestSQLRejectsCompositePrimaryKeyAssignments(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	if _, err := Execute(db, "create table pairs (a int64, b bytes, value int64, primary key (a,b))"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"update pairs set a = 1", "update pairs set b = 'x'"} {
		if _, err := Execute(db, query); !errors.Is(err, ErrExec) {
			t.Fatalf("%s: error=%v", query, err)
		}
	}
}

func TestSQLValidationDoesNotDependOnRowsOrAccessPath(t *testing.T) {
	t.Parallel()
	for _, populated := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "populated"}[populated], func(t *testing.T) {
			db := mustOpenSQLTable(t)
			defer db.Close()
			for _, q := range []string{"create table users (id int64, name bytes, age int64, primary key (id))", "create index users_age on users (age)"} {
				if _, err := Execute(db, q); err != nil {
					t.Fatal(err)
				}
			}
			if populated {
				if _, err := Execute(db, "insert into users (id,name,age) values (1,'Ada',31)"); err != nil {
					t.Fatal(err)
				}
			}
			predicates := []string{"missing = 1", "name = 1", "age = 'x'", "id = 99 and missing = 1", "age = 99 and name = 1", "id < 0 and missing = 1", "id = 1 and name", "name or id = 1", "(id = 1) = (age = 31)"}
			for _, predicate := range predicates {
				for _, prefix := range []string{"select * from users where ", "delete from users where ", "update users set age = 32 where "} {
					q := prefix + predicate
					if _, err := Execute(db, q); !errors.Is(err, ErrExec) {
						t.Errorf("Execute(%q): error=%v", q, err)
					}
					if _, err := Explain(db, q); !errors.Is(err, ErrExec) {
						t.Errorf("Explain(%q): error=%v", q, err)
					}
					if prefix == "select * from users where " {
						if _, err := Analyze(db, q); !errors.Is(err, ErrExec) {
							t.Errorf("Analyze(%q): error=%v", q, err)
						}
					}
				}
			}
			for _, q := range []string{"update users set missing = 1 where id = 99", "update users set age = 'bad' where age = 99", "update users set age = 1, age = 2 where id < 0"} {
				if _, err := Execute(db, q); !errors.Is(err, ErrExec) {
					t.Errorf("Execute(%q): error=%v", q, err)
				}
				if _, err := Explain(db, q); !errors.Is(err, ErrExec) {
					t.Errorf("Explain(%q): error=%v", q, err)
				}
			}
			if _, err := Explain(db, "select missing from users"); !errors.Is(err, ErrExec) {
				t.Errorf("invalid projection Explain error=%v", err)
			}
			result, err := Execute(db, "select * from users")
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if populated {
				want = 1
			}
			if len(result.Rows) != want {
				t.Fatalf("invalid statements changed row count: %d", len(result.Rows))
			}
			if populated && (result.Rows[0][2].I64 != 31 || string(result.Rows[0][1].Bytes) != "Ada") {
				t.Fatalf("invalid statements changed row: %+v", result.Rows)
			}
		})
	}
}

func TestSQLRejectsDuplicateInsertColumns(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	if _, err := Execute(db, "create table users (id int64, name bytes, primary key (id))"); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{"insert into users (id,id,name) values (1,2,'Ada')", "insert into users (id,name,name) values (1,'Ada','Grace')"} {
		if _, err := Execute(db, q); !errors.Is(err, ErrExec) {
			t.Errorf("%q: error=%v", q, err)
		}
	}
	result, err := Execute(db, "select * from users")
	if err != nil || len(result.Rows) != 0 {
		t.Fatalf("invalid inserts persisted: %+v error=%v", result, err)
	}
}

func TestSQLValidatesConstructedStatements(t *testing.T) {
	t.Parallel()
	db := mustOpenSQLTable(t)
	defer db.Close()
	if _, err := Execute(db, "create table users (id int64, name bytes, primary key (id))"); err != nil {
		t.Fatal(err)
	}
	negative := int64(-1)
	for _, stmt := range []Statement{
		&InsertStmt{Table: "users", Columns: []string{"id", "name"}, Values: []Literal{{Kind: LiteralInt, Int: 1}}},
		&InsertStmt{Table: "users"},
		&InsertStmt{Table: "users", Columns: []string{"id", "id", "name"}, Values: []Literal{{Kind: LiteralInt, Int: 1}, {Kind: LiteralInt, Int: 2}, {Kind: LiteralString, String: "x"}}},
		&UpdateStmt{Table: "users"},
		&SelectStmt{Table: "users", AllColumns: true, Limit: &negative},
		&SelectStmt{Table: "users", AllColumns: true, Offset: &negative},
	} {
		if _, err := ExecuteStatement(db, stmt); !errors.Is(err, ErrExec) {
			t.Errorf("%T: error=%v", stmt, err)
		}
	}
	for _, stmt := range []*SelectStmt{{Table: "users", AllColumns: true, Limit: &negative}, {Table: "users", AllColumns: true, Offset: &negative}} {
		if _, err := AnalyzeSelect(db, stmt); !errors.Is(err, ErrExec) {
			t.Errorf("AnalyzeSelect error=%v", err)
		}
		if _, err := ExplainStatement(db, stmt); !errors.Is(err, ErrExec) {
			t.Errorf("ExplainStatement error=%v", err)
		}
	}
}

func TestSQLRejectsNegativePaginationAndAcceptsZero(t *testing.T) {
	t.Parallel()
	for _, q := range []string{"select * from users limit -1", "select * from users limit -2", "select * from users offset -1", "select * from users limit 1 offset -1"} {
		if _, err := Parse(q); !errors.Is(err, ErrParse) {
			t.Errorf("Parse(%q): error=%v", q, err)
		}
	}
	db := mustOpenSQLTable(t)
	defer db.Close()
	for _, q := range []string{"create table users (id int64, name bytes, primary key (id))", "insert into users (id,name) values (1,'Ada')"} {
		if _, err := Execute(db, q); err != nil {
			t.Fatal(err)
		}
	}
	for _, test := range []struct {
		query string
		want  int
	}{{"select * from users limit 0 offset 0", 0}, {"select * from users limit 1 offset 0", 1}, {"select * from users offset 1", 0}} {
		r, err := Execute(db, test.query)
		if err != nil || len(r.Rows) != test.want {
			t.Errorf("%s: rows=%d error=%v", test.query, len(r.Rows), err)
		}
	}
}
