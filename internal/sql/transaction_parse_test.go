package sql

import (
	"reflect"
	"testing"
)

func TestParseTransactionControl(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]Statement{
		"BEGIN":                     &BeginStmt{},
		"begin transaction":         &BeginStmt{},
		"Begin Work":                &BeginStmt{},
		"COMMIT":                    &CommitStmt{},
		"commit transaction":        &CommitStmt{},
		"ROLLBACK":                  &RollbackStmt{},
		"rollback work":             &RollbackStmt{},
		"ROLLBACK TO before_edit":   &RollbackStmt{Savepoint: "before_edit"},
		"rollback to savepoint S1":  &RollbackStmt{Savepoint: "s1"},
		"rollback transaction to a": &RollbackStmt{Savepoint: "a"},
		"SAVEPOINT A":               &SavepointStmt{Name: "a"},
		"release savepoint a":       &ReleaseStmt{Name: "a"},
		"RELEASE a":                 &ReleaseStmt{Name: "a"},
	} {
		got, err := Parse(input)
		if err != nil {
			t.Fatalf("Parse(%q): %v", input, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Parse(%q) = %#v, want %#v", input, got, want)
		}
		if !IsTransactionControl(got) {
			t.Fatalf("IsTransactionControl(%q) = false", input)
		}
	}
	for _, input := range []string{"savepoint", "release", "rollback to", "begin begin", "commit now", "savepoint 1"} {
		if _, err := Parse(input); err == nil {
			t.Fatalf("Parse(%q) accepted", input)
		}
	}
	if stmt, _ := Parse("select * from t"); IsTransactionControl(stmt) {
		t.Fatal("SELECT is not transaction control")
	}
}

func TestExecuteRejectsTransactionControl(t *testing.T) {
	t.Parallel()
	for _, stmt := range []Statement{&BeginStmt{}, &CommitStmt{}, &RollbackStmt{}, &SavepointStmt{Name: "a"}, &ReleaseStmt{Name: "a"}} {
		if _, err := ExecuteStatement(nil, stmt); err == nil {
			t.Fatalf("ExecuteStatement(%T) accepted", stmt)
		}
	}
}
