package sql

import (
	"errors"
	"testing"
)

func TestExplainPrimaryKeyLookup(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLTable(t)
	defer db.Close()

	if _, err := Execute(db, "create table users (id int64, name bytes, age int64, primary key (id))"); err != nil {
		t.Fatalf("Execute(create table) error = %v", err)
	}

	plan, err := Explain(db, "select * from users where id = 1")
	if err != nil {
		t.Fatalf("Explain() error = %v", err)
	}
	if plan.Statement != "select" {
		t.Fatalf("Statement = %q, want select", plan.Statement)
	}
	if plan.Table != "users" {
		t.Fatalf("Table = %q, want users", plan.Table)
	}
	if plan.Access != AccessPrimaryKeyLookup {
		t.Fatalf("Access = %q, want %q", plan.Access, AccessPrimaryKeyLookup)
	}
	if len(plan.Lookup) != 1 {
		t.Fatalf("Lookup count = %d, want 1", len(plan.Lookup))
	}
	if got := plan.Lookup[0]; got.Column != "id" || got.Op != "=" || got.Literal.Kind != LiteralInt || got.Literal.Int != 1 {
		t.Fatalf("Lookup = %+v, want id = 1", got)
	}
	if plan.Residual != nil {
		t.Fatalf("Residual = %s, want nil", FormatExpr(plan.Residual))
	}
}

func TestExplainPrimaryKeyRangeWithPagination(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLTable(t)
	defer db.Close()

	if _, err := Execute(db, "create table users (id int64, name bytes, age int64, primary key (id))"); err != nil {
		t.Fatalf("Execute(create table) error = %v", err)
	}

	plan, err := Explain(db, "select * from users where id >= 10 and id < 20 limit 5 offset 2")
	if err != nil {
		t.Fatalf("Explain() error = %v", err)
	}
	if plan.Access != AccessPrimaryKeyRange {
		t.Fatalf("Access = %q, want %q", plan.Access, AccessPrimaryKeyRange)
	}
	if plan.Lower == nil || plan.Lower.Column != "id" || plan.Lower.Op != ">=" || plan.Lower.Literal.Int != 10 {
		t.Fatalf("Lower = %+v, want id >= 10", plan.Lower)
	}
	if plan.Upper == nil || plan.Upper.Column != "id" || plan.Upper.Op != "<" || plan.Upper.Literal.Int != 20 {
		t.Fatalf("Upper = %+v, want id < 20", plan.Upper)
	}
	if plan.Limit == nil || *plan.Limit != 5 {
		t.Fatalf("Limit = %v, want 5", plan.Limit)
	}
	if plan.Offset == nil || *plan.Offset != 2 {
		t.Fatalf("Offset = %v, want 2", plan.Offset)
	}
	if plan.Residual != nil {
		t.Fatalf("Residual = %s, want nil", FormatExpr(plan.Residual))
	}
}

func TestExplainSecondaryIndexLookupWithResidual(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLTable(t)
	defer db.Close()

	if _, err := Execute(db, "create table users (id int64, name bytes, age int64, primary key (id))"); err != nil {
		t.Fatalf("Execute(create table) error = %v", err)
	}
	if _, err := Execute(db, "create index users_age on users (age)"); err != nil {
		t.Fatalf("Execute(create index) error = %v", err)
	}

	plan, err := Explain(db, "select * from users where age = 31 and name = 'Ada'")
	if err != nil {
		t.Fatalf("Explain() error = %v", err)
	}
	if plan.Access != AccessSecondaryIndexLookup {
		t.Fatalf("Access = %q, want %q", plan.Access, AccessSecondaryIndexLookup)
	}
	if plan.Index != "users_age" {
		t.Fatalf("Index = %q, want users_age", plan.Index)
	}
	if len(plan.Lookup) != 1 {
		t.Fatalf("Lookup count = %d, want 1", len(plan.Lookup))
	}
	if got := plan.Lookup[0]; got.Column != "age" || got.Op != "=" || got.Literal.Int != 31 {
		t.Fatalf("Lookup = %+v, want age = 31", got)
	}
	if got := FormatExpr(plan.Residual); got != "name = 'Ada'" {
		t.Fatalf("Residual = %q, want %q", got, "name = 'Ada'")
	}
}

func TestExplainSupportsUpdateAndDelete(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLTable(t)
	defer db.Close()

	if _, err := Execute(db, "create table users (id int64, name bytes, age int64, primary key (id))"); err != nil {
		t.Fatalf("Execute(create table) error = %v", err)
	}

	updatePlan, err := Explain(db, "update users set name = 'Ada' where id = 7")
	if err != nil {
		t.Fatalf("Explain(update) error = %v", err)
	}
	if updatePlan.Access != AccessPrimaryKeyLookup {
		t.Fatalf("update Access = %q, want %q", updatePlan.Access, AccessPrimaryKeyLookup)
	}

	deletePlan, err := Explain(db, "delete from users where id > 7 and id <= 10")
	if err != nil {
		t.Fatalf("Explain(delete) error = %v", err)
	}
	if deletePlan.Access != AccessPrimaryKeyRange {
		t.Fatalf("delete Access = %q, want %q", deletePlan.Access, AccessPrimaryKeyRange)
	}
}

func TestExplainRejectsUnsupportedStatement(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLTable(t)
	defer db.Close()

	if _, err := Explain(db, "create table users (id int64, primary key (id))"); !errors.Is(err, ErrExec) {
		t.Fatalf("Explain(create table) error = %v, want %v", err, ErrExec)
	}
}
