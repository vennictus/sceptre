package sql

import (
	"errors"
	"testing"
)

func TestParseCreateTable(t *testing.T) {
	t.Parallel()

	stmt, err := Parse("CREATE TABLE users (id INT64, name BYTES, PRIMARY KEY (id))")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	create, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("Parse() = %T, want *CreateTableStmt", stmt)
	}
	if got, want := create.Name, "users"; got != want {
		t.Fatalf("Name = %q, want %q", got, want)
	}
	if len(create.Columns) != 2 {
		t.Fatalf("column count = %d, want 2", len(create.Columns))
	}
	if got, want := create.Columns[0].Type, "int64"; got != want {
		t.Fatalf("column type = %q, want %q", got, want)
	}
	if len(create.PrimaryKey) != 1 || create.PrimaryKey[0] != "id" {
		t.Fatalf("PrimaryKey = %v, want [id]", create.PrimaryKey)
	}
}

func TestParseCreateIndex(t *testing.T) {
	t.Parallel()

	stmt, err := Parse("create index users_age on users (age, id)")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	create, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("Parse() = %T, want *CreateIndexStmt", stmt)
	}
	if create.Name != "users_age" || create.Table != "users" {
		t.Fatalf("CreateIndex = %+v", create)
	}
	if len(create.Columns) != 2 || create.Columns[0] != "age" || create.Columns[1] != "id" {
		t.Fatalf("Columns = %v, want [age id]", create.Columns)
	}
}

func TestParseInsert(t *testing.T) {
	t.Parallel()

	stmt, err := Parse("insert into users (id, name) values (-7, 'Ada''s')")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	insert, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("Parse() = %T, want *InsertStmt", stmt)
	}
	if insert.Table != "users" {
		t.Fatalf("Table = %q, want users", insert.Table)
	}
	if len(insert.Values) != 2 || insert.Values[0].Int != -7 || insert.Values[1].String != "Ada's" {
		t.Fatalf("Values = %+v", insert.Values)
	}
}

func TestParseSelectWherePrecedence(t *testing.T) {
	t.Parallel()

	stmt, err := Parse("select id, name from users where age >= 30 and name != 'x' or id = 1 limit 10 offset 2")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("Parse() = %T, want *SelectStmt", stmt)
	}
	if selectStmt.Table != "users" || len(selectStmt.Columns) != 2 {
		t.Fatalf("Select = %+v", selectStmt)
	}
	if selectStmt.Limit == nil || *selectStmt.Limit != 10 {
		t.Fatalf("Limit = %v, want 10", selectStmt.Limit)
	}
	if selectStmt.Offset == nil || *selectStmt.Offset != 2 {
		t.Fatalf("Offset = %v, want 2", selectStmt.Offset)
	}
	root, ok := selectStmt.Where.(*BinaryExpr)
	if !ok || root.Op != "or" {
		t.Fatalf("Where root = %#v, want OR binary", selectStmt.Where)
	}
	left, ok := root.Left.(*BinaryExpr)
	if !ok || left.Op != "and" {
		t.Fatalf("Where left = %#v, want AND binary", root.Left)
	}
}

func TestParseUpdateAndDelete(t *testing.T) {
	t.Parallel()

	updateStmt, err := Parse("update users set name = 'Ada', age = 32 where id = 1")
	if err != nil {
		t.Fatalf("Parse(update) error = %v", err)
	}
	update, ok := updateStmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("Parse(update) = %T, want *UpdateStmt", updateStmt)
	}
	if update.Table != "users" || len(update.Assignments) != 2 || update.Where == nil {
		t.Fatalf("Update = %+v", update)
	}

	deleteStmt, err := Parse("delete from users where id = 1")
	if err != nil {
		t.Fatalf("Parse(delete) error = %v", err)
	}
	deleteParsed, ok := deleteStmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("Parse(delete) = %T, want *DeleteStmt", deleteStmt)
	}
	if deleteParsed.Table != "users" || deleteParsed.Where == nil {
		t.Fatalf("Delete = %+v", deleteParsed)
	}
}

func TestParseRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	invalid := []string{
		"",
		"insert into users (id) values (1, 2)",
		"select from users",
		"create table users (id INT64, primary (id))",
		"insert into users (name) values ('unterminated)",
	}
	for _, input := range invalid {
		if _, err := Parse(input); !errors.Is(err, ErrParse) {
			t.Fatalf("Parse(%q) error = %v, want %v", input, err, ErrParse)
		}
	}
}
