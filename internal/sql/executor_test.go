package sql

import (
	"path/filepath"
	"sceptre/internal/table"
	"testing"
)

func TestExecuteCreateInsertSelect(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLTable(t)
	defer db.Close()

	if _, err := Execute(db, "create table users (id int64, name bytes, age int64, primary key (id))"); err != nil {
		t.Fatalf("Execute(create table) error = %v", err)
	}
	if result, err := Execute(db, "insert into users (id, name, age) values (1, 'Ada', 31)"); err != nil {
		t.Fatalf("Execute(insert Ada) error = %v", err)
	} else if result.RowsAffected != 1 {
		t.Fatalf("insert RowsAffected = %d, want 1", result.RowsAffected)
	}
	if _, err := Execute(db, "insert into users (id, name, age) values (2, 'Grace', 40)"); err != nil {
		t.Fatalf("Execute(insert Grace) error = %v", err)
	}

	result, err := Execute(db, "select id, name from users where age >= 40")
	if err != nil {
		t.Fatalf("Execute(select) error = %v", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("select row count = %d, want 1", len(result.Rows))
	}
	if got := result.Rows[0][0].I64; got != 2 {
		t.Fatalf("selected id = %d, want 2", got)
	}
	if got := string(result.Rows[0][1].Bytes); got != "Grace" {
		t.Fatalf("selected name = %q, want Grace", got)
	}
}

func TestExecuteUpdateDeleteAndIndex(t *testing.T) {
	t.Parallel()

	db := mustOpenSQLTable(t)
	defer db.Close()

	if _, err := Execute(db, "create table users (id int64, name bytes, age int64, primary key (id))"); err != nil {
		t.Fatalf("Execute(create table) error = %v", err)
	}
	if _, err := Execute(db, "create index users_age on users (age)"); err != nil {
		t.Fatalf("Execute(create index) error = %v", err)
	}
	if _, err := Execute(db, "insert into users (id, name, age) values (1, 'Ada', 31)"); err != nil {
		t.Fatalf("Execute(insert Ada) error = %v", err)
	}
	if _, err := Execute(db, "insert into users (id, name, age) values (2, 'Grace', 40)"); err != nil {
		t.Fatalf("Execute(insert Grace) error = %v", err)
	}

	result, err := Execute(db, "update users set age = 32 where name = 'Ada'")
	if err != nil {
		t.Fatalf("Execute(update) error = %v", err)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("update RowsAffected = %d, want 1", result.RowsAffected)
	}

	rows, err := db.LookupIndex("users", "users_age", table.NewRecord(map[string]table.Value{"age": table.Int64Value(32)}))
	if err != nil {
		t.Fatalf("LookupIndex(age=32) error = %v", err)
	}
	if len(rows) != 1 || rows[0].Values["id"].I64 != 1 {
		t.Fatalf("LookupIndex(age=32) rows = %+v", rows)
	}

	result, err = Execute(db, "delete from users where age < 40")
	if err != nil {
		t.Fatalf("Execute(delete) error = %v", err)
	}
	if result.RowsAffected != 1 {
		t.Fatalf("delete RowsAffected = %d, want 1", result.RowsAffected)
	}

	result, err = Execute(db, "select * from users")
	if err != nil {
		t.Fatalf("Execute(select all) error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0][0].I64 != 2 {
		t.Fatalf("remaining rows = %+v", result.Rows)
	}
}

func mustOpenSQLTable(t *testing.T) *table.DB {
	t.Helper()

	db, err := table.Open(filepath.Join(t.TempDir(), "sceptre.db"), table.Options{PageSize: 512})
	if err != nil {
		t.Fatalf("table.Open() error = %v", err)
	}
	return db
}
