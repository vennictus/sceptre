package table

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestSecondaryIndexBackfillAndLookup(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)

	mustInsertUser(t, db, 1, "Ada", 31)
	mustInsertUser(t, db, 2, "Grace", 31)
	mustInsertUser(t, db, 3, "Linus", 40)

	if err := db.CreateIndex("users", IndexDef{Name: "users_age", Columns: []string{"age"}}); err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}

	index, ok, err := db.Index("users", "users_age")
	if err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if !ok {
		t.Fatal("Index() ok = false, want true")
	}
	if index.Prefix == 0 {
		t.Fatal("Index().Prefix = 0, want non-zero")
	}

	rows, err := db.LookupIndex("users", "users_age", NewRecord(map[string]Value{"age": Int64Value(31)}))
	if err != nil {
		t.Fatalf("LookupIndex(age=31) error = %v", err)
	}
	assertRowIDs(t, rows, []int64{1, 2})
}

func TestSecondaryIndexMaintenance(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)

	mustInsertUser(t, db, 1, "Ada", 31)
	mustInsertUser(t, db, 2, "Grace", 31)
	if err := db.CreateIndex("users", IndexDef{Name: "users_age", Columns: []string{"age"}}); err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}

	mustInsertUser(t, db, 3, "Barbara", 31)
	rows, err := db.LookupIndex("users", "users_age", NewRecord(map[string]Value{"age": Int64Value(31)}))
	if err != nil {
		t.Fatalf("LookupIndex(age=31) after insert error = %v", err)
	}
	assertRowIDs(t, rows, []int64{1, 2, 3})

	if err := db.Update("users", NewRecord(map[string]Value{
		"id":   Int64Value(2),
		"name": BytesValue([]byte("Grace")),
		"age":  Int64Value(40),
	})); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	rows, err = db.LookupIndex("users", "users_age", NewRecord(map[string]Value{"age": Int64Value(31)}))
	if err != nil {
		t.Fatalf("LookupIndex(age=31) after update error = %v", err)
	}
	assertRowIDs(t, rows, []int64{1, 3})

	rows, err = db.LookupIndex("users", "users_age", NewRecord(map[string]Value{"age": Int64Value(40)}))
	if err != nil {
		t.Fatalf("LookupIndex(age=40) after update error = %v", err)
	}
	assertRowIDs(t, rows, []int64{2})

	removed, err := db.Delete("users", NewRecord(map[string]Value{"id": Int64Value(1)}))
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !removed {
		t.Fatal("Delete() removed = false, want true")
	}

	rows, err = db.LookupIndex("users", "users_age", NewRecord(map[string]Value{"age": Int64Value(31)}))
	if err != nil {
		t.Fatalf("LookupIndex(age=31) after delete error = %v", err)
	}
	assertRowIDs(t, rows, []int64{3})
}

func TestSecondaryIndexValidation(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)

	if err := db.CreateIndex("users", IndexDef{Name: "bad", Columns: []string{"missing"}}); !errors.Is(err, ErrInvalidIndexDef) {
		t.Fatalf("CreateIndex(missing column) error = %v, want %v", err, ErrInvalidIndexDef)
	}
	if err := db.CreateIndex("users", IndexDef{Name: "users_age", Columns: []string{"age"}}); err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}
	if err := db.CreateIndex("users", IndexDef{Name: "users_age", Columns: []string{"name"}}); !errors.Is(err, ErrIndexExists) {
		t.Fatalf("CreateIndex(duplicate) error = %v, want %v", err, ErrIndexExists)
	}

	_, err := db.LookupIndex("users", "missing", NewRecord(map[string]Value{"age": Int64Value(31)}))
	if !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("LookupIndex(missing) error = %v, want %v", err, ErrIndexNotFound)
	}

	_, err = db.LookupIndex("users", "users_age", NewRecord(map[string]Value{"age": BytesValue([]byte("31"))}))
	if !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("LookupIndex(wrong type) error = %v, want %v", err, ErrInvalidValue)
	}
}

func assertRowIDs(t *testing.T, rows []Record, want []int64) {
	t.Helper()

	if len(rows) != len(want) {
		t.Fatalf("row count = %d, want %d", len(rows), len(want))
	}
	for i := range want {
		if got := rows[i].Values["id"].I64; got != want[i] {
			t.Fatalf("row[%d].id = %d, want %d", i, got, want[i])
		}
	}
}
