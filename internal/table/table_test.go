package table

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
)

func TestCreateTablePersistsSchema(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sceptre.db")
	db := mustOpenTableDB(t, path)

	users := usersTableDef()
	if err := db.CreateTable(users); err != nil {
		t.Fatalf("CreateTable(users) error = %v", err)
	}
	if err := db.CreateTable(TableDef{
		Name: "accounts",
		Columns: []Column{
			{Name: "tenant", Type: TypeBytes},
			{Name: "id", Type: TypeInt64},
		},
		PrimaryKey: []string{"tenant", "id"},
	}); err != nil {
		t.Fatalf("CreateTable(accounts) error = %v", err)
	}

	loaded, ok, err := db.Table("users")
	if err != nil {
		t.Fatalf("Table(users) error = %v", err)
	}
	if !ok {
		t.Fatal("Table(users) ok = false, want true")
	}
	if loaded.Prefix != 1 {
		t.Fatalf("users Prefix = %d, want 1", loaded.Prefix)
	}
	if got, want := loaded.Columns[1].Name, "name"; got != want {
		t.Fatalf("users column[1] = %q, want %q", got, want)
	}

	accounts, ok, err := db.Table("accounts")
	if err != nil {
		t.Fatalf("Table(accounts) error = %v", err)
	}
	if !ok {
		t.Fatal("Table(accounts) ok = false, want true")
	}
	if accounts.Prefix != 2 {
		t.Fatalf("accounts Prefix = %d, want 2", accounts.Prefix)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened := mustOpenTableDB(t, path)
	defer reopened.Close()

	loaded, ok, err = reopened.Table("users")
	if err != nil {
		t.Fatalf("Table(users) after reopen error = %v", err)
	}
	if !ok {
		t.Fatal("Table(users) after reopen ok = false, want true")
	}
	if loaded.Prefix != 1 {
		t.Fatalf("reopened users Prefix = %d, want 1", loaded.Prefix)
	}
}

func TestPrimaryKeyCRUD(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	if err := db.CreateTable(usersTableDef()); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	row := NewRecord(map[string]Value{
		"id":   Int64Value(7),
		"name": BytesValue([]byte{0x00, 'a', 0x01}),
		"age":  Int64Value(31),
	})
	if err := db.Insert("users", row); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if err := db.Insert("users", row); !errors.Is(err, ErrRowExists) {
		t.Fatalf("Insert() duplicate error = %v, want %v", err, ErrRowExists)
	}

	got, ok, err := db.Get("users", NewRecord(map[string]Value{"id": Int64Value(7)}))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	assertValue(t, got.Values["name"], BytesValue([]byte{0x00, 'a', 0x01}))
	assertValue(t, got.Values["age"], Int64Value(31))

	updated := NewRecord(map[string]Value{
		"id":   Int64Value(7),
		"name": BytesValue([]byte("Ada")),
		"age":  Int64Value(32),
	})
	if err := db.Update("users", updated); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	got, ok, err = db.Get("users", NewRecord(map[string]Value{"id": Int64Value(7)}))
	if err != nil {
		t.Fatalf("Get() after update error = %v", err)
	}
	if !ok {
		t.Fatal("Get() after update ok = false, want true")
	}
	assertValue(t, got.Values["name"], BytesValue([]byte("Ada")))
	assertValue(t, got.Values["age"], Int64Value(32))

	missing := NewRecord(map[string]Value{
		"id":   Int64Value(8),
		"name": BytesValue([]byte("Grace")),
		"age":  Int64Value(41),
	})
	if err := db.Update("users", missing); !errors.Is(err, ErrRowNotFound) {
		t.Fatalf("Update() missing error = %v, want %v", err, ErrRowNotFound)
	}
	if err := db.Upsert("users", missing); err != nil {
		t.Fatalf("Upsert() missing error = %v", err)
	}

	removed, err := db.Delete("users", NewRecord(map[string]Value{"id": Int64Value(7)}))
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if !removed {
		t.Fatal("Delete() removed = false, want true")
	}
	removed, err = db.Delete("users", NewRecord(map[string]Value{"id": Int64Value(7)}))
	if err != nil {
		t.Fatalf("Delete() second error = %v", err)
	}
	if removed {
		t.Fatal("Delete() second removed = true, want false")
	}
}

func TestCompositePrimaryKeyRoundTrip(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	if err := db.CreateTable(TableDef{
		Name: "accounts",
		Columns: []Column{
			{Name: "tenant", Type: TypeBytes},
			{Name: "id", Type: TypeInt64},
			{Name: "balance", Type: TypeInt64},
		},
		PrimaryKey: []string{"tenant", "id"},
	}); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}

	row := NewRecord(map[string]Value{
		"tenant":  BytesValue([]byte("acme")),
		"id":      Int64Value(-4),
		"balance": Int64Value(900),
	})
	if err := db.Insert("accounts", row); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}

	got, ok, err := db.Get("accounts", NewRecord(map[string]Value{
		"tenant": BytesValue([]byte("acme")),
		"id":     Int64Value(-4),
	}))
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if !ok {
		t.Fatal("Get() ok = false, want true")
	}
	assertValue(t, got.Values["tenant"], BytesValue([]byte("acme")))
	assertValue(t, got.Values["id"], Int64Value(-4))
	assertValue(t, got.Values["balance"], Int64Value(900))
}

func TestValidationErrors(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()

	if err := db.CreateTable(TableDef{Name: "bad"}); !errors.Is(err, ErrInvalidTableDef) {
		t.Fatalf("CreateTable(invalid) error = %v, want %v", err, ErrInvalidTableDef)
	}
	if err := db.CreateTable(usersTableDef()); err != nil {
		t.Fatalf("CreateTable() error = %v", err)
	}
	if err := db.CreateTable(usersTableDef()); !errors.Is(err, ErrTableExists) {
		t.Fatalf("CreateTable(duplicate) error = %v, want %v", err, ErrTableExists)
	}

	missingColumn := NewRecord(map[string]Value{
		"id":   Int64Value(1),
		"name": BytesValue([]byte("Ada")),
	})
	if err := db.Insert("users", missingColumn); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("Insert(missing column) error = %v, want %v", err, ErrInvalidRecord)
	}

	wrongType := NewRecord(map[string]Value{
		"id":   BytesValue([]byte("1")),
		"name": BytesValue([]byte("Ada")),
		"age":  Int64Value(31),
	})
	if err := db.Insert("users", wrongType); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("Insert(wrong type) error = %v, want %v", err, ErrInvalidValue)
	}

	_, _, err := db.Get("missing", NewRecord(map[string]Value{"id": Int64Value(1)}))
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("Get(missing table) error = %v, want %v", err, ErrTableNotFound)
	}
}

func mustOpenTableDB(t *testing.T, path string) *DB {
	t.Helper()

	db, err := Open(path, Options{PageSize: 512})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return db
}

func usersTableDef() TableDef {
	return TableDef{
		Name: "users",
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeBytes},
			{Name: "age", Type: TypeInt64},
		},
		PrimaryKey: []string{"id"},
	}
}

func assertValue(t *testing.T, got, want Value) {
	t.Helper()

	if got.Type != want.Type {
		t.Fatalf("value Type = %d, want %d", got.Type, want.Type)
	}
	switch want.Type {
	case TypeInt64:
		if got.I64 != want.I64 {
			t.Fatalf("value I64 = %d, want %d", got.I64, want.I64)
		}
	case TypeBytes:
		if !bytes.Equal(got.Bytes, want.Bytes) {
			t.Fatalf("value Bytes = %v, want %v", got.Bytes, want.Bytes)
		}
	default:
		t.Fatalf("unexpected value type %d", want.Type)
	}
}
