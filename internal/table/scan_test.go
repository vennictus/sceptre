package table

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func TestScanRowsInPrimaryKeyOrder(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)

	for _, id := range []int64{3, 1, 2} {
		mustInsertUser(t, db, id, "user", 20+id)
	}

	scanner, err := db.Scan("users", ScanBounds{})
	if err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	got := collectScanInt64s(t, scanner, "id")
	want := []int64{1, 2, 3}
	assertInt64s(t, got, want)
}

func TestScanHonorsInclusiveAndExclusiveBounds(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)

	for id := int64(1); id <= 5; id++ {
		mustInsertUser(t, db, id, "user", 20+id)
	}

	scanner, err := db.Scan("users", ScanBounds{
		Lower: Inclusive(NewRecord(map[string]Value{"id": Int64Value(2)})),
		Upper: Exclusive(NewRecord(map[string]Value{"id": Int64Value(5)})),
	})
	if err != nil {
		t.Fatalf("Scan() inclusive/exclusive error = %v", err)
	}
	assertInt64s(t, collectScanInt64s(t, scanner, "id"), []int64{2, 3, 4})

	scanner, err = db.Scan("users", ScanBounds{
		Lower: Exclusive(NewRecord(map[string]Value{"id": Int64Value(2)})),
		Upper: Inclusive(NewRecord(map[string]Value{"id": Int64Value(4)})),
	})
	if err != nil {
		t.Fatalf("Scan() exclusive/inclusive error = %v", err)
	}
	assertInt64s(t, collectScanInt64s(t, scanner, "id"), []int64{3, 4})
}

func TestScanDoesNotCrossTablePrefixes(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)
	if err := db.CreateTable(TableDef{
		Name: "accounts",
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "balance", Type: TypeInt64},
		},
		PrimaryKey: []string{"id"},
	}); err != nil {
		t.Fatalf("CreateTable(accounts) error = %v", err)
	}

	mustInsertUser(t, db, 1, "Ada", 32)
	if err := db.Insert("accounts", NewRecord(map[string]Value{
		"id":      Int64Value(1),
		"balance": Int64Value(100),
	})); err != nil {
		t.Fatalf("Insert(accounts) error = %v", err)
	}

	scanner, err := db.Scan("users", ScanBounds{})
	if err != nil {
		t.Fatalf("Scan(users) error = %v", err)
	}
	assertInt64s(t, collectScanInt64s(t, scanner, "id"), []int64{1})
}

func TestCompositePrimaryKeyScanOrder(t *testing.T) {
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
		t.Fatalf("CreateTable(accounts) error = %v", err)
	}

	for _, row := range []Record{
		NewRecord(map[string]Value{"tenant": BytesValue([]byte("b")), "id": Int64Value(1), "balance": Int64Value(10)}),
		NewRecord(map[string]Value{"tenant": BytesValue([]byte("a")), "id": Int64Value(2), "balance": Int64Value(20)}),
		NewRecord(map[string]Value{"tenant": BytesValue([]byte("a")), "id": Int64Value(1), "balance": Int64Value(30)}),
	} {
		if err := db.Insert("accounts", row); err != nil {
			t.Fatalf("Insert(accounts) error = %v", err)
		}
	}

	scanner, err := db.Scan("accounts", ScanBounds{})
	if err != nil {
		t.Fatalf("Scan(accounts) error = %v", err)
	}

	var got []string
	for scanner.Valid() {
		row, err := scanner.Deref()
		if err != nil {
			t.Fatalf("Deref() error = %v", err)
		}
		got = append(got, fmt.Sprintf("%s:%d", row.Values["tenant"].Bytes, row.Values["id"].I64))
		if err := scanner.Next(); err != nil {
			t.Fatalf("Next() error = %v", err)
		}
	}
	want := []string{"a:1", "a:2", "b:1"}
	if len(got) != len(want) {
		t.Fatalf("scan count = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scan[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestEncodedValuesPreserveLogicalOrder(t *testing.T) {
	t.Parallel()

	assertEncodedLess(t, Int64Value(-2), Int64Value(-1))
	assertEncodedLess(t, Int64Value(-1), Int64Value(0))
	assertEncodedLess(t, Int64Value(0), Int64Value(1))
	assertEncodedLess(t, BytesValue([]byte("a")), BytesValue([]byte("aa")))
	assertEncodedLess(t, BytesValue([]byte("aa")), BytesValue([]byte("b")))
	assertEncodedLess(t, BytesValue([]byte{0x00}), BytesValue([]byte{0x01}))
}

func mustCreateUsers(t *testing.T, db *DB) {
	t.Helper()

	if err := db.CreateTable(usersTableDef()); err != nil {
		t.Fatalf("CreateTable(users) error = %v", err)
	}
}

func mustInsertUser(t *testing.T, db *DB, id int64, name string, age int64) {
	t.Helper()

	if err := db.Insert("users", NewRecord(map[string]Value{
		"id":   Int64Value(id),
		"name": BytesValue([]byte(name)),
		"age":  Int64Value(age),
	})); err != nil {
		t.Fatalf("Insert(users id=%d) error = %v", id, err)
	}
}

func collectScanInt64s(t *testing.T, scanner *Scanner, column string) []int64 {
	t.Helper()

	var out []int64
	for scanner.Valid() {
		row, err := scanner.Deref()
		if err != nil {
			t.Fatalf("Deref() error = %v", err)
		}
		out = append(out, row.Values[column].I64)
		if err := scanner.Next(); err != nil {
			t.Fatalf("Next() error = %v", err)
		}
	}
	return out
}

func assertInt64s(t *testing.T, got, want []int64) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("length = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("value[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func assertEncodedLess(t *testing.T, left, right Value) {
	t.Helper()

	leftEncoded := appendEncodedValue(nil, left)
	rightEncoded := appendEncodedValue(nil, right)
	if bytes.Compare(leftEncoded, rightEncoded) >= 0 {
		t.Fatalf("encoded %v >= %v", leftEncoded, rightEncoded)
	}
}
