package table

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/tx"
)

func TestTransactionalCatalogPrefixesAndIndexVisibility(t *testing.T) {
	t.Parallel()
	store, err := kv.Open(filepath.Join(t.TempDir(), "db"), kv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, commit := range []bool{false, true} {
		transaction := tx.Begin(store)
		db := NewTransactional(transaction)
		for _, name := range []string{"one", "two"} {
			def := TableDef{Name: name, Columns: []Column{{Name: "id", Type: TypeInt64}, {Name: "body", Type: TypeBytes}}, PrimaryKey: []string{"id"}}
			if err := db.CreateTable(def); err != nil {
				t.Fatal(err)
			}
			if err := db.Insert(name, NewRecord(map[string]Value{"id": Int64Value(1), "body": BytesValue([]byte(name))})); err != nil {
				t.Fatal(err)
			}
			if err := db.CreateIndex(name, IndexDef{Name: "body_idx", Columns: []string{"body"}}); err != nil {
				t.Fatal(err)
			}
			rows, err := db.LookupIndex(name, "body_idx", NewRecord(map[string]Value{"body": BytesValue([]byte(name))}))
			if err != nil || len(rows) != 1 {
				t.Fatal(rows, err)
			}
		}
		defs, err := db.Tables()
		if err != nil || len(defs) != 2 {
			t.Fatal(defs, err)
		}
		prefixes := map[uint32]bool{}
		for _, def := range defs {
			for _, prefix := range []uint32{def.Prefix, def.Indexes[0].Prefix} {
				if prefixes[prefix] {
					t.Fatal("prefix reused", prefix)
				}
				prefixes[prefix] = true
			}
		}
		committed, err := New(store).Tables()
		if err != nil || len(committed) != 0 {
			t.Fatal("pending catalog leaked", committed, err)
		}
		if commit {
			if err := transaction.Commit(); err != nil {
				t.Fatal(err)
			}
		} else {
			transaction.Abort()
		}
	}
	check, err := New(store).Check()
	if err != nil || !check.OK() || len(check.Tables) != 2 {
		t.Fatal(check, err)
	}
}

func TestNewNilStorePreservesUnavailableBehavior(t *testing.T) {
	_, _, err := New(nil).Table("missing")
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatal(err)
	}
}
