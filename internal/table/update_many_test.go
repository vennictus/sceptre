package table

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
)

func TestUpdateManyStagesRowsAndIndexesBeforeCommitting(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		second Record
		want   error
	}{
		{"missing", NewRecord(map[string]Value{"id": Int64Value(99), "body": BytesValue([]byte("new"))}), ErrRowNotFound},
		{"invalid_type", NewRecord(map[string]Value{"id": Int64Value(2), "body": Int64Value(7)}), ErrInvalidValue},
		{"duplicate_key", NewRecord(map[string]Value{"id": Int64Value(1), "body": BytesValue([]byte("other"))}), ErrInvalidRecord},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := kv.Open(filepath.Join(t.TempDir(), "db"), kv.Options{PageSize: 512})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			db := New(store)
			if err := db.CreateTable(TableDef{Name: "t", Columns: []Column{{Name: "id", Type: TypeInt64}, {Name: "body", Type: TypeBytes}}, PrimaryKey: []string{"id"}}); err != nil {
				t.Fatal(err)
			}
			if err := db.CreateIndex("t", IndexDef{Name: "body_idx", Columns: []string{"body"}}); err != nil {
				t.Fatal(err)
			}
			old := NewRecord(map[string]Value{"id": Int64Value(1), "body": BytesValue([]byte("old"))})
			if err := db.Insert("t", old); err != nil {
				t.Fatal(err)
			}
			if err := db.Insert("t", NewRecord(map[string]Value{"id": Int64Value(2), "body": BytesValue([]byte("old-2"))})); err != nil {
				t.Fatal(err)
			}
			generation := store.Pager().Meta().Generation
			err = db.UpdateMany("t", []Record{NewRecord(map[string]Value{"id": Int64Value(1), "body": BytesValue([]byte("new"))}), test.second})
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
			if store.Pager().Meta().Generation != generation {
				t.Fatal("failed staging published a commit")
			}
			row, ok, err := db.Get("t", NewRecord(map[string]Value{"id": Int64Value(1)}))
			if err != nil || !ok || string(row.Values["body"].Bytes) != "old" {
				t.Fatalf("first row changed: %+v error=%v", row, err)
			}
			matches, err := db.LookupIndex("t", "body_idx", NewRecord(map[string]Value{"body": BytesValue([]byte("old"))}))
			if err != nil || len(matches) != 1 {
				t.Fatalf("old index changed: %+v error=%v", matches, err)
			}
			report, err := db.Check()
			if err != nil || !report.OK() {
				t.Fatalf("integrity: %+v error=%v", report, err)
			}
		})
	}
}
