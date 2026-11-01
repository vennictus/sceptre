package table

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
)

func openNullDB(t *testing.T) *DB {
	t.Helper()
	store, err := kv.Open(filepath.Join(t.TempDir(), "db"), kv.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return New(store)
}

func mustCheckClean(t *testing.T, db *DB) {
	t.Helper()
	rep, err := db.Check()
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK() {
		t.Fatalf("check issues: %+v", rep.Issues)
	}
}

var postsDef = TableDef{
	Name: "posts",
	Columns: []Column{
		{Name: "id", Type: TypeInt64},
		{Name: "title", Type: TypeText},
		{Name: "summary", Type: TypeText, Nullable: true},
		{Name: "views", Type: TypeInt64, Nullable: true},
	},
	PrimaryKey: []string{"id"},
}

func post(id int64, title string, summary, views Value) Record {
	return NewRecord(map[string]Value{"id": Int64Value(id), "title": TextValue(title), "summary": summary, "views": views})
}

func TestNullValuesRoundTripAndIndex(t *testing.T) {
	t.Parallel()
	db := openNullDB(t)
	if err := db.CreateTable(postsDef); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("posts", IndexDef{Name: "posts_views", Columns: []string{"views"}}); err != nil {
		t.Fatal(err)
	}
	rows := []Record{
		post(1, "a", NullValue(), NullValue()),
		post(2, "b", TextValue("s"), Int64Value(5)),
		post(3, "c", NullValue(), Int64Value(5)),
		post(4, "d", TextValue(""), NullValue()),
	}
	if err := db.InsertMany("posts", rows); err != nil {
		t.Fatal(err)
	}
	for _, want := range rows {
		got, ok, err := db.Get("posts", NewRecord(map[string]Value{"id": want.Values["id"]}))
		if err != nil || !ok {
			t.Fatalf("Get: %v %v", ok, err)
		}
		for name, v := range want.Values {
			g := got.Values[name]
			if g.Type != v.Type || string(g.Bytes) != string(v.Bytes) || g.I64 != v.I64 {
				t.Fatalf("row %d column %s = %+v, want %+v", want.Values["id"].I64, name, g, v)
			}
		}
	}
	// An empty string is not NULL.
	if got, _, _ := db.Get("posts", NewRecord(map[string]Value{"id": Int64Value(4)})); got.Values["summary"].IsNull() {
		t.Fatal("empty text read back as NULL")
	}
	found, err := db.LookupIndex("posts", "posts_views", NewRecord(map[string]Value{"views": Int64Value(5)}))
	if err != nil || len(found) != 2 {
		t.Fatalf("index lookup = %d rows, %v", len(found), err)
	}
	// Updating NULL to a value and back keeps the index exact.
	if err := db.Update("posts", post(1, "a", NullValue(), Int64Value(5))); err != nil {
		t.Fatal(err)
	}
	if err := db.Update("posts", post(2, "b", TextValue("s"), NullValue())); err != nil {
		t.Fatal(err)
	}
	found, _ = db.LookupIndex("posts", "posts_views", NewRecord(map[string]Value{"views": Int64Value(5)}))
	if len(found) != 2 {
		t.Fatalf("index lookup after updates = %d rows", len(found))
	}
	mustCheckClean(t, db)
}

func TestNullRejectedWhereNotAllowed(t *testing.T) {
	t.Parallel()
	db := openNullDB(t)
	if err := db.CreateTable(postsDef); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("posts", NewRecord(map[string]Value{"id": Int64Value(1), "title": NullValue(), "summary": NullValue(), "views": NullValue()})); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("NULL in a NOT NULL column: %v", err)
	}
	if err := db.Insert("posts", NewRecord(map[string]Value{"id": NullValue(), "title": TextValue("t"), "summary": NullValue(), "views": NullValue()})); err == nil {
		t.Fatal("NULL primary key accepted")
	}
	bad := TableDef{Name: "bad", Columns: []Column{{Name: "id", Type: TypeInt64, Nullable: true}}, PrimaryKey: []string{"id"}}
	if err := db.CreateTable(bad); !errors.Is(err, ErrInvalidTableDef) {
		t.Fatalf("nullable primary key column: %v", err)
	}
}

// Tables written before the tagged row format keep their encoding.
func TestLegacyRowFormatStillWorks(t *testing.T) {
	t.Parallel()
	db := openNullDB(t)
	def := TableDef{Name: "old", Columns: []Column{{Name: "id", Type: TypeInt64}, {Name: "body", Type: TypeBytes}}, PrimaryKey: []string{"id"}}
	if err := db.CreateTable(def); err != nil {
		t.Fatal(err)
	}
	// Rewrite the catalog entry as format 0, as an older binary wrote it.
	stored, _, err := db.Table("old")
	if err != nil {
		t.Fatal(err)
	}
	stored.RowFormat = 0
	encoded, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.store.Apply([]kv.Mutation{kv.Put(catalogTableKey("old"), encoded)}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("old", IndexDef{Name: "old_body", Columns: []string{"body"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("old", NewRecord(map[string]Value{"id": Int64Value(1), "body": BytesValue([]byte("x"))})); err != nil {
		t.Fatal(err)
	}
	raw, ok, err := db.store.Get(append(encodedRowKeyFor(t, db, "old"), 0x80, 0, 0, 0, 0, 0, 0, 1))
	if err != nil || !ok || string(raw) != "x\x00" {
		t.Fatalf("legacy row bytes = %q %v %v, want untagged", raw, ok, err)
	}
	if rows, err := db.LookupIndex("old", "old_body", NewRecord(map[string]Value{"body": BytesValue([]byte("x"))})); err != nil || len(rows) != 1 {
		t.Fatalf("legacy index lookup = %v %v", rows, err)
	}
	if err := db.Insert("old", NewRecord(map[string]Value{"id": Int64Value(2), "body": NullValue()})); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("NULL in a legacy table: %v", err)
	}
	// A new table writes tagged rows.
	def.Name = "new"
	if err := db.CreateTable(def); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("new", NewRecord(map[string]Value{"id": Int64Value(1), "body": BytesValue([]byte("x"))})); err != nil {
		t.Fatal(err)
	}
	raw, ok, err = db.store.Get(append(encodedRowKeyFor(t, db, "new"), 0x80, 0, 0, 0, 0, 0, 0, 1))
	if err != nil || !ok || string(raw) != "\x01x\x00" {
		t.Fatalf("new row bytes = %q %v %v, want tagged", raw, ok, err)
	}
	mustCheckClean(t, db)
}

func encodedRowKeyFor(t *testing.T, db *DB, name string) []byte {
	t.Helper()
	def, _, err := db.Table(name)
	if err != nil {
		t.Fatal(err)
	}
	return []byte{rowKeyPrefix, byte(def.Prefix >> 24), byte(def.Prefix >> 16), byte(def.Prefix >> 8), byte(def.Prefix)}
}

func TestMissingTrailingNullableColumnsReadAsNull(t *testing.T) {
	t.Parallel()
	db := openNullDB(t)
	def := TableDef{Name: "t", Columns: []Column{{Name: "id", Type: TypeInt64}, {Name: "a", Type: TypeInt64}}, PrimaryKey: []string{"id"}}
	if err := db.CreateTable(def); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert("t", NewRecord(map[string]Value{"id": Int64Value(1), "a": Int64Value(7)})); err != nil {
		t.Fatal(err)
	}
	// Add a nullable column to the catalog without rewriting the row.
	stored, _, _ := db.Table("t")
	stored.Columns = append(stored.Columns, Column{Name: "b", Type: TypeText, Nullable: true})
	encoded, _ := json.Marshal(stored)
	if err := db.store.Apply([]kv.Mutation{kv.Put(catalogTableKey("t"), encoded)}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := db.Get("t", NewRecord(map[string]Value{"id": Int64Value(1)}))
	if err != nil || !ok || !got.Values["b"].IsNull() || got.Values["a"].I64 != 7 {
		t.Fatalf("row after adding a column = %+v %v %v", got, ok, err)
	}
	mustCheckClean(t, db)
}
