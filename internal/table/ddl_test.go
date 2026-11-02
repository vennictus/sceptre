package table

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
)

func seedPosts(t *testing.T, db *DB) {
	t.Helper()
	if err := db.CreateTable(postsDef); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("posts", IndexDef{Name: "posts_views", Columns: []string{"views"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertMany("posts", []Record{
		post(1, "a", TextValue("s1"), Int64Value(1)),
		post(2, "b", NullValue(), Int64Value(2)),
		post(3, "c", TextValue("s3"), NullValue()),
	}); err != nil {
		t.Fatal(err)
	}
}

func countKeys(t *testing.T, db *DB) int {
	t.Helper()
	iter := db.store.ScanIterator()
	defer iter.Close()
	n := 0
	for err := iter.SeekFirst(); iter.Valid(); err = iter.Next() {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	return n
}

func TestDropTableAndIndexRemoveTheirData(t *testing.T) {
	t.Parallel()
	db := openNullDB(t)
	before := countKeys(t, db)
	seedPosts(t, db)
	if err := db.DropIndex("posts", "posts_views"); err != nil {
		t.Fatal(err)
	}
	if def, _, _ := db.Table("posts"); len(def.Indexes) != 0 {
		t.Fatalf("index still defined: %+v", def.Indexes)
	}
	mustCheckClean(t, db)
	if err := db.DropIndex("posts", "posts_views"); !errors.Is(err, ErrIndexNotFound) {
		t.Fatalf("second DropIndex: %v", err)
	}
	if err := db.DropTable("posts"); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := db.Table("posts"); ok {
		t.Fatal("table still exists")
	}
	// Only the catalog's next-prefix counter remains.
	if after := countKeys(t, db); after != before+1 {
		t.Fatalf("keys after drop = %d, want %d", after, before+1)
	}
	mustCheckClean(t, db)
	// The name can be reused; the new table starts empty.
	seedPosts(t, db)
	mustCheckClean(t, db)
}

func TestRenameTableAndColumn(t *testing.T) {
	t.Parallel()
	db := openNullDB(t)
	seedPosts(t, db)
	if err := db.RenameColumn("posts", "views", "hits"); err != nil {
		t.Fatal(err)
	}
	if err := db.RenameColumn("posts", "id", "post_id"); err != nil {
		t.Fatal(err)
	}
	if err := db.RenameTable("posts", "articles"); err != nil {
		t.Fatal(err)
	}
	def, ok, err := db.Table("articles")
	if err != nil || !ok || def.PrimaryKey[0] != "post_id" || def.Indexes[0].Columns[0] != "hits" {
		t.Fatalf("renamed def = %+v %v %v", def, ok, err)
	}
	rows, err := db.LookupIndex("articles", "posts_views", NewRecord(map[string]Value{"hits": Int64Value(2)}))
	if err != nil || len(rows) != 1 || rows[0].Values["post_id"].I64 != 2 {
		t.Fatalf("lookup after renames = %+v %v", rows, err)
	}
	if err := db.RenameColumn("articles", "hits", "title"); !errors.Is(err, ErrInvalidTableDef) {
		t.Fatalf("rename onto an existing column: %v", err)
	}
	if err := db.CreateTable(postsDef); err != nil {
		t.Fatal(err)
	}
	if err := db.RenameTable("articles", "posts"); !errors.Is(err, ErrTableExists) {
		t.Fatalf("rename onto an existing table: %v", err)
	}
	mustCheckClean(t, db)
}

func TestAddAndDropColumn(t *testing.T) {
	t.Parallel()
	db := openNullDB(t)
	seedPosts(t, db)
	if err := db.AddColumn("posts", Column{Name: "tags", Type: TypeText, Nullable: true}); err != nil {
		t.Fatal(err)
	}
	row, _, err := db.Get("posts", NewRecord(map[string]Value{"id": Int64Value(1)}))
	if err != nil || !row.Values["tags"].IsNull() {
		t.Fatalf("added column on an old row = %+v %v", row, err)
	}
	if err := db.Update("posts", NewRecord(map[string]Value{"id": Int64Value(1), "title": TextValue("a"), "summary": NullValue(), "views": Int64Value(1), "tags": TextValue("go")})); err != nil {
		t.Fatal(err)
	}
	if err := db.AddColumn("posts", Column{Name: "x", Type: TypeInt64}); !errors.Is(err, ErrInvalidTableDef) {
		t.Fatalf("adding a NOT NULL column: %v", err)
	}
	if err := db.DropColumn("posts", "views"); !errors.Is(err, ErrInvalidTableDef) {
		t.Fatalf("dropping an indexed column: %v", err)
	}
	if err := db.DropColumn("posts", "id"); !errors.Is(err, ErrInvalidTableDef) {
		t.Fatalf("dropping the primary key: %v", err)
	}
	if err := db.DropColumn("posts", "summary"); err != nil {
		t.Fatal(err)
	}
	row, _, err = db.Get("posts", NewRecord(map[string]Value{"id": Int64Value(1)}))
	if err != nil || len(row.Values) != 4 || string(row.Values["tags"].Bytes) != "go" || row.Values["views"].I64 != 1 {
		t.Fatalf("row after DropColumn = %+v %v", row, err)
	}
	if rows, err := db.LookupIndex("posts", "posts_views", NewRecord(map[string]Value{"views": Int64Value(2)})); err != nil || len(rows) != 1 {
		t.Fatalf("index after DropColumn = %v %v", rows, err)
	}
	mustCheckClean(t, db)
}

// Adding a column to a legacy-format table rewrites it, indexes included.
func TestAddColumnConvertsLegacyTables(t *testing.T) {
	t.Parallel()
	db := openNullDB(t)
	def := TableDef{Name: "old", Columns: []Column{{Name: "id", Type: TypeInt64}, {Name: "body", Type: TypeBytes}}, PrimaryKey: []string{"id"}}
	if err := db.CreateTable(def); err != nil {
		t.Fatal(err)
	}
	stored, _, _ := db.Table("old")
	stored.RowFormat = 0
	encoded, _ := json.Marshal(stored)
	if err := db.store.Apply([]kv.Mutation{kv.Put(catalogTableKey("old"), encoded)}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateIndex("old", IndexDef{Name: "old_body", Columns: []string{"body"}}); err != nil {
		t.Fatal(err)
	}
	for i := int64(1); i <= 3; i++ {
		if err := db.Insert("old", NewRecord(map[string]Value{"id": Int64Value(i), "body": BytesValue([]byte{byte('a' + i)})})); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.AddColumn("old", Column{Name: "note", Type: TypeText, Nullable: true}); err != nil {
		t.Fatal(err)
	}
	got, _, _ := db.Table("old")
	if got.RowFormat != RowFormatTagged {
		t.Fatalf("row format after AddColumn = %d", got.RowFormat)
	}
	if rows, err := db.LookupIndex("old", "old_body", NewRecord(map[string]Value{"body": BytesValue([]byte{'c'})})); err != nil || len(rows) != 1 || !rows[0].Values["note"].IsNull() {
		t.Fatalf("lookup after conversion = %+v %v", rows, err)
	}
	mustCheckClean(t, db)
}
