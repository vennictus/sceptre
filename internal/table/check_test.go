package table

import (
	"encoding/binary"
	"path/filepath"
	"testing"
)

func TestCheckReportsHealthyDatabase(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)
	mustInsertUser(t, db, 1, "Ada", 31)
	mustInsertUser(t, db, 2, "Grace", 40)
	if err := db.CreateIndex("users", IndexDef{Name: "users_age", Columns: []string{"age"}}); err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}

	report, err := db.Check()
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if !report.OK() {
		t.Fatalf("Check() issues = %+v, want none", report.Issues)
	}
	if len(report.Tables) != 1 {
		t.Fatalf("table count = %d, want 1", len(report.Tables))
	}
	if got := report.Tables[0]; got.Name != "users" || got.Rows != 2 || got.Indexes != 1 {
		t.Fatalf("table summary = %+v, want users rows=2 indexes=1", got)
	}
}

func TestCheckReportsMissingIndexEntry(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)
	mustInsertUser(t, db, 1, "Ada", 31)
	if err := db.CreateIndex("users", IndexDef{Name: "users_age", Columns: []string{"age"}}); err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}

	def, err := db.mustTable("users")
	if err != nil {
		t.Fatalf("mustTable() error = %v", err)
	}
	key, err := encodeIndexKey(def, def.Indexes[0], NewRecord(map[string]Value{
		"id":   Int64Value(1),
		"name": BytesValue([]byte("Ada")),
		"age":  Int64Value(31),
	}))
	if err != nil {
		t.Fatalf("encodeIndexKey() error = %v", err)
	}
	if _, err := db.kv.Del(key); err != nil {
		t.Fatalf("kv.Del(index key) error = %v", err)
	}

	report, err := db.Check()
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if report.OK() {
		t.Fatal("Check().OK() = true, want false")
	}
	if !hasIssue(report, "missing_index_entry") {
		t.Fatalf("Check() issues = %+v, want missing_index_entry", report.Issues)
	}
}

func TestCheckReportsOrphanIndexEntry(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)
	mustInsertUser(t, db, 1, "Ada", 31)
	if err := db.CreateIndex("users", IndexDef{Name: "users_age", Columns: []string{"age"}}); err != nil {
		t.Fatalf("CreateIndex() error = %v", err)
	}

	def, err := db.mustTable("users")
	if err != nil {
		t.Fatalf("mustTable() error = %v", err)
	}
	key, err := encodeIndexKey(def, def.Indexes[0], NewRecord(map[string]Value{
		"id":   Int64Value(99),
		"name": BytesValue([]byte("Missing")),
		"age":  Int64Value(31),
	}))
	if err != nil {
		t.Fatalf("encodeIndexKey() error = %v", err)
	}
	if err := db.kv.Set(key, nil); err != nil {
		t.Fatalf("kv.Set(orphan index key) error = %v", err)
	}

	report, err := db.Check()
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if report.OK() {
		t.Fatal("Check().OK() = true, want false")
	}
	if !hasIssue(report, "unexpected_index_entry") {
		t.Fatalf("Check() issues = %+v, want unexpected_index_entry", report.Issues)
	}
	if !hasIssue(report, "orphan_index_entry") {
		t.Fatalf("Check() issues = %+v, want orphan_index_entry", report.Issues)
	}
}

func TestCheckReportsUnknownRowPrefix(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)

	key := []byte{rowKeyPrefix, 0, 0, 0, 99}
	key = append(key, []byte("bad")...)
	if err := db.kv.Set(key, []byte("value")); err != nil {
		t.Fatalf("kv.Set(unknown row prefix) error = %v", err)
	}

	report, err := db.Check()
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if !hasIssue(report, "unknown_row_prefix") {
		t.Fatalf("Check() issues = %+v, want unknown_row_prefix", report.Issues)
	}
}

func TestCheckReportsDuplicateTablePrefix(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)
	if err := db.CreateTable(TableDef{
		Name: "accounts",
		Columns: []Column{
			{Name: "id", Type: TypeInt64},
			{Name: "name", Type: TypeBytes},
		},
		PrimaryKey: []string{"id"},
	}); err != nil {
		t.Fatalf("CreateTable(accounts) error = %v", err)
	}

	accounts, ok, err := db.Table("accounts")
	if err != nil {
		t.Fatalf("Table(accounts) error = %v", err)
	}
	if !ok {
		t.Fatal("Table(accounts) ok = false, want true")
	}
	accounts.Prefix = 1
	encoded, err := encodeTableDef(accounts)
	if err != nil {
		t.Fatalf("encodeTableDef() error = %v", err)
	}
	if err := db.kv.Set(catalogTableKey("accounts"), encoded); err != nil {
		t.Fatalf("kv.Set(accounts schema) error = %v", err)
	}

	report, err := db.Check()
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if !hasIssue(report, "duplicate_table_prefix") {
		t.Fatalf("Check() issues = %+v, want duplicate_table_prefix", report.Issues)
	}
}

func TestCheckReportsFreelistPageBounds(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)

	meta := db.kv.Pager().Meta()
	meta.FreeListPage = meta.PageCount + 100
	if err := db.kv.Pager().PublishMeta(meta); err != nil {
		t.Fatalf("PublishMeta() error = %v", err)
	}

	report, err := db.Check()
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if !hasIssue(report, "freelist_head_out_of_range") && !hasIssue(report, "freelist_invalid") {
		t.Fatalf("Check() issues = %+v, want freelist bounds issue", report.Issues)
	}
}

func TestCheckReportsInvalidCatalogMeta(t *testing.T) {
	t.Parallel()

	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "sceptre.db"))
	defer db.Close()
	mustCreateUsers(t, db)

	var bad [4]byte
	binary.BigEndian.PutUint32(bad[:], 0)
	if err := db.kv.Set(catalogMetaKey, bad[:]); err != nil {
		t.Fatalf("kv.Set(catalog meta) error = %v", err)
	}

	report, err := db.Check()
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if !hasIssue(report, "catalog_meta_invalid") {
		t.Fatalf("Check() issues = %+v, want catalog_meta_invalid", report.Issues)
	}
}

func hasIssue(report CheckReport, code string) bool {
	for _, issue := range report.Issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}
