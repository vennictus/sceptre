package table

import (
	"errors"
	"path/filepath"
	"testing"
)

// Names are stored in the JSON catalog, which would replace bytes that are
// not UTF-8 and lose the name; every way to set one rejects such names.
func TestCatalogNamesMustSurviveJSON(t *testing.T) {
	t.Parallel()
	db := mustOpenTableDB(t, filepath.Join(t.TempDir(), "db"))
	defer db.Close()
	if err := db.CreateTable(usersTableDef()); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "a\xffb", "tab\there", "nul\x00"} {
		def := usersTableDef()
		def.Name = bad
		if err := db.CreateTable(def); !errors.Is(err, ErrInvalidTableDef) {
			t.Errorf("CreateTable(%q): %v", bad, err)
		}
		def = usersTableDef()
		def.Name = "other"
		def.Columns[1].Name = bad
		if err := db.CreateTable(def); !errors.Is(err, ErrInvalidTableDef) {
			t.Errorf("CreateTable with column %q: %v", bad, err)
		}
		if err := db.RenameTable("users", bad); !errors.Is(err, ErrInvalidTableDef) {
			t.Errorf("RenameTable to %q: %v", bad, err)
		}
		if err := db.RenameColumn("users", "id", bad); !errors.Is(err, ErrInvalidTableDef) {
			t.Errorf("RenameColumn to %q: %v", bad, err)
		}
		if err := db.CreateIndex("users", IndexDef{Name: bad, Columns: []string{"id"}}); !errors.Is(err, ErrInvalidIndexDef) {
			t.Errorf("CreateIndex(%q): %v", bad, err)
		}
	}
	def := usersTableDef()
	def.Name = "日本 table"
	if err := db.CreateTable(def); err != nil {
		t.Fatalf("a UTF-8 name with a space: %v", err)
	}
}
