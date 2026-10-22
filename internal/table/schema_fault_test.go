package table

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/pager"
)

func TestCatalogAndIndexBackfillRemainAtomicOnPhysicalIOFailures(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"create_table", "create_index"} {
		for _, fault := range []string{"data_short", "data_sync", "meta_short", "meta_sync"} {
			t.Run(operation+"/"+fault, func(t *testing.T) {
				armed, fired, syncs := false, false, 0
				hooks := &pager.IOHooks{
					WriteAt: func(f *os.File, data []byte, offset int64) (int, error) {
						meta := offset < 2*4096
						if armed && !fired && ((fault == "data_short" && !meta) || (fault == "meta_short" && meta)) {
							fired = true
							return f.WriteAt(data[:32], offset)
						}
						return f.WriteAt(data, offset)
					},
					Sync: func(f *os.File) error {
						if armed {
							syncs++
						}
						if armed && !fired && ((fault == "data_sync" && syncs == 1) || (fault == "meta_sync" && syncs == 2)) {
							fired = true
							return errors.New("injected catalog sync failure")
						}
						return f.Sync()
					},
				}
				path := filepath.Join(t.TempDir(), "db")
				store, err := kv.Open(path, kv.Options{IO: hooks})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { store.Close() })
				db := New(store)
				def := TableDef{Name: "t", Columns: []Column{{Name: "id", Type: TypeInt64}, {Name: "body", Type: TypeBytes}}, PrimaryKey: []string{"id"}}
				if err := db.CreateTable(def); err != nil {
					t.Fatal(err)
				}
				for i := int64(1); i <= 3; i++ {
					if err := db.Insert("t", NewRecord(map[string]Value{"id": Int64Value(i), "body": BytesValue([]byte("same"))})); err != nil {
						t.Fatal(err)
					}
				}
				generation := store.Pager().Meta().Generation
				apply := func() error {
					if operation == "create_index" {
						return db.CreateIndex("t", IndexDef{Name: "body_idx", Columns: []string{"body"}})
					}
					extra := def
					extra.Name = "extra"
					return db.CreateTable(extra)
				}
				armed = true
				err = apply()
				if err == nil || !fired {
					t.Fatalf("fault not exercised: %v", err)
				}
				if (fault == "meta_short" || fault == "meta_sync") && !errors.Is(err, kv.ErrRecoveryRequired) {
					t.Fatalf("uncertain schema outcome not surfaced: %v", err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				store, err = kv.Open(path, kv.Options{})
				if err != nil {
					t.Fatal(err)
				}
				db = New(store)
				committed := fault == "meta_sync"
				wantGeneration := generation
				if committed {
					wantGeneration++
				}
				if store.Pager().Meta().Generation != wantGeneration {
					t.Fatal("unexpected generation after recovery")
				}
				var exists bool
				if operation == "create_index" {
					_, exists, err = db.Index("t", "body_idx")
				} else {
					_, exists, err = db.Table("extra")
				}
				if err != nil || exists != committed {
					t.Fatalf("partial schema state: exists=%v want=%v error=%v", exists, committed, err)
				}
				if !committed {
					if err := apply(); err != nil {
						t.Fatal(err)
					}
				}
				if operation == "create_index" {
					rows, err := db.LookupIndex("t", "body_idx", NewRecord(map[string]Value{"body": BytesValue([]byte("same"))}))
					if err != nil || len(rows) != 3 {
						t.Fatalf("partial backfill: rows=%d err=%v", len(rows), err)
					}
				}
				if err := db.Insert("t", NewRecord(map[string]Value{"id": Int64Value(4), "body": BytesValue([]byte("after-recovery"))})); err != nil {
					t.Fatal(err)
				}
				report, err := db.Check()
				if err != nil || !report.OK() {
					t.Fatalf("invariants after recovery: %+v %v", report, err)
				}
			})
		}
	}
}
