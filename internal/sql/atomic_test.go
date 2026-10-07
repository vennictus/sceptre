package sql

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/table"
)

func TestSQLUpdateFailurePreservesAllRowsAndIndexesAcrossReopen(t *testing.T) {
	t.Parallel()
	for _, indexed := range []bool{false, true} {
		t.Run(fmt.Sprintf("indexed=%v", indexed), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			db, err := table.Open(path, table.Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			queries := []string{"create table t (id bytes, body bytes, primary key (id))"}
			if indexed {
				queries = append(queries, "create index body_idx on t (body)")
			}
			queries = append(queries, "insert into t (id,body) values ('a','old-a')", fmt.Sprintf("insert into t (id,body) values ('%s','old-b')", strings.Repeat("z", 1800)))
			for _, q := range queries {
				if _, err := Execute(db, q); err != nil {
					t.Fatal(err)
				}
			}
			before, err := Execute(db, "select * from t")
			if err != nil {
				t.Fatal(err)
			}
			if r, err := Execute(db, fmt.Sprintf("update t set body = '%s'", strings.Repeat("x", 2800))); err == nil || r.RowsAffected != 0 {
				t.Fatalf("expected oversized-record failure: %+v error=%v", r, err)
			}
			assertSQLState(t, db, "t", before)
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = table.Open(path, table.Options{})
			if err != nil {
				t.Fatal(err)
			}
			assertSQLState(t, db, "t", before)
			if indexed {
				for _, body := range []string{"old-a", "old-b"} {
					rows, err := db.LookupIndex("t", "body_idx", table.NewRecord(map[string]table.Value{"body": table.BytesValue([]byte(body))}))
					if err != nil || len(rows) != 1 {
						t.Fatalf("old index %s: rows=%v error=%v", body, rows, err)
					}
				}
			}
		})
	}
}

func TestSQLMultiRowMutationsPublishOneGeneration(t *testing.T) {
	t.Parallel()
	store, err := kv.Open(filepath.Join(t.TempDir(), "db"), kv.Options{PageSize: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	db := table.New(store)
	for _, q := range []string{"create table t (id int64, body bytes, primary key (id))", "create index body_idx on t (body)", "insert into t (id,body) values (1,'a')", "insert into t (id,body) values (2,'b')", "insert into t (id,body) values (3,'c')"} {
		if _, err := Execute(db, q); err != nil {
			t.Fatal(err)
		}
	}
	for _, q := range []string{"update t set body = 'new'", "delete from t"} {
		generation := store.Pager().Meta().Generation
		r, err := Execute(db, q)
		if err != nil || r.RowsAffected != 3 {
			t.Fatalf("%s: result=%+v error=%v", q, r, err)
		}
		if got := store.Pager().Meta().Generation; got != generation+1 {
			t.Fatalf("%s published %d generations, want one", q, got-generation)
		}
		report, err := db.Check()
		if err != nil || !report.OK() {
			t.Fatalf("integrity after %s: %+v error=%v", q, report, err)
		}
	}
	for _, q := range []string{"update t set body = 'empty'", "delete from t"} {
		generation := store.Pager().Meta().Generation
		r, err := Execute(db, q)
		if err != nil || r.RowsAffected != 0 {
			t.Fatalf("empty mutation: %+v error=%v", r, err)
		}
		if store.Pager().Meta().Generation != generation {
			t.Fatal("empty mutation published a commit")
		}
	}
}

func TestSQLMultiRowMutationRecoveryAtCommitBoundaries(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"pages-written", "pages-synced", "meta-published"} {
		for _, operation := range []string{"update", "delete"} {
			t.Run(stage+"/"+operation, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "db")
				db, err := table.Open(path, table.Options{PageSize: 512})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { db.Close() })
				for _, q := range []string{"create table t (id int64, body bytes, primary key (id))", "create index body_idx on t (body)", "insert into t (id,body) values (1,'a')", "insert into t (id,body) values (2,'b')"} {
					if _, err := Execute(db, q); err != nil {
						t.Fatal(err)
					}
				}
				before, err := Execute(db, "select * from t")
				if err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = table.Open(path, table.Options{FailAfterCommitStage: stage})
				if err != nil {
					t.Fatal(err)
				}
				q := "delete from t"
				if operation == "update" {
					q = "update t set body = 'new'"
				}
				if r, err := Execute(db, q); err == nil || r.RowsAffected != 0 {
					t.Fatalf("expected interruption: %+v error=%v", r, err)
				}
				want := before
				if stage == "meta-published" {
					want = Result{Columns: []string{"id", "body"}}
					if operation == "update" {
						want.Rows = [][]table.Value{{table.Int64Value(1), table.BytesValue([]byte("new"))}, {table.Int64Value(2), table.BytesValue([]byte("new"))}}
						want.RowsAffected = 2
					}
				}
				assertSQLState(t, db, "t", want)
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				db, err = table.Open(path, table.Options{})
				if err != nil {
					t.Fatal(err)
				}
				assertSQLState(t, db, "t", want)
				if _, err := Execute(db, "insert into t (id,body) values (3,'after-recovery')"); err != nil {
					t.Fatal(err)
				}
				report, err := db.Check()
				if err != nil || !report.OK() {
					t.Fatalf("write after recovery: %+v error=%v", report, err)
				}
			})
		}
	}
}

func assertSQLState(t *testing.T, db *table.DB, name string, want Result) {
	t.Helper()
	got, err := Execute(db, "select * from "+name)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state differs: got=%+v want=%+v", got, want)
	}
	report, err := db.Check()
	if err != nil || !report.OK() {
		t.Fatalf("integrity: %+v error=%v", report, err)
	}
}
