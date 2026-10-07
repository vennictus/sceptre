package table

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestIndexedWorkloadAcrossPageReuseAndReopen(t *testing.T) {
	t.Parallel()
	histories := []workloadHistory{generatedHistory(1), generatedHistory(7), generatedHistory(42), generatedHistory(2026)}
	if path := os.Getenv("SCEPTRE_WORKLOAD_REPLAY"); path != "" {
		h, err := readHistory(path)
		if err != nil {
			t.Fatal(err)
		}
		histories = []workloadHistory{h}
	}
	for _, history := range histories {
		t.Run(fmt.Sprint(history.Seed), func(t *testing.T) {
			executed := workloadHistory{Version: 1, Seed: history.Seed, PageSize: 512}
			t.Cleanup(func() { persistFailure(t, &executed) })
			path := filepath.Join(t.TempDir(), "db")
			db, err := Open(path, Options{PageSize: 512})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			if err := db.CreateTable(TableDef{Name: "t", Columns: []Column{{Name: "id", Type: TypeInt64}, {Name: "body", Type: TypeBytes}}, PrimaryKey: []string{"id"}}); err != nil {
				t.Fatal(err)
			}
			if err := db.CreateIndex("t", IndexDef{Name: "body_idx", Columns: []string{"body"}}); err != nil {
				t.Fatal(err)
			}
			oracle := make(map[int64]string)
			check := func(step int) {
				t.Helper()
				report, err := db.Check()
				if err != nil || !report.OK() {
					t.Fatalf("step=%d integrity=%+v err=%v", step, report, err)
				}
				scan, err := db.Scan("t", ScanBounds{})
				if err != nil {
					t.Fatal(err)
				}
				seen := make(map[int64]bool)
				for scan.Valid() {
					row, err := scan.Deref()
					if err != nil {
						t.Fatal(err)
					}
					id := row.Values["id"].I64
					body, exists := oracle[id]
					if !exists || seen[id] || string(row.Values["body"].Bytes) != body {
						t.Fatalf("step=%d row differs: %+v", step, row)
					}
					seen[id] = true
					if err := scan.Next(); err != nil {
						t.Fatal(err)
					}
				}
				if len(seen) != len(oracle) {
					t.Fatalf("step=%d row count=%d want=%d", step, len(seen), len(oracle))
				}
				for id, body := range oracle {
					row, ok, err := db.Get("t", NewRecord(map[string]Value{"id": Int64Value(id)}))
					if err != nil || !ok || string(row.Values["body"].Bytes) != body {
						t.Fatalf("step=%d point lookup id=%d err=%v", step, id, err)
					}
					rows, err := db.LookupIndex("t", "body_idx", NewRecord(map[string]Value{"body": BytesValue([]byte(body))}))
					if err != nil || len(rows) != 1 || rows[0].Values["id"].I64 != id {
						t.Fatalf("step=%d index id=%d err=%v", step, id, err)
					}
				}
			}
			for step, op := range history.Operations {
				executed.Operations = append(executed.Operations, op)
				id, body := op.ID, op.Body
				record := NewRecord(map[string]Value{"id": Int64Value(id), "body": BytesValue([]byte(body))})
				switch op.Kind {
				case "upsert":
					if err := db.Upsert("t", record); err != nil {
						t.Fatal(err)
					}
					oracle[id] = body
				case "delete":
					_, exists := oracle[id]
					removed, err := db.Delete("t", NewRecord(map[string]Value{"id": Int64Value(id)}))
					if err != nil || removed != exists {
						t.Fatalf("step=%d delete=%v err=%v", step, removed, err)
					}
					delete(oracle, id)
				case "insert":
					_, exists := oracle[id]
					err := db.Insert("t", record)
					if exists {
						if !errors.Is(err, ErrRowExists) {
							t.Fatalf("step=%d duplicate err=%v", step, err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						oracle[id] = body
					}
				}
				if step%10 == 0 {
					check(step)
				}
				if step%25 == 0 {
					if err := db.Close(); err != nil {
						t.Fatal(err)
					}
					db, err = Open(path, Options{})
					if err != nil {
						t.Fatal(err)
					}
					check(step)
				}
			}
			check(len(history.Operations))
		})
	}
}
