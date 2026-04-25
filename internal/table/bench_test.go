package table

import (
	"fmt"
	"path/filepath"
	"testing"
)

func BenchmarkInsertRows(b *testing.B) {
	db := mustOpenBenchDB(b)
	defer db.Close()
	mustCreateBenchUsers(b, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Insert("users", benchUser(int64(i), "Ada", 31)); err != nil {
			b.Fatalf("Insert() error = %v", err)
		}
	}
}

func BenchmarkPointLookup(b *testing.B) {
	db := mustOpenBenchDB(b)
	defer db.Close()
	mustCreateBenchUsers(b, db)
	for i := 0; i < 1000; i++ {
		if err := db.Insert("users", benchUser(int64(i), "Ada", 31)); err != nil {
			b.Fatalf("Insert() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok, err := db.Get("users", NewRecord(map[string]Value{"id": Int64Value(int64(i % 1000))})); err != nil {
			b.Fatalf("Get() error = %v", err)
		} else if !ok {
			b.Fatal("Get() ok = false, want true")
		}
	}
}

func BenchmarkFullScan(b *testing.B) {
	db := mustOpenBenchDB(b)
	defer db.Close()
	mustCreateBenchUsers(b, db)
	for i := 0; i < 1000; i++ {
		if err := db.Insert("users", benchUser(int64(i), "Ada", int64(i%100))); err != nil {
			b.Fatalf("Insert() error = %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner, err := db.Scan("users", ScanBounds{})
		if err != nil {
			b.Fatalf("Scan() error = %v", err)
		}
		rows := 0
		for scanner.Valid() {
			if _, err := scanner.Deref(); err != nil {
				b.Fatalf("Deref() error = %v", err)
			}
			rows++
			if err := scanner.Next(); err != nil {
				b.Fatalf("Next() error = %v", err)
			}
		}
		if rows != 1000 {
			b.Fatalf("rows = %d, want 1000", rows)
		}
	}
}

func BenchmarkSecondaryIndexLookup(b *testing.B) {
	db := mustOpenBenchDB(b)
	defer db.Close()
	mustCreateBenchUsers(b, db)
	for i := 0; i < 1000; i++ {
		if err := db.Insert("users", benchUser(int64(i), "Ada", int64(i%100))); err != nil {
			b.Fatalf("Insert() error = %v", err)
		}
	}
	if err := db.CreateIndex("users", IndexDef{Name: "users_age", Columns: []string{"age"}}); err != nil {
		b.Fatalf("CreateIndex() error = %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.LookupIndex("users", "users_age", NewRecord(map[string]Value{"age": Int64Value(int64(i % 100))}))
		if err != nil {
			b.Fatalf("LookupIndex() error = %v", err)
		}
		if len(rows) != 10 {
			b.Fatalf("row count = %d, want 10", len(rows))
		}
	}
}

func mustOpenBenchDB(b *testing.B) *DB {
	b.Helper()

	db, err := Open(filepath.Join(b.TempDir(), "sceptre.db"), Options{PageSize: 512})
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	return db
}

func mustCreateBenchUsers(b *testing.B, db *DB) {
	b.Helper()

	if err := db.CreateTable(usersTableDef()); err != nil {
		b.Fatalf("CreateTable() error = %v", err)
	}
}

func benchUser(id int64, name string, age int64) Record {
	return NewRecord(map[string]Value{
		"id":   Int64Value(id),
		"name": BytesValue([]byte(fmt.Sprintf("%s-%d", name, id))),
		"age":  Int64Value(age),
	})
}
