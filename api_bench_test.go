package sceptre

import (
	"context"
	"path/filepath"
	"testing"
)

// BenchmarkParallelSelect measures point SELECTs through DB.Execute from
// parallel callers. Before reads ran on snapshots, every call took the
// execution gate, so callers ran one at a time.
func BenchmarkParallelSelect(b *testing.B) {
	ctx := context.Background()
	db, err := Open(filepath.Join(b.TempDir(), "db"), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Execute(ctx, "create table posts (id int64, body bytes, primary key (id))"); err != nil {
		b.Fatal(err)
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		if _, err := tx.Execute(ctx, "insert into posts (id, body) values (?, 'post body')", Int64(int64(i))); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			res, err := db.Execute(ctx, "select body from posts where id = ?", Int64(int64(i*7919%1000)))
			if err != nil || len(res.Rows) != 1 {
				b.Error(err)
				return
			}
			i++
		}
	})
}
