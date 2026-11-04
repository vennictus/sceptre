package sceptre

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
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

// BenchmarkAutocommitInsert measures one-row autocommit INSERTs under each
// durability mode. Normal skips the meta page sync, one of two per commit.
func BenchmarkAutocommitInsert(b *testing.B) {
	for _, dur := range []Durability{DurabilityFull, DurabilityNormal} {
		b.Run(dur.String(), func(b *testing.B) {
			ctx := context.Background()
			db, err := Open(filepath.Join(b.TempDir(), "db"), Options{Durability: dur})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Execute(ctx, "create table posts (id int64, body bytes, primary key (id))"); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Execute(ctx, "insert into posts (id, body) values (?, 'post body')", Int64(int64(i))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkStagedUpsertUnique stages one multi-row upsert into a table with a
// unique index, in a transaction that is rolled back. Each row probes the
// unique index through the transaction's pending writes, so the cost per row
// must stay flat as the statement grows.
func BenchmarkStagedUpsertUnique(b *testing.B) {
	for _, n := range []int{200, 800, 3200} {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			ctx := context.Background()
			db, err := Open(filepath.Join(b.TempDir(), "db"), Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Execute(ctx, "create table t (id int64 primary key, v int64 unique)"); err != nil {
				b.Fatal(err)
			}
			rows := make([]string, n)
			for i := range rows {
				rows[i] = fmt.Sprintf("(%d,%d)", i, i)
			}
			q := "insert into t values " + strings.Join(rows, ",") + " on conflict (id) do nothing"
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := db.Begin(ctx)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := tx.Execute(ctx, q); err != nil {
					b.Fatal(err)
				}
				if err := tx.Rollback(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
