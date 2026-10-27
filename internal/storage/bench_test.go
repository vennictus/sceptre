package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/vennictus/sceptre/internal/pager"
)

// benchEnv returns an environment holding n keys with payload-byte values,
// written in batches, with a cache large enough to keep reads warm.
func benchEnv(b *testing.B, n, payload int) *Env {
	b.Helper()
	e, err := Open(filepath.Join(b.TempDir(), "db"), Options{CacheBytes: 256 << 20})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { e.Close() })
	value := bytes.Repeat([]byte("v"), payload)
	for start := 0; start < n; start += 1000 {
		w, err := e.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		for i := start; i < start+1000 && i < n; i++ {
			if err := w.Put(benchKey(i), value); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	return e
}

func benchKey(i int) []byte {
	k := make([]byte, 9)
	k[0] = 'k'
	binary.BigEndian.PutUint64(k[1:], uint64(i))
	return k
}

var readSizes = []struct{ keys, payload int }{{10000, 128}, {10000, 2048}}

// BenchmarkPointGet measures a warm primary-key lookup through a read snapshot.
func BenchmarkPointGet(b *testing.B) {
	for _, sz := range readSizes {
		b.Run(fmt.Sprintf("keys=%d/payload=%d", sz.keys, sz.payload), func(b *testing.B) {
			e := benchEnv(b, sz.keys, sz.payload)
			r, err := e.BeginRead()
			if err != nil {
				b.Fatal(err)
			}
			defer r.Release()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok, err := r.Get(benchKey(i * 7919 % sz.keys)); err != nil || !ok {
					b.Fatal(ok, err)
				}
			}
		})
	}
}

// BenchmarkRangeScan32 measures seeking and reading 32 consecutive entries.
func BenchmarkRangeScan32(b *testing.B) {
	for _, sz := range readSizes {
		b.Run(fmt.Sprintf("keys=%d/payload=%d", sz.keys, sz.payload), func(b *testing.B) {
			e := benchEnv(b, sz.keys, sz.payload)
			r, err := e.BeginRead()
			if err != nil {
				b.Fatal(err)
			}
			defer r.Release()
			c := r.Cursor()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := c.SeekGE(benchKey(i * 7919 % (sz.keys - 32))); err != nil {
					b.Fatal(err)
				}
				for j := 0; j < 32 && c.Valid(); j++ {
					if _, err := c.Key(); err != nil {
						b.Fatal(err)
					}
					if _, err := c.Value(); err != nil {
						b.Fatal(err)
					}
					if err := c.Next(); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkParallelPointGet measures warm lookups from concurrent readers,
// where page-cache locking shows up.
func BenchmarkParallelPointGet(b *testing.B) {
	e := benchEnv(b, 10000, 128)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r, err := e.BeginRead()
		if err != nil {
			b.Error(err)
			return
		}
		defer r.Release()
		i := 0
		for pb.Next() {
			if _, ok, err := r.Get(benchKey(i * 7919 % 10000)); err != nil || !ok {
				b.Error(ok, err)
				return
			}
			i++
		}
	})
}

// BenchmarkSmallCommitWithManyFreePages measures a one-key commit in a file
// holding thousands of free pages, reporting bytes written per commit. With
// the freelist stored as a chain, every commit rewrote the whole list.
func BenchmarkSmallCommitWithManyFreePages(b *testing.B) {
	var written atomic.Int64
	hooks := &pager.IOHooks{WriteAt: func(f *os.File, p []byte, off int64) (int, error) {
		written.Add(int64(len(p)))
		return f.WriteAt(p, off)
	}}
	e, err := Open(filepath.Join(b.TempDir(), "db"), Options{IO: hooks})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()
	value := bytes.Repeat([]byte("v"), 600)
	commitAll := func(apply func(w *WriteTxn) error) {
		w, err := e.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		if err := apply(w); err != nil {
			b.Fatal(err)
		}
		if err := w.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	commitAll(func(w *WriteTxn) error {
		for i := 0; i < 20000; i++ {
			if err := w.Put(benchKey(i), value); err != nil {
				return err
			}
		}
		return nil
	})
	commitAll(func(w *WriteTxn) error { // frees thousands of pages
		for i := 0; i < 15000; i++ {
			if _, err := w.Delete(benchKey(i)); err != nil {
				return err
			}
		}
		return nil
	})
	free := e.Info().FreePages
	written.Store(0)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		commitAll(func(w *WriteTxn) error { return w.Put(benchKey(20000+i%100), value) })
	}
	b.ReportMetric(float64(written.Load())/float64(b.N), "B-written/op")
	b.ReportMetric(float64(free), "free-pages")
}
