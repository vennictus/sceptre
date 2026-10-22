package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
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
