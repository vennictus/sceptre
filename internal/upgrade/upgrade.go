// Package upgrade converts format v1 database files to format v2.
package upgrade

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vennictus/sceptre/internal/btree"
	"github.com/vennictus/sceptre/internal/pager"
	"github.com/vennictus/sceptre/internal/storage"
)

var (
	// ErrAlreadyCurrent reports a file that already uses format v2.
	ErrAlreadyCurrent = errors.New("upgrade: file already uses format v2")
	// ErrLeftover reports files from an earlier upgrade attempt that are in the way.
	ErrLeftover = errors.New("upgrade: leftover file from an earlier attempt")
	// ErrMismatch reports that the new file does not hold exactly the old contents.
	ErrMismatch = errors.New("upgrade: converted contents differ from the original")
)

// batchKeys bounds the keys per write transaction, so memory stays bounded
// for large files. The target is a temporary file until verified.
const batchKeys = 10000

// Report describes a completed upgrade.
type Report struct {
	Path         string
	Backup       string // the original v1 file, kept unchanged
	Keys         int
	Bytes        int64 // key and value bytes copied
	OldPageSize  uint32
	NewPageSize  uint32
	OldPageCount uint64
	NewPageCount uint64
	Duration     time.Duration
}

// Paths returns the temporary and backup paths Upgrade uses for path.
func Paths(path string) (temp, backup string) {
	return path + ".upgrade", path + ".v1.bak"
}

// Upgrade converts the v1 file at path to v2 in place, keeping the original
// as a backup. The conversion is written to a temporary file and verified
// key by key and with a full storage check before any file is renamed, so a
// failure at any point leaves the original untouched at path.
func Upgrade(path string) (Report, error) {
	start := time.Now()
	temp, backup := Paths(path)
	if _, err := os.Stat(path); err != nil {
		return Report{}, err // never create an empty database here
	}
	src, err := pager.Open(path, pager.Options{})
	if err != nil {
		return Report{}, err
	}
	srcOpen := true
	defer func() {
		if srcOpen {
			src.Close()
		}
	}()
	meta := src.Meta()
	if meta.Version != pager.FormatV1 {
		return Report{}, ErrAlreadyCurrent
	}
	for _, p := range []string{temp, backup} {
		if _, err := os.Stat(p); err == nil {
			return Report{}, fmt.Errorf("%w: %s", ErrLeftover, p)
		} else if !errors.Is(err, os.ErrNotExist) {
			return Report{}, err
		}
	}
	pages, err := btree.ValidateTree(meta.RootPage, src.ReadPage)
	if err != nil {
		return Report{}, fmt.Errorf("upgrade: read v1 tree: %w", err)
	}
	tree, err := btree.NewTreeFromSnapshot(int(meta.PageSize), btree.Snapshot{Root: meta.RootPage, NextPage: meta.PageCount, Pages: pages})
	if err != nil {
		return Report{}, fmt.Errorf("upgrade: load v1 tree: %w", err)
	}

	// v1 allowed page sizes v2 does not; those files move to the default.
	pageSize := meta.PageSize
	if pageSize < storage.MinPageSize || pageSize > storage.MaxPageSize || pageSize&(pageSize-1) != 0 {
		pageSize = pager.DefaultPageSize
	}
	rep := Report{Path: path, Backup: backup, OldPageSize: meta.PageSize, NewPageSize: pageSize, OldPageCount: meta.PageCount}

	dst, err := storage.Open(temp, storage.Options{PageSize: pageSize})
	if err != nil {
		return Report{}, err
	}
	dstOpen := true
	defer func() {
		if dstOpen {
			dst.Close()
		}
		if err != nil {
			os.Remove(temp)
		}
	}()

	want := sha256.New()
	if err = copyAll(tree, dst, want, &rep); err != nil {
		return Report{}, err
	}
	if err = verify(dst, rep.Keys, want.Sum(nil)); err != nil {
		return Report{}, err
	}
	rep.NewPageCount = dst.Info().PageCount

	if err = dst.Close(); err != nil {
		return Report{}, err
	}
	dstOpen = false
	if err = src.Close(); err != nil {
		return Report{}, err
	}
	srcOpen = false
	if err = os.Rename(path, backup); err != nil {
		return Report{}, err
	}
	if err = os.Rename(temp, path); err != nil {
		// Put the original back so path never goes missing.
		if restoreErr := os.Rename(backup, path); restoreErr != nil {
			return Report{}, fmt.Errorf("upgrade: install failed (%v); original is at %s", err, backup)
		}
		return Report{}, err
	}
	syncDir(filepath.Dir(path))
	rep.Duration = time.Since(start)
	return rep, nil
}

// copyAll writes every v1 entry to dst in key order, hashing the stream.
func copyAll(tree *btree.Tree, dst *storage.Env, sum interface{ Write([]byte) (int, error) }, rep *Report) error {
	it := tree.Iterator()
	if err := it.SeekFirst(); err != nil {
		return err
	}
	for it.Valid() {
		w, err := dst.BeginWrite()
		if err != nil {
			return err
		}
		for n := 0; n < batchKeys && it.Valid(); n++ {
			cell, err := it.Deref()
			if err != nil {
				w.Abort()
				return err
			}
			if err := w.Put(cell.Key, cell.Value); err != nil {
				w.Abort()
				return fmt.Errorf("upgrade: key %q: %w", cell.Key, err)
			}
			hashEntry(sum, cell.Key, cell.Value)
			rep.Keys++
			rep.Bytes += int64(len(cell.Key) + len(cell.Value))
			if err := it.Next(); err != nil {
				w.Abort()
				return err
			}
		}
		if err := w.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// verify re-reads the converted file and compares it with the original.
func verify(dst *storage.Env, keys int, want []byte) error {
	if _, err := dst.Check(); err != nil {
		return fmt.Errorf("upgrade: verify converted file: %w", err)
	}
	r, err := dst.BeginRead()
	if err != nil {
		return err
	}
	defer r.Release()
	got := sha256.New()
	n := 0
	c := r.Cursor()
	for err := c.SeekFirst(); c.Valid(); err = c.Next() {
		if err != nil {
			return err
		}
		k, err := c.Key()
		if err != nil {
			return err
		}
		v, err := c.Value()
		if err != nil {
			return err
		}
		hashEntry(got, k, v)
		n++
	}
	if c.Err() != nil {
		return c.Err()
	}
	if n != keys || !bytes.Equal(got.Sum(nil), want) {
		return fmt.Errorf("%w: %d keys read back, %d written", ErrMismatch, n, keys)
	}
	return nil
}

// hashEntry feeds a length-prefixed entry so different splits cannot collide.
func hashEntry(sum interface{ Write([]byte) (int, error) }, key, value []byte) {
	var lens [8]byte
	putUint32(lens[:4], uint32(len(key)))
	putUint32(lens[4:], uint32(len(value)))
	sum.Write(lens[:])
	sum.Write(key)
	sum.Write(value)
}

func putUint32(b []byte, v uint32) {
	b[0], b[1], b[2], b[3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
}

// syncDir makes the renames durable where the platform supports it.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync() // not supported on every platform; best effort
		d.Close()
	}
}
