package kv

import (
	"runtime"

	"github.com/vennictus/sceptre/internal/storage"
)

// Snapshot is a consistent read-only view of one committed state.
// Release it when done so its pages can be reused.
type Snapshot struct {
	r *storage.ReadTxn
}

// Snapshot takes a snapshot of the latest committed state.
func (kv *KV) Snapshot() (*Snapshot, error) {
	r, err := kv.env.BeginRead()
	if err != nil {
		return nil, err
	}
	return &Snapshot{r: r}, nil
}

// Get looks up key in the snapshot.
func (s *Snapshot) Get(key []byte) ([]byte, bool, error) { return s.r.Get(key) }

// Cursor returns an ordered cursor over the snapshot, valid until Release.
func (s *Snapshot) Cursor() *storage.Cursor { return s.r.Cursor() }

// Release ends the snapshot. Repeated calls are safe.
func (s *Snapshot) Release() {
	if s != nil && s.r != nil {
		s.r.Release()
	}
}

// Iterator walks one committed snapshot in key order. Its view never changes,
// even across later writes. Close releases the snapshot; an unreachable
// iterator is also released by the garbage collector as a backstop.
type Iterator struct {
	snap *Snapshot
	c    *storage.Cursor
	err  error
}

// Iterator returns an iterator over the latest committed state.
func (kv *KV) Iterator() *Iterator {
	snap, err := kv.Snapshot()
	if err != nil {
		return &Iterator{err: err}
	}
	it := &Iterator{snap: snap, c: snap.Cursor()}
	runtime.SetFinalizer(it, (*Iterator).Close)
	return it
}

// ScanIterator implements Store.
func (kv *KV) ScanIterator() Cursor { return kv.Iterator() }

func (it *Iterator) failed() error {
	if it.err != nil {
		return it.err
	}
	return it.c.Err()
}

// Valid reports whether the iterator points at a key/value pair.
func (it *Iterator) Valid() bool { return it != nil && it.err == nil && it.c.Valid() }

// SeekFirst positions the iterator at the first key.
func (it *Iterator) SeekFirst() error {
	if it.err != nil {
		return it.err
	}
	return it.c.SeekFirst()
}

// SeekLast positions the iterator at the last key.
func (it *Iterator) SeekLast() error {
	if it.err != nil {
		return it.err
	}
	return it.c.SeekLast()
}

// SeekGE positions the iterator at the first key at or after key.
func (it *Iterator) SeekGE(key []byte) error {
	if it.err != nil {
		return it.err
	}
	return it.c.SeekGE(key)
}

// SeekLE positions the iterator at the last key at or before key.
func (it *Iterator) SeekLE(key []byte) error {
	if it.err != nil {
		return it.err
	}
	return it.c.SeekLE(key)
}

// Next advances to the next key/value pair.
func (it *Iterator) Next() error {
	if it.err != nil {
		return it.err
	}
	return it.c.Next()
}

// Prev moves to the previous key/value pair.
func (it *Iterator) Prev() error {
	if it.err != nil {
		return it.err
	}
	return it.c.Prev()
}

// Deref returns copies of the current key and value.
func (it *Iterator) Deref() ([]byte, []byte, error) {
	if err := it.failed(); err != nil {
		return nil, nil, err
	}
	k, err := it.c.Key()
	if err != nil {
		return nil, nil, err
	}
	v, err := it.c.Value()
	if err != nil {
		return nil, nil, err
	}
	return k, v, nil
}

// Close releases the iterator's snapshot. Repeated calls are safe.
func (it *Iterator) Close() error {
	if it != nil && it.snap != nil {
		it.snap.Release()
		runtime.SetFinalizer(it, nil)
	}
	return nil
}
