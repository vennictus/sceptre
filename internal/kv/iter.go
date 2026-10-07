package kv

import "github.com/vennictus/sceptre/internal/btree"

// Iterator walks the current committed KV snapshot in key order.
type Iterator struct {
	inner *btree.Iterator
	err   error
}

// Iterator returns a new ordered iterator over the current tree state.
func (kv *KV) Iterator() *Iterator {
	return &Iterator{inner: kv.tree.Iterator(), err: kv.failure}
}

// Valid reports whether the iterator points at a key/value pair.
func (it *Iterator) Valid() bool {
	return it != nil && it.err == nil && it.inner != nil && it.inner.Valid()
}

// SeekFirst positions the iterator at the first key.
func (it *Iterator) SeekFirst() error {
	if it.err != nil {
		return it.err
	}
	return it.inner.SeekFirst()
}

// SeekLast positions the iterator at the last key.
func (it *Iterator) SeekLast() error {
	if it.err != nil {
		return it.err
	}
	return it.inner.SeekLast()
}

// SeekGE positions the iterator at the first key greater than or equal to key.
func (it *Iterator) SeekGE(key []byte) error {
	if it.err != nil {
		return it.err
	}
	return it.inner.SeekGE(key)
}

// SeekLE positions the iterator at the last key less than or equal to key.
func (it *Iterator) SeekLE(key []byte) error {
	if it.err != nil {
		return it.err
	}
	return it.inner.SeekLE(key)
}

// Next advances to the next key/value pair.
func (it *Iterator) Next() error {
	if it.err != nil {
		return it.err
	}
	return it.inner.Next()
}

// Prev moves to the previous key/value pair.
func (it *Iterator) Prev() error {
	if it.err != nil {
		return it.err
	}
	return it.inner.Prev()
}

// Deref returns the current key/value pair.
func (it *Iterator) Deref() ([]byte, []byte, error) {
	if it.err != nil {
		return nil, nil, it.err
	}
	cell, err := it.inner.Deref()
	if err != nil {
		return nil, nil, err
	}
	return cell.Key, cell.Value, nil
}
