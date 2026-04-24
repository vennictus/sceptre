package kv

import "sceptre/internal/btree"

// Iterator walks the current committed KV snapshot in key order.
type Iterator struct {
	inner *btree.Iterator
}

// Iterator returns a new ordered iterator over the current tree state.
func (kv *KV) Iterator() *Iterator {
	return &Iterator{inner: kv.tree.Iterator()}
}

// Valid reports whether the iterator points at a key/value pair.
func (it *Iterator) Valid() bool {
	return it != nil && it.inner != nil && it.inner.Valid()
}

// SeekFirst positions the iterator at the first key.
func (it *Iterator) SeekFirst() error {
	return it.inner.SeekFirst()
}

// SeekLast positions the iterator at the last key.
func (it *Iterator) SeekLast() error {
	return it.inner.SeekLast()
}

// SeekGE positions the iterator at the first key greater than or equal to key.
func (it *Iterator) SeekGE(key []byte) error {
	return it.inner.SeekGE(key)
}

// SeekLE positions the iterator at the last key less than or equal to key.
func (it *Iterator) SeekLE(key []byte) error {
	return it.inner.SeekLE(key)
}

// Next advances to the next key/value pair.
func (it *Iterator) Next() error {
	return it.inner.Next()
}

// Prev moves to the previous key/value pair.
func (it *Iterator) Prev() error {
	return it.inner.Prev()
}

// Deref returns the current key/value pair.
func (it *Iterator) Deref() ([]byte, []byte, error) {
	cell, err := it.inner.Deref()
	if err != nil {
		return nil, nil, err
	}
	return cell.Key, cell.Value, nil
}
