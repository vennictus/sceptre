package tx

import (
	"bytes"
	"sort"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/storage"
)

// Apply validates every mutation of a statement before buffering any of them,
// so a failed statement leaves the Tx and its earlier statements intact.
func (tx *Tx) Apply(mutations []kv.Mutation) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	for _, m := range mutations {
		if err := tx.store.Validate(m); err != nil {
			return err
		}
	}
	for _, m := range mutations {
		if m.Delete {
			tx.record(kv.Delete(m.Key))
		} else {
			tx.record(kv.Put(m.Key, m.Value))
		}
	}
	return nil
}

// ScanIterator merges pending mutations over the snapshot, as of this call.
// It conservatively tracks a range read as a read of the whole keyspace for
// managed conflict detection.
func (tx *Tx) ScanIterator() kv.Cursor {
	if err := tx.ensureOpen(); err != nil {
		return &cursor{err: err}
	}
	tx.scanned = true
	keys := make([]string, 0, len(tx.pending))
	pending := make(map[string]kv.Mutation, len(tx.pending))
	for key, p := range tx.pending {
		keys = append(keys, key)
		pending[key] = p.mutation
	}
	sort.Strings(keys)
	return &cursor{base: tx.snapshot.Cursor(), keys: keys, pending: pending}
}

// cursor walks the snapshot and the transaction's pending writes as one
// ordered sequence. A pending write replaces the snapshot entry with the same
// key, and a pending delete hides it.
type cursor struct {
	base    *storage.Cursor
	keys    []string
	pending map[string]kv.Mutation
	pi      int // next pending key
	err     error

	valid bool
	key   []byte
	value []byte // nil means read the value from base
}

func (c *cursor) Valid() bool { return c.err == nil && c.valid }

func (c *cursor) Close() error { return nil } // the snapshot belongs to the Tx

func (c *cursor) SeekFirst() error {
	if c.err != nil {
		return c.err
	}
	c.pi = 0
	if err := c.base.SeekFirst(); err != nil {
		return c.fail(err)
	}
	return c.settle()
}

func (c *cursor) SeekGE(key []byte) error {
	if c.err != nil {
		return c.err
	}
	c.pi = sort.SearchStrings(c.keys, string(key))
	if err := c.base.SeekGE(key); err != nil {
		return c.fail(err)
	}
	return c.settle()
}

func (c *cursor) Next() error {
	if c.err != nil || !c.valid {
		return c.err
	}
	// Advance every source positioned on the current key.
	if c.pi < len(c.keys) && c.keys[c.pi] == string(c.key) {
		c.pi++
	}
	if c.base.Valid() {
		k, err := c.base.Key()
		if err != nil {
			return c.fail(err)
		}
		if bytes.Equal(k, c.key) {
			if err := c.base.Next(); err != nil {
				return c.fail(err)
			}
		}
	}
	return c.settle()
}

// settle picks the smallest key across both sources, skipping pending deletes
// and the snapshot entries they hide.
func (c *cursor) settle() error {
	for {
		var baseKey []byte
		if c.base.Valid() {
			k, err := c.base.Key()
			if err != nil {
				return c.fail(err)
			}
			baseKey = k
		} else if err := c.base.Err(); err != nil {
			return c.fail(err)
		}
		hasPending := c.pi < len(c.keys)
		switch {
		case baseKey == nil && !hasPending:
			c.valid = false
			return nil
		case hasPending && (baseKey == nil || c.keys[c.pi] <= string(baseKey)):
			key := c.keys[c.pi]
			m := c.pending[key]
			if baseKey != nil && key == string(baseKey) {
				if err := c.base.Next(); err != nil {
					return c.fail(err)
				}
			}
			if m.Delete {
				c.pi++
				continue
			}
			// Keep the pending key current; Next advances past it.
			c.valid, c.key, c.value = true, []byte(key), m.Value
			return nil
		default:
			c.valid, c.key, c.value = true, baseKey, nil
			return nil
		}
	}
}

func (c *cursor) Deref() ([]byte, []byte, error) {
	if c.err != nil {
		return nil, nil, c.err
	}
	if !c.valid {
		return nil, nil, storage.ErrCursorInvalid
	}
	if c.value != nil || c.pi < len(c.keys) && c.keys[c.pi] == string(c.key) {
		return cloneBytes(c.key), cloneBytes(c.value), nil
	}
	v, err := c.base.Value()
	if err != nil {
		return nil, nil, c.fail(err)
	}
	return cloneBytes(c.key), v, nil
}

func (c *cursor) fail(err error) error {
	c.err = err
	c.valid = false
	return err
}

func cloneBytes(b []byte) []byte { return append([]byte(nil), b...) }
