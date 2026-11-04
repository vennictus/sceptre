package tx

import (
	"bytes"
	"sort"
	"strings"

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
//
// Opening a cursor is cheap: it reads the pending writes in place, through
// the transaction's sorted key index. The first write to the transaction
// while the cursor is still open freezes the cursor on a private copy of
// the pending writes it could see, so it keeps reading them as of this call.
func (tx *Tx) ScanIterator() kv.Cursor {
	if err := tx.ensureOpen(); err != nil {
		return &cursor{err: err}
	}
	tx.scanned = true
	c := &cursor{base: tx.snapshot.Cursor(), tx: tx}
	if tx.cursors == nil {
		tx.cursors = make(map[*cursor]struct{})
	}
	tx.cursors[c] = struct{}{}
	return c
}

// freezeCursors gives every open live cursor its own copy of the pending
// writes before the transaction changes them.
func (tx *Tx) freezeCursors() {
	for c := range tx.cursors {
		c.freeze()
	}
	clear(tx.cursors)
}

// cursor walks the snapshot and the transaction's pending writes as one
// ordered sequence. A pending write replaces the snapshot entry with the same
// key, and a pending delete hides it.
//
// A live cursor (tx set) reads tx.index and tx.pending at position (pb, pi).
// A frozen one reads its own keys and pending at position pi.
type cursor struct {
	base    *storage.Cursor
	tx      *Tx
	pb      int
	keys    []string
	pending map[string]kv.Mutation
	pi      int // next pending key
	err     error

	valid bool
	key   []byte
	value []byte // nil means read the value from base
}

func (c *cursor) Valid() bool { return c.err == nil && c.valid }

// Close stops tracking the cursor. The snapshot belongs to the Tx.
func (c *cursor) Close() error {
	if c.tx != nil {
		delete(c.tx.cursors, c)
	}
	return nil
}

// freeze copies the pending writes a live cursor reads, keeping its position.
func (c *cursor) freeze() {
	tx := c.tx
	if tx == nil {
		return
	}
	n := 0
	for _, blk := range tx.index.blocks {
		n += len(blk)
	}
	keys := make([]string, 0, n)
	pi := 0
	for b, blk := range tx.index.blocks {
		if b < c.pb {
			pi += len(blk)
		} else if b == c.pb {
			pi += c.pi
		}
		keys = append(keys, blk...)
	}
	pending := make(map[string]kv.Mutation, len(keys))
	for _, key := range keys {
		pending[key] = tx.pending[key].mutation
	}
	c.tx, c.keys, c.pending, c.pb, c.pi = nil, keys, pending, 0, pi
}

func (c *cursor) hasPending() bool {
	if c.tx != nil {
		return c.tx.index.valid(c.pb, c.pi)
	}
	return c.pi < len(c.keys)
}

func (c *cursor) pendingKey() string {
	if c.tx != nil {
		return c.tx.index.blocks[c.pb][c.pi]
	}
	return c.keys[c.pi]
}

func (c *cursor) pendingMutation(key string) kv.Mutation {
	if c.tx != nil {
		return c.tx.pending[key].mutation
	}
	return c.pending[key]
}

func (c *cursor) advancePending() {
	if c.tx != nil {
		c.pb, c.pi = c.tx.index.next(c.pb, c.pi)
		return
	}
	c.pi++
}

func (c *cursor) seekPending(key string) {
	if c.tx != nil {
		c.pb, c.pi = c.tx.index.find(key)
		return
	}
	c.pi = sort.SearchStrings(c.keys, key)
}

// ready reports the cursor's error, or ErrClosed once its Tx has ended.
func (c *cursor) ready() error {
	if c.err == nil && c.tx != nil && c.tx.closed {
		return c.fail(ErrClosed)
	}
	return c.err
}

func (c *cursor) SeekFirst() error {
	if err := c.ready(); err != nil {
		return err
	}
	c.seekPending("")
	if err := c.base.SeekFirst(); err != nil {
		return c.fail(err)
	}
	return c.settle()
}

func (c *cursor) SeekGE(key []byte) error {
	if err := c.ready(); err != nil {
		return err
	}
	c.seekPending(string(key))
	if err := c.base.SeekGE(key); err != nil {
		return c.fail(err)
	}
	return c.settle()
}

func (c *cursor) Next() error {
	if err := c.ready(); err != nil || !c.valid {
		return err
	}
	// Advance every source positioned on the current key.
	if c.hasPending() && c.pendingKey() == string(c.key) {
		c.advancePending()
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
		hasPending := c.hasPending()
		switch {
		case baseKey == nil && !hasPending:
			c.valid = false
			return nil
		case hasPending && (baseKey == nil || strings.Compare(c.pendingKey(), string(baseKey)) <= 0):
			key := c.pendingKey()
			m := c.pendingMutation(key)
			if baseKey != nil && key == string(baseKey) {
				if err := c.base.Next(); err != nil {
					return c.fail(err)
				}
			}
			if m.Delete {
				c.advancePending()
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
	if err := c.ready(); err != nil {
		return nil, nil, err
	}
	if !c.valid {
		return nil, nil, storage.ErrCursorInvalid
	}
	if c.value != nil || c.hasPending() && c.pendingKey() == string(c.key) {
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
