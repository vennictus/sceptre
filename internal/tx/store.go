package tx

import (
	"github.com/vennictus/sceptre/internal/btree"
	"github.com/vennictus/sceptre/internal/kv"
)

// Apply validates the entire statement against a private working tree before
// publishing any pending mutations. A failed statement leaves the Tx usable.
func (tx *Tx) Apply(mutations []kv.Mutation) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	working, err := tx.view()
	if err != nil {
		return err
	}
	for _, m := range mutations {
		if m.Delete {
			_, err = working.Delete(m.Key)
		} else {
			_, err = working.Delete(m.Key)
			if err == nil {
				err = working.Insert(m.Key, m.Value)
			}
		}
		if err != nil {
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

func (tx *Tx) view() (*btree.Tree, error) {
	tree, err := btree.NewTreeFromSnapshot(int(tx.store.Pager().PageSize()), tx.snapshot.Snapshot())
	if err != nil {
		return nil, err
	}
	for _, key := range tx.order {
		m := tx.pending[key].mutation
		if m.Delete {
			_, err = tree.Delete(m.Key)
		} else {
			_, err = tree.Delete(m.Key)
			if err == nil {
				err = tree.Insert(m.Key, m.Value)
			}
		}
		if err != nil {
			return nil, err
		}
	}
	return tree, nil
}

// ScanIterator includes pending mutations and conservatively tracks a range
// read as a read of the whole keyspace for managed conflict detection.
func (tx *Tx) ScanIterator() kv.Cursor {
	if err := tx.ensureOpen(); err != nil {
		return &cursor{err: err}
	}
	tx.scanned = true
	tree, err := tx.view()
	if err != nil {
		return &cursor{err: err}
	}
	return &cursor{inner: tree.Iterator()}
}

type cursor struct {
	inner *btree.Iterator
	err   error
}

func (c *cursor) SeekFirst() error {
	if c.err != nil {
		return c.err
	}
	return c.inner.SeekFirst()
}

func (c *cursor) Valid() bool { return c.err == nil && c.inner != nil && c.inner.Valid() }
func (c *cursor) SeekGE(k []byte) error {
	if c.err != nil {
		return c.err
	}
	return c.inner.SeekGE(k)
}
func (c *cursor) Next() error {
	if c.err != nil {
		return c.err
	}
	return c.inner.Next()
}
func (c *cursor) Deref() ([]byte, []byte, error) {
	if c.err != nil {
		return nil, nil, c.err
	}
	v, err := c.inner.Deref()
	return v.Key, v.Value, err
}
