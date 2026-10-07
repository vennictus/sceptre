package kv

// Cursor is an ordered, immutable view used by relational scans.
type Cursor interface {
	Valid() bool
	SeekFirst() error
	SeekGE([]byte) error
	Next() error
	Deref() ([]byte, []byte, error)
}

// Store supplies statement-atomic mutations and reads of their current view.
type Store interface {
	Get([]byte) ([]byte, bool, error)
	Apply([]Mutation) error
	ScanIterator() Cursor
}

func (kv *KV) ScanIterator() Cursor { return kv.Iterator() }
