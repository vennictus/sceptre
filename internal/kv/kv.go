package kv

import (
	"errors"
	"fmt"
	"sync"

	"github.com/vennictus/sceptre/internal/pager"
	"github.com/vennictus/sceptre/internal/storage"
)

var (
	ErrCommitCommitted  = storage.ErrCommitCommitted
	ErrRecoveryRequired = storage.ErrRecoveryRequired
	ErrUpgradeRequired  = storage.ErrUpgradeRequired
)

// Options controls how the KV layer opens its storage engine.
type Options struct {
	PageSize             uint32
	CacheBytes           int
	FailAfterCommitStage string
	IO                   *pager.IOHooks
}

// KV is an ordered key/value store on the copy-on-write storage engine.
// Each Apply is one atomic, durable commit. Reads and iterators use
// snapshots and never block writes.
type KV struct {
	env *storage.Env

	mu         sync.Mutex
	commitHook commitHook // fault injection for tests
}

// Mutation describes one KV update in an atomic Apply call.
type Mutation struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// Put returns a mutation that stores key/value.
func Put(key, value []byte) Mutation {
	return Mutation{Key: cloneBytes(key), Value: cloneBytes(value)}
}

// Delete returns a mutation that removes key.
func Delete(key []byte) Mutation {
	return Mutation{Key: cloneBytes(key), Delete: true}
}

// Open opens or creates a format v2 database file.
func Open(path string, opts Options) (*KV, error) {
	store := &KV{}
	if opts.FailAfterCommitStage != "" {
		hook, err := commitHookForStage(opts.FailAfterCommitStage)
		if err != nil {
			return nil, err
		}
		store.commitHook = hook
	}
	env, err := storage.Open(path, storage.Options{
		PageSize:   opts.PageSize,
		CacheBytes: opts.CacheBytes,
		IO:         opts.IO,
		CommitHook: store.runCommitHook,
	})
	if err != nil {
		return nil, err
	}
	store.env = env
	return store, nil
}

// Close closes the database file.
func (kv *KV) Close() error {
	if kv == nil || kv.env == nil {
		return nil
	}
	return kv.env.Close()
}

// Err reports whether an uncertain publication has made this handle unusable.
func (kv *KV) Err() error { return kv.env.Err() }

// Env exposes the storage engine.
func (kv *KV) Env() *storage.Env { return kv.env }

// Pager exposes the underlying file for inspection.
func (kv *KV) Pager() *pager.Pager { return kv.env.Pager() }

// Generation returns the transaction ID of the latest committed state.
func (kv *KV) Generation() uint64 { return kv.env.TxID() }

// Get looks up a key in the latest committed state.
func (kv *KV) Get(key []byte) ([]byte, bool, error) {
	r, err := kv.env.BeginRead()
	if err != nil {
		return nil, false, err
	}
	defer r.Release()
	return r.Get(key)
}

// Set inserts or replaces a key/value pair in one commit.
func (kv *KV) Set(key, value []byte) error {
	return kv.Apply([]Mutation{Put(key, value)})
}

// Del removes a key in one commit and reports whether it existed.
func (kv *KV) Del(key []byte) (bool, error) {
	w, err := kv.env.BeginWrite()
	if err != nil {
		return false, err
	}
	found, err := w.Delete(key)
	if err != nil || !found {
		w.Abort()
		return false, err
	}
	if err := w.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// Validate reports whether a mutation can be stored, without storing it.
func (kv *KV) Validate(m Mutation) error {
	switch {
	case len(m.Key) == 0:
		return storage.ErrEmptyKey
	case len(m.Key) > kv.env.MaxKeySize():
		return fmt.Errorf("%w: %d bytes, limit %d", storage.ErrKeyTooLarge, len(m.Key), kv.env.MaxKeySize())
	case !m.Delete && len(m.Value) > storage.MaxValueSize:
		return storage.ErrValueTooLarge
	}
	return nil
}

// Apply atomically applies all mutations in one durable commit. On any error
// other than ErrCommitCommitted, none of them are visible.
func (kv *KV) Apply(mutations []Mutation) error {
	if len(mutations) == 0 {
		return kv.env.Err()
	}
	for _, m := range mutations {
		if err := kv.Validate(m); err != nil {
			return err
		}
	}
	w, err := kv.env.BeginWrite()
	if err != nil {
		return err
	}
	for _, m := range mutations {
		if m.Delete {
			_, err = w.Delete(m.Key)
		} else {
			err = w.Put(m.Key, m.Value)
		}
		if err != nil {
			w.Abort()
			return err
		}
	}
	return w.Commit()
}

// Check verifies the full committed state, including every page.
func (kv *KV) Check() (storage.CheckReport, error) { return kv.env.Check() }

func (kv *KV) runCommitHook(stage string) error {
	kv.mu.Lock()
	hook := kv.commitHook
	kv.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(commitStage(stage))
}

func (kv *KV) setCommitHook(hook commitHook) {
	kv.mu.Lock()
	kv.commitHook = hook
	kv.mu.Unlock()
}

func cloneBytes(src []byte) []byte {
	return append([]byte(nil), src...)
}

// IsCorrupt reports whether err means stored data failed verification.
func IsCorrupt(err error) bool {
	return errors.Is(err, storage.ErrPageCorrupt) || errors.Is(err, storage.ErrSnapshotPage)
}
