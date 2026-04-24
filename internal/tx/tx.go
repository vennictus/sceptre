package tx

import (
	"errors"
	"sceptre/internal/btree"
	"sceptre/internal/kv"
	"sync"
)

var (
	ErrClosed   = errors.New("tx: transaction is closed")
	ErrConflict = errors.New("tx: conflict detected")
)

type committedTx struct {
	version uint64
	writes  map[string]struct{}
}

// Manager coordinates snapshot transactions and serialized commits.
type Manager struct {
	store   *kv.KV
	mu      sync.Mutex
	version uint64
	active  map[*Tx]uint64
	history []committedTx
}

type pendingMutation struct {
	mutation kv.Mutation
}

// Tx buffers KV writes until Commit publishes them atomically.
type Tx struct {
	store    *kv.KV
	manager  *Manager
	snapshot *btree.Tree
	version  uint64
	pending  map[string]pendingMutation
	reads    map[string]struct{}
	writes   map[string]struct{}
	order    []string
	closed   bool
}

// NewManager creates a transaction manager for a KV store.
func NewManager(store *kv.KV) *Manager {
	return &Manager{
		store:  store,
		active: make(map[*Tx]uint64),
	}
}

// Begin starts a managed snapshot transaction.
func (m *Manager) Begin() *Tx {
	m.mu.Lock()
	defer m.mu.Unlock()

	tx := beginSnapshot(m.store)
	tx.manager = m
	tx.version = m.version
	m.active[tx] = tx.version
	return tx
}

// ActiveSnapshots returns the versions still held by open transactions.
func (m *Manager) ActiveSnapshots() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	versions := make([]uint64, 0, len(m.active))
	for _, version := range m.active {
		versions = append(versions, version)
	}
	return versions
}

// Version returns the latest committed manager version.
func (m *Manager) Version() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.version
}

// Begin starts a transaction over the provided KV store.
func Begin(store *kv.KV) *Tx {
	return beginSnapshot(store)
}

func beginSnapshot(store *kv.KV) *Tx {
	snapshot := store.Tree().Snapshot()
	tree, _ := btree.NewTreeFromSnapshot(int(store.Pager().PageSize()), snapshot)
	return &Tx{
		store:    store,
		snapshot: tree,
		pending:  make(map[string]pendingMutation),
		reads:    make(map[string]struct{}),
		writes:   make(map[string]struct{}),
	}
}

// Get returns a key while honoring this transaction's pending writes.
func (tx *Tx) Get(key []byte) ([]byte, bool, error) {
	if err := tx.ensureOpen(); err != nil {
		return nil, false, err
	}
	if pending, ok := tx.pending[string(key)]; ok {
		if pending.mutation.Delete {
			return nil, false, nil
		}
		return append([]byte(nil), pending.mutation.Value...), true, nil
	}
	tx.reads[string(key)] = struct{}{}
	return tx.snapshot.Get(key)
}

// Set buffers a key/value write.
func (tx *Tx) Set(key, value []byte) error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	tx.record(kv.Put(key, value))
	return nil
}

// Del buffers a key delete and reports whether the transaction could see it.
func (tx *Tx) Del(key []byte) (bool, error) {
	if err := tx.ensureOpen(); err != nil {
		return false, err
	}
	_, ok, err := tx.Get(key)
	if err != nil {
		return false, err
	}
	tx.record(kv.Delete(key))
	return ok, nil
}

// Commit publishes all buffered writes through one KV Apply call.
func (tx *Tx) Commit() error {
	if err := tx.ensureOpen(); err != nil {
		return err
	}
	if tx.manager != nil {
		return tx.manager.commit(tx)
	}
	mutations := make([]kv.Mutation, 0, len(tx.order))
	for _, key := range tx.order {
		mutations = append(mutations, tx.pending[key].mutation)
	}
	if err := tx.store.Apply(mutations); err != nil {
		return err
	}
	tx.close()
	return nil
}

// Abort discards all buffered writes.
func (tx *Tx) Abort() {
	if tx == nil || tx.closed {
		return
	}
	if tx.manager != nil {
		tx.manager.abort(tx)
		return
	}
	tx.close()
}

func (tx *Tx) record(mutation kv.Mutation) {
	key := string(mutation.Key)
	if _, exists := tx.pending[key]; !exists {
		tx.order = append(tx.order, key)
	}
	tx.pending[key] = pendingMutation{mutation: mutation}
	tx.writes[key] = struct{}{}
}

func (tx *Tx) ensureOpen() error {
	if tx == nil || tx.closed || tx.store == nil {
		return ErrClosed
	}
	return nil
}

func (m *Manager) commit(tx *Tx) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tx.closed {
		return ErrClosed
	}
	if m.detectConflict(tx) {
		delete(m.active, tx)
		tx.close()
		return ErrConflict
	}

	mutations := make([]kv.Mutation, 0, len(tx.order))
	for _, key := range tx.order {
		mutations = append(mutations, tx.pending[key].mutation)
	}
	if err := m.store.Apply(mutations); err != nil {
		return err
	}

	m.version++
	m.history = append(m.history, committedTx{
		version: m.version,
		writes:  cloneKeySet(tx.writes),
	})
	delete(m.active, tx)
	tx.close()
	return nil
}

func (m *Manager) abort(tx *Tx) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.active, tx)
	tx.close()
}

func (m *Manager) detectConflict(tx *Tx) bool {
	for i := len(m.history) - 1; i >= 0; i-- {
		committed := m.history[i]
		if committed.version <= tx.version {
			break
		}
		if setsOverlap(tx.reads, committed.writes) || setsOverlap(tx.writes, committed.writes) {
			return true
		}
	}
	return false
}

func (tx *Tx) close() {
	tx.closed = true
	tx.pending = nil
	tx.reads = nil
	tx.writes = nil
	tx.order = nil
	tx.snapshot = nil
}

func setsOverlap(left, right map[string]struct{}) bool {
	for key := range left {
		if _, ok := right[key]; ok {
			return true
		}
	}
	return false
}

func cloneKeySet(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for key := range in {
		out[key] = struct{}{}
	}
	return out
}
