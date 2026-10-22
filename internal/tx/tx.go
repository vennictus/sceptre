package tx

import (
	"errors"
	"sync"

	"github.com/vennictus/sceptre/internal/kv"
)

var (
	ErrClosed   = errors.New("tx: transaction is closed")
	ErrConflict = errors.New("tx: conflict detected")
)

type committedTx struct {
	version uint64
	writes  map[string]struct{}
	allKeys bool
}

// Manager coordinates snapshot transactions and serialized commits.
type Manager struct {
	store      *kv.KV
	mu         sync.Mutex
	version    uint64
	active     map[*Tx]uint64
	history    []committedTx
	generation uint64
}

type pendingMutation struct {
	mutation kv.Mutation
}

// Tx buffers KV writes until Commit publishes them atomically.
type Tx struct {
	store    *kv.KV
	manager  *Manager
	snapshot *kv.Snapshot
	err      error // set when the snapshot could not be taken
	version  uint64
	pending  map[string]pendingMutation
	reads    map[string]struct{}
	writes   map[string]struct{}
	order    []string
	closed   bool
	scanned  bool
}

// NewManager creates a transaction manager for a KV store.
func NewManager(store *kv.KV) *Manager {
	return &Manager{
		store:      store,
		active:     make(map[*Tx]uint64),
		generation: store.Generation(),
	}
}

// Begin starts a managed snapshot transaction.
func (m *Manager) Begin() *Tx {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.observeExternalCommit()

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
	snapshot, err := store.Snapshot()
	return &Tx{
		store:    store,
		snapshot: snapshot,
		err:      err,
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
	err := tx.store.Apply(mutations)
	if err == nil || errors.Is(err, kv.ErrCommitCommitted) || errors.Is(err, kv.ErrRecoveryRequired) {
		tx.close()
	}
	return err
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
	if tx.err != nil {
		return tx.err
	}
	return tx.store.Err()
}

func (m *Manager) commit(tx *Tx) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if tx.closed {
		return ErrClosed
	}
	m.observeExternalCommit()
	if m.detectConflict(tx) {
		delete(m.active, tx)
		tx.close()
		m.pruneHistory()
		return ErrConflict
	}

	mutations := make([]kv.Mutation, 0, len(tx.order))
	for _, key := range tx.order {
		mutations = append(mutations, tx.pending[key].mutation)
	}
	err := m.store.Apply(mutations)
	if err != nil && !errors.Is(err, kv.ErrCommitCommitted) {
		if errors.Is(err, kv.ErrRecoveryRequired) {
			delete(m.active, tx)
			tx.close()
			m.pruneHistory()
		}
		return err
	}

	m.version++
	m.generation = m.store.Generation()
	m.history = append(m.history, committedTx{
		version: m.version,
		writes:  cloneKeySet(tx.writes),
	})
	delete(m.active, tx)
	tx.close()
	m.pruneHistory()
	return err
}

func (m *Manager) abort(tx *Tx) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.active, tx)
	tx.close()
	m.pruneHistory()
}

// Serialized writes outside this manager cannot be attributed to specific keys.
// Conservatively invalidate older transactions instead of losing conflicts.
func (m *Manager) observeExternalCommit() {
	generation := m.store.Generation()
	if generation == m.generation {
		return
	}
	m.version++
	m.generation = generation
	m.history = append(m.history, committedTx{version: m.version, allKeys: true})
	m.pruneHistory()
}

func (m *Manager) pruneHistory() {
	oldest := m.version
	for _, version := range m.active {
		if version < oldest {
			oldest = version
		}
	}
	n := 0
	for n < len(m.history) && m.history[n].version <= oldest {
		n++
	}
	remaining := copy(m.history, m.history[n:])
	clear(m.history[remaining:])
	m.history = m.history[:remaining]
}

func (m *Manager) detectConflict(tx *Tx) bool {
	for i := len(m.history) - 1; i >= 0; i-- {
		committed := m.history[i]
		if committed.version <= tx.version {
			break
		}
		if committed.allKeys || (tx.scanned && len(committed.writes) > 0) || setsOverlap(tx.reads, committed.writes) || setsOverlap(tx.writes, committed.writes) {
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
	tx.snapshot.Release()
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
