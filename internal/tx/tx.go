package tx

import (
	"errors"
	"sceptre/internal/kv"
)

var ErrClosed = errors.New("tx: transaction is closed")

type pendingMutation struct {
	mutation kv.Mutation
}

// Tx buffers KV writes until Commit publishes them atomically.
type Tx struct {
	store   *kv.KV
	pending map[string]pendingMutation
	order   []string
	closed  bool
}

// Begin starts a transaction over the provided KV store.
func Begin(store *kv.KV) *Tx {
	return &Tx{
		store:   store,
		pending: make(map[string]pendingMutation),
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
	return tx.store.Get(key)
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
	mutations := make([]kv.Mutation, 0, len(tx.order))
	for _, key := range tx.order {
		mutations = append(mutations, tx.pending[key].mutation)
	}
	if err := tx.store.Apply(mutations); err != nil {
		return err
	}
	tx.closed = true
	tx.pending = nil
	tx.order = nil
	return nil
}

// Abort discards all buffered writes.
func (tx *Tx) Abort() {
	if tx == nil || tx.closed {
		return
	}
	tx.closed = true
	tx.pending = nil
	tx.order = nil
}

func (tx *Tx) record(mutation kv.Mutation) {
	key := string(mutation.Key)
	if _, exists := tx.pending[key]; !exists {
		tx.order = append(tx.order, key)
	}
	tx.pending[key] = pendingMutation{mutation: mutation}
}

func (tx *Tx) ensureOpen() error {
	if tx == nil || tx.closed || tx.store == nil {
		return ErrClosed
	}
	return nil
}
