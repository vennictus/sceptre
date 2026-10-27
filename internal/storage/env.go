package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/vennictus/sceptre/internal/pager"
)

var (
	ErrClosed          = errors.New("storage: environment is closed")
	ErrTxnClosed       = errors.New("storage: transaction is closed")
	ErrUpgradeRequired = errors.New("storage: file uses format v1; run sceptre upgrade")
	// ErrRecoveryRequired means a commit's outcome is unknown. The environment
	// rejects further work until it is closed and reopened.
	ErrRecoveryRequired = errors.New("storage: uncertain commit; close and reopen required")
	// ErrCommitCommitted means the commit is durable despite the reported error.
	ErrCommitCommitted = errors.New("storage: commit completed despite reported error")
)

// Commit stages, in order, for fault-injection hooks.
const (
	StagePagesWritten  = "pages-written"
	StagePagesSynced   = "pages-synced"
	StageMetaPublished = "meta-published"
)

// CommitStages lists the stages a commit hook observes.
func CommitStages() []string {
	return []string{StagePagesWritten, StagePagesSynced, StageMetaPublished}
}

// DefaultCacheBytes is the page cache size when Options.CacheBytes is zero.
const DefaultCacheBytes = 64 << 20

// dataTree is the directory name of the single keyspace used today. The
// directory lets tables and indexes get their own trees later.
var dataTree = []byte("data")

type Options struct {
	PageSize   uint32 // for new files; default pager.DefaultPageSize
	CacheBytes int
	IO         *pager.IOHooks
	// CommitHook runs after each commit stage; an error interrupts the commit
	// there, as a crash would. For tests.
	CommitHook func(stage string) error
}

// Env owns a v2 database file.
type Env struct {
	p       *pager.Pager
	l       layout
	cache   *pageCache
	hook    func(string) error
	writeMu sync.Mutex // one write transaction at a time

	mu      sync.Mutex // guards everything below
	state   snapshot
	free    *freeIndex // committed free pages
	chain   []uint64   // format v2 only: pages of the freelist chain, freed by the converting commit
	readers map[*ReadTxn]uint64
	failure error
	closed  atomic.Bool // written under mu; read without it on hot paths
}

// snapshot is one committed state.
type snapshot struct {
	txid      uint64
	dirRoot   uint64 // tree directory root
	dataRoot  uint64
	freeRoot  uint64 // free tree root (format v3)
	pageCount uint64
}

// Open opens or creates a database file. New files use format v3. A v2 file
// opens normally and converts to v3 with its first commit. A v1 file returns
// ErrUpgradeRequired.
func Open(path string, opts Options) (*Env, error) {
	if opts.PageSize != 0 {
		if _, err := newLayout(int(opts.PageSize)); err != nil {
			return nil, err
		}
	}
	p, err := pager.Open(path, pager.Options{PageSize: opts.PageSize, Format: pager.FormatV3, IO: opts.IO})
	if err != nil {
		return nil, err
	}
	env, err := openPager(p, opts)
	if err != nil {
		p.Close()
		return nil, err
	}
	return env, nil
}

func openPager(p *pager.Pager, opts Options) (*Env, error) {
	meta := p.Meta()
	if meta.Version != pager.FormatV2 && meta.Version != pager.FormatV3 {
		return nil, ErrUpgradeRequired
	}
	l, err := newLayout(int(meta.PageSize))
	if err != nil {
		return nil, err
	}
	cacheBytes := opts.CacheBytes
	if cacheBytes <= 0 {
		cacheBytes = DefaultCacheBytes
	}
	env := &Env{
		p:       p,
		l:       l,
		cache:   newPageCache(cacheBytes / l.pageSize),
		hook:    opts.CommitHook,
		readers: map[*ReadTxn]uint64{},
	}
	state := snapshot{txid: meta.Generation, dirRoot: meta.RootPage, pageCount: meta.PageCount}
	src := env.source(state)
	if state.dataRoot, err = readRoot(src, l, state.dirRoot, dataTree); err != nil {
		return nil, err
	}
	if state.freeRoot, err = readRoot(src, l, state.dirRoot, freeTree); err != nil {
		return nil, err
	}
	if meta.Version == pager.FormatV2 {
		if state.freeRoot != 0 {
			return nil, corrupt("format v2 file has a free tree")
		}
		free, chain, err := loadFreelist(src, state.pageCount, meta.FreeListPage)
		if err != nil {
			return nil, fmt.Errorf("storage: read freelist: %w", err)
		}
		env.free, env.chain = legacyFreeIndex(free, l), chain
	} else {
		if meta.FreeListPage != 0 {
			return nil, corrupt("format v3 meta points at a freelist chain page %d", meta.FreeListPage)
		}
		if env.free, err = loadFreeIndex(src, l, state.freeRoot, state.pageCount); err != nil {
			return nil, fmt.Errorf("storage: read free tree: %w", err)
		}
	}
	env.state = state
	return env, nil
}

// readRoot looks up a tree's root page in the directory; 0 means no tree.
func readRoot(src pageSource, l layout, dirRoot uint64, name []byte) (uint64, error) {
	if dirRoot == 0 {
		return 0, nil
	}
	v, ok, err := get(src, l, dirRoot, name)
	if err != nil {
		return 0, fmt.Errorf("storage: read tree directory: %w", err)
	}
	if !ok {
		return 0, nil
	}
	if len(v) != 8 {
		return 0, corrupt("directory entry for %q has %d bytes", name, len(v))
	}
	return binary.LittleEndian.Uint64(v), nil
}

// Close releases the file. Open transactions fail afterwards.
func (e *Env) Close() error {
	e.mu.Lock()
	if e.closed.Load() {
		e.mu.Unlock()
		return nil
	}
	e.closed.Store(true)
	e.mu.Unlock()
	return e.p.Close()
}

// Err reports an uncertain commit that requires reopening.
func (e *Env) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.failure
}

func (e *Env) usable() error {
	if e.closed.Load() {
		return ErrClosed
	}
	return e.failure
}

// TxID returns the transaction ID of the latest committed state. Unlike Info it
// does no other work, so it suits per-transaction callers.
func (e *Env) TxID() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state.txid
}

// PageSize returns the file's page size.
func (e *Env) PageSize() int { return e.l.pageSize }

// MaxKeySize returns the largest key the layout accepts.
func (e *Env) MaxKeySize() int { return e.l.maxKey }

// Pager exposes the file for inspection tools.
func (e *Env) Pager() *pager.Pager { return e.p }

type snapshotSource struct {
	e *Env
	s snapshot
}

func (e *Env) source(s snapshot) snapshotSource { return snapshotSource{e: e, s: s} }

// page reads a committed page of snapshot s, verifying it on a cache miss and
// rejecting any page written after s.
func (src snapshotSource) page(id uint64) ([]byte, error) {
	if id < pager.MetaPageCount || id >= src.s.pageCount {
		return nil, corrupt("page %d outside snapshot of %d pages", id, src.s.pageCount)
	}
	page, ok := src.e.cache.get(id)
	if !ok {
		buf := make([]byte, src.e.l.pageSize)
		if err := src.e.p.ReadPageInto(id, buf); err != nil {
			return nil, err
		}
		if err := verify(buf, id); err != nil {
			return nil, err
		}
		src.e.cache.put(id, buf)
		page = buf
	}
	if txIDOf(page) > src.s.txid {
		return nil, fmt.Errorf("%w: page %d written by transaction %d, snapshot %d", ErrSnapshotPage, id, txIDOf(page), src.s.txid)
	}
	return page, nil
}

// ReadTxn is a consistent snapshot. Readers never block the writer or each
// other. Release it so its pages can be reused.
type ReadTxn struct {
	e    *Env
	s    snapshot
	done atomic.Bool
}

// BeginRead takes a snapshot of the latest committed state.
func (e *Env) BeginRead() (*ReadTxn, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.usable(); err != nil {
		return nil, err
	}
	r := &ReadTxn{e: e, s: e.state}
	e.readers[r] = r.s.txid
	return r, nil
}

// TxID returns the snapshot's transaction ID.
func (r *ReadTxn) TxID() uint64 { return r.s.txid }

// Release ends the snapshot. Repeated calls are safe.
func (r *ReadTxn) Release() {
	r.e.mu.Lock()
	defer r.e.mu.Unlock()
	if !r.done.Swap(true) {
		delete(r.e.readers, r)
	}
}

// check runs on every read, so it uses atomic flags instead of the env mutex.
func (r *ReadTxn) check() error {
	if r.done.Load() {
		return ErrTxnClosed
	}
	if r.e.closed.Load() {
		return ErrClosed
	}
	return nil
}

// Get returns a copy of key's value.
func (r *ReadTxn) Get(key []byte) ([]byte, bool, error) {
	if err := r.check(); err != nil {
		return nil, false, err
	}
	return get(r.e.source(r.s), r.e.l, r.s.dataRoot, key)
}

// Cursor walks the snapshot in key order. It is valid until Release.
func (r *ReadTxn) Cursor() *Cursor {
	if err := r.check(); err != nil {
		return errCursor(err)
	}
	return newCursor(r.e.source(r.s), r.e.l, r.s.dataRoot)
}

// WriteTxn is the single writer. Its changes are private until Commit.
type WriteTxn struct {
	e        *Env
	base     snapshot
	w        *writer
	root     uint64
	limit    uint64         // pages freed at or before this txid are reusable
	poolKeys []string       // free records whose pages are reusable, oldest first
	poolPos  int            // the record being consumed
	taken    map[string]int // pages consumed from the front of each record
	consumed []uint64       // every committed-free page this transaction reused
	spare    []uint64       // pages this transaction allocated and released
	convert  bool           // format v2 file: this commit writes the free tree and v3 meta
	next     uint64         // next page ID at the end of the file
	done     bool
}

// BeginWrite waits for the writer slot and starts a transaction on the latest state.
func (e *Env) BeginWrite() (*WriteTxn, error) {
	e.writeMu.Lock()
	e.mu.Lock()
	if err := e.usable(); err != nil {
		e.mu.Unlock()
		e.writeMu.Unlock()
		return nil, err
	}
	base := e.state
	// A page freed by transaction T was live in every state before T. It is
	// reusable once no reader holds such a state and the fallback meta
	// (the state before the latest) no longer does.
	limit := uint64(0)
	if base.txid > 0 {
		limit = base.txid - 1
	}
	for _, s := range e.readers {
		if s < limit {
			limit = s
		}
	}
	poolKeys := e.free.eligible(limit)
	convert := e.chain != nil
	e.mu.Unlock()

	t := &WriteTxn{e: e, base: base, root: base.dataRoot, limit: limit, poolKeys: poolKeys, taken: map[string]int{}, convert: convert, next: base.pageCount}
	t.w = &writer{
		src:     e.source(base),
		l:       e.l,
		dirty:   map[uint64][]byte{},
		alloc:   t.alloc,
		recycle: func(id uint64) { t.spare = append(t.spare, id) },
	}
	return t, nil
}

func (t *WriteTxn) alloc() (uint64, error) {
	if n := len(t.spare); n > 0 {
		id := t.spare[n-1]
		t.spare = t.spare[:n-1]
		return id, nil
	}
	// Reuse the oldest free records first, so a commit trims few records.
	for t.poolPos < len(t.poolKeys) {
		k := t.poolKeys[t.poolPos]
		rec := t.e.free.recs[k] // stable: only commits change the index, and this txn holds the writer slot
		if n := t.taken[k]; n < len(rec.pages) {
			t.taken[k] = n + 1
			t.consumed = append(t.consumed, rec.pages[n])
			return rec.pages[n], nil
		}
		t.poolPos++
	}
	if t.next == math.MaxUint64 {
		return 0, errors.New("storage: page IDs exhausted")
	}
	id := t.next
	t.next++
	return id, nil
}

func (t *WriteTxn) check() error {
	if t.done {
		return ErrTxnClosed
	}
	t.e.mu.Lock()
	defer t.e.mu.Unlock()
	return t.e.usable()
}

// Get reads key, including this transaction's own changes.
func (t *WriteTxn) Get(key []byte) ([]byte, bool, error) {
	if err := t.check(); err != nil {
		return nil, false, err
	}
	return get(t.w, t.e.l, t.root, key)
}

// Put inserts or replaces key. On error the transaction must be aborted.
func (t *WriteTxn) Put(key, value []byte) error {
	if err := t.check(); err != nil {
		return err
	}
	root, err := t.w.put(t.root, key, value)
	if err != nil {
		return err
	}
	t.root = root
	return nil
}

// Delete removes key and reports whether it existed.
func (t *WriteTxn) Delete(key []byte) (bool, error) {
	if err := t.check(); err != nil {
		return false, err
	}
	root, found, err := t.w.del(t.root, key)
	if err != nil {
		return false, err
	}
	t.root = root
	return found, nil
}

// Cursor walks the transaction's current view. Writes invalidate it.
func (t *WriteTxn) Cursor() *Cursor {
	if err := t.check(); err != nil {
		return errCursor(err)
	}
	return newCursor(t.w, t.e.l, t.root)
}

// Abort discards the transaction. Repeated calls are safe.
func (t *WriteTxn) Abort() {
	if !t.done {
		t.done = true
		t.e.writeMu.Unlock()
	}
}

// Commit makes the transaction durable and visible, then ends it.
//
// An error wrapping ErrCommitCommitted means the commit is durable. An error
// wrapping ErrRecoveryRequired means the outcome is unknown until reopen. Any
// other error means nothing was published.
func (t *WriteTxn) Commit() error {
	if err := t.check(); err != nil {
		t.Abort()
		return err
	}
	defer t.Abort()
	if t.root == t.base.dataRoot && len(t.w.dirty) == 0 && len(t.w.freed) == 0 {
		return nil // nothing changed
	}
	e := t.e
	txid := t.base.txid + 1

	if t.convert {
		// The v2 freelist chain is replaced by the free tree in this commit.
		e.mu.Lock()
		t.w.freed = append(t.w.freed, e.chain...)
		e.mu.Unlock()
	}
	// Write the free tree (only what changed) and the directory.
	dirRoot, freeRoot, edits, err := t.settle(txid)
	if err != nil {
		return err
	}
	pages := t.w.dirty

	ids := make([]uint64, 0, len(pages))
	for id := range pages {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		seal(pages[id], id, txid)
		if err := e.p.WritePage(id, pages[id]); err != nil {
			return err
		}
	}
	if err := t.stage(StagePagesWritten); err != nil {
		return err
	}
	if err := e.p.Sync(); err != nil {
		return err
	}
	if err := t.stage(StagePagesSynced); err != nil {
		return err
	}

	meta := e.p.Meta()
	meta.RootPage = dirRoot
	meta.FreeListPage = 0 // format v3 keeps free pages in the free tree
	meta.PageCount = t.next
	if t.convert {
		if err := e.p.UpgradeFormat(pager.FormatV3); err != nil {
			return err
		}
	}
	if err := e.p.PublishMeta(meta); err != nil {
		e.mu.Lock()
		e.failure = fmt.Errorf("%w: %w", ErrRecoveryRequired, err)
		e.mu.Unlock()
		return e.failure
	}

	for _, id := range ids {
		e.cache.put(id, pages[id])
	}
	e.mu.Lock()
	e.state = snapshot{txid: e.p.Meta().Generation, dirRoot: dirRoot, dataRoot: t.root, freeRoot: freeRoot, pageCount: t.next}
	e.free.install(edits, t.consumed)
	e.chain = nil
	e.mu.Unlock()
	if err := t.stage(StageMetaPublished); err != nil {
		return fmt.Errorf("%w: %w", ErrCommitCommitted, err)
	}
	return nil
}

func (t *WriteTxn) stage(name string) error {
	if t.e.hook == nil {
		return nil
	}
	return t.e.hook(name)
}

// Info describes the latest committed state.
type Info struct {
	TxID          uint64
	PageSize      int
	PageCount     uint64
	DirectoryRoot uint64
	DataRoot      uint64
	FreePages     int // free pages
	FreelistPages int // format v2 only: pages of the freelist chain awaiting conversion
	Reusable      int // free pages no reader or fallback state still needs
	Readers       int
	OldestReader  uint64 // 0 when there are no readers
	Cache         CacheStats
}

// Info reports the latest committed state and engine activity.
func (e *Env) Info() Info {
	e.mu.Lock()
	defer e.mu.Unlock()
	info := Info{
		TxID: e.state.txid, PageSize: e.l.pageSize, PageCount: e.state.pageCount,
		DirectoryRoot: e.state.dirRoot, DataRoot: e.state.dataRoot,
		FreePages: len(e.free.pages), FreelistPages: len(e.chain), Readers: len(e.readers),
		Cache: e.cache.stats(),
	}
	limit := uint64(0)
	if e.state.txid > 0 {
		limit = e.state.txid - 1
	}
	for _, s := range e.readers {
		if info.OldestReader == 0 || s < info.OldestReader {
			info.OldestReader = s
		}
		if s < limit {
			limit = s
		}
	}
	info.Reusable = e.free.reusable(limit)
	return info
}
