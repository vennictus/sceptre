package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

var (
	ErrInvalidPageSize   = fmt.Errorf("pager: page size must be at least %d bytes", metaHeaderSize)
	ErrFileTooSmall      = errors.New("pager: file too small for meta pages")
	ErrNoValidMetaPage   = errors.New("pager: no valid meta page found")
	ErrDatabaseLocked    = errors.New("pager: database is already open")
	ErrReservedPageID    = errors.New("pager: page id is reserved for meta pages")
	ErrPageNotAllocated  = errors.New("pager: page id not allocated")
	ErrInvalidPageBuffer = errors.New("pager: page buffer size mismatch")
	ErrMetaPageSize      = errors.New("pager: meta page size does not match pager")
	ErrShortPageRead     = errors.New("pager: short page read")
	ErrCommitUncertain   = errors.New("pager: metadata publication outcome is uncertain; reopen required")
	ErrInvalidFormat     = errors.New("pager: invalid format version")
)

// Options controls how the pager initializes a database file.
type Options struct {
	PageSize uint32
	// Format selects the layout of a new file (0 means FormatVersion). An
	// existing file keeps its own format.
	Format uint32
	IO     *IOHooks
}

// IOHooks provides an optional physical I/O seam for storage-error tests.
// Nil functions use real file operations. Hooks must not change during use.
type IOHooks struct {
	WriteAt func(*os.File, []byte, int64) (int, error)
	Sync    func(*os.File) error
}

// Pager owns the database file and the currently selected meta page.
type Pager struct {
	path       string
	file       *os.File
	pageSize   uint32
	meta       Meta
	activeSlot int
	io         *IOHooks
}

// Open opens an existing pager file or initializes a new one.
func Open(path string, opts Options) (*Pager, error) {
	if opts.Format != 0 && opts.Format != FormatV1 && opts.Format != FormatV2 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidFormat, opts.Format)
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}
	if err := acquireOwnership(file); err != nil {
		file.Close()
		return nil, err
	}

	pager := &Pager{
		path: path,
		file: file,
		io:   opts.IO,
	}
	if err := pager.open(opts); err != nil {
		file.Close()
		return nil, err
	}
	return pager, nil
}

// ownershipWait bounds how long Open retries a held lock before reporting it.
// A lock can stay held briefly after its owner closes: a subprocess started
// by any goroutine inherits open descriptors until it execs, so it shares the
// lock for that moment. A real second owner still gets ErrDatabaseLocked.
var ownershipWait = 500 * time.Millisecond

func acquireOwnership(file *os.File) error {
	deadline := time.Now().Add(ownershipWait)
	delay := time.Millisecond
	for {
		err := lockDatabase(file)
		if !errors.Is(err, ErrDatabaseLocked) || !time.Now().Before(deadline) {
			return err
		}
		time.Sleep(delay)
		if delay < 50*time.Millisecond {
			delay *= 2
		}
	}
}

// Close closes the backing file.
func (p *Pager) Close() error {
	if p == nil || p.file == nil {
		return nil
	}
	fileErr := p.file.Close()
	p.file = nil
	return fileErr
}

// Path returns the filesystem path backing the pager.
func (p *Pager) Path() string {
	return p.path
}

// PageSize returns the configured page size.
func (p *Pager) PageSize() uint32 {
	return p.pageSize
}

// Meta returns the active meta payload.
func (p *Pager) Meta() Meta {
	return p.meta
}

// PreviousMeta returns the other valid metadata slot, if one exists.
// Its tree and freelist pages must not be reused before the next publication.
func (p *Pager) PreviousMeta() (Meta, bool) {
	meta, err := p.readMetaPage(1-p.activeSlot, p.pageSize)
	return meta, err == nil
}

// ActiveMetaSlot returns which reserved meta page is currently active.
func (p *Pager) ActiveMetaSlot() int {
	return p.activeSlot
}

// Sync flushes file contents to durable storage.
func (p *Pager) Sync() error {
	if p.io != nil && p.io.Sync != nil {
		return p.io.Sync(p.file)
	}
	return p.file.Sync()
}

func (p *Pager) writeAt(data []byte, offset int64) error {
	var n int
	var err error
	if p.io != nil && p.io.WriteAt != nil {
		n, err = p.io.WriteAt(p.file, data, offset)
	} else {
		n, err = p.file.WriteAt(data, offset)
	}
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// ReadPage reads a full non-meta page by page ID.
func (p *Pager) ReadPage(pageID uint64) ([]byte, error) {
	if pageID < MetaPageCount {
		return nil, ErrReservedPageID
	}
	if pageID >= p.meta.PageCount {
		return nil, ErrPageNotAllocated
	}

	page := make([]byte, p.pageSize)
	n, err := p.file.ReadAt(page, p.pageOffset(pageID))
	if err != nil {
		if errors.Is(err, io.EOF) && n != len(page) {
			return nil, fmt.Errorf("%w: page %d", ErrShortPageRead, pageID)
		}
		return nil, err
	}
	if n != len(page) {
		return nil, fmt.Errorf("%w: page %d", ErrShortPageRead, pageID)
	}
	return page, nil
}

// WritePage writes a full non-meta page by page ID.
func (p *Pager) WritePage(pageID uint64, page []byte) error {
	if pageID < MetaPageCount {
		return ErrReservedPageID
	}
	if len(page) != int(p.pageSize) {
		return ErrInvalidPageBuffer
	}

	return p.writeAt(page, p.pageOffset(pageID))
}

// PublishMeta writes the next meta page and makes it active in memory.
func (p *Pager) PublishMeta(meta Meta) error {
	if meta.PageSize != p.pageSize {
		return ErrMetaPageSize
	}
	meta.Version = p.meta.Version
	meta.Generation = p.meta.Generation + 1

	slot := 1 - p.activeSlot
	if err := p.writeMetaPage(slot, meta); err != nil {
		return fmt.Errorf("%w: %w", ErrCommitUncertain, err)
	}
	if err := p.Sync(); err != nil {
		return fmt.Errorf("%w: %w", ErrCommitUncertain, err)
	}

	p.meta = meta
	p.activeSlot = slot
	return nil
}

func (p *Pager) open(opts Options) error {
	info, err := p.file.Stat()
	if err != nil {
		return err
	}

	if info.Size() == 0 {
		pageSize := opts.PageSize
		if pageSize == 0 {
			pageSize = DefaultPageSize
		}
		format := opts.Format
		if format == 0 {
			format = FormatVersion
		}
		return p.initialize(pageSize, format)
	}

	return p.load(opts)
}

func (p *Pager) initialize(pageSize, format uint32) error {
	if pageSize < uint32(metaHeaderSize) {
		return ErrInvalidPageSize
	}

	current := Meta{
		Version:    format,
		PageSize:   pageSize,
		PageCount:  uint64(MetaPageCount),
		Generation: 1,
	}
	previous := current
	previous.Generation = 0

	if err := p.writeMetaPage(0, previous); err != nil {
		return err
	}
	if err := p.writeMetaPage(1, current); err != nil {
		return err
	}
	if err := p.Sync(); err != nil {
		return err
	}

	p.pageSize = pageSize
	p.meta = current
	p.activeSlot = 1
	return nil
}

func (p *Pager) load(opts Options) error {
	header := make([]byte, metaHeaderSize)
	if _, err := p.file.ReadAt(header, 0); err != nil && err != io.EOF {
		return err
	}

	info, err := p.file.Stat()
	if err != nil {
		return err
	}

	for _, pageSize := range candidatePageSizes(header, opts.PageSize, info.Size()) {
		first, err := p.readMetaPage(0, pageSize)
		firstOK := err == nil
		second, err := p.readMetaPage(1, pageSize)
		secondOK := err == nil

		switch {
		case firstOK && secondOK && second.Generation >= first.Generation:
			p.pageSize = pageSize
			p.meta = second
			p.activeSlot = 1
			return nil
		case firstOK && secondOK:
			p.pageSize = pageSize
			p.meta = first
			p.activeSlot = 0
			return nil
		case secondOK:
			p.pageSize = pageSize
			p.meta = second
			p.activeSlot = 1
			return nil
		case firstOK:
			p.pageSize = pageSize
			p.meta = first
			p.activeSlot = 0
			return nil
		}
	}

	if info.Size() < int64(metaHeaderSize)*MetaPageCount {
		return ErrFileTooSmall
	}
	return ErrNoValidMetaPage
}

func (p *Pager) writeMetaPage(slot int, meta Meta) error {
	page, err := meta.Encode()
	if err != nil {
		return err
	}
	return p.writeAt(page, p.metaOffset(slot, meta.PageSize))
}

func (p *Pager) readMetaPage(slot int, pageSize uint32) (Meta, error) {
	page := make([]byte, pageSize)
	if _, err := p.file.ReadAt(page, p.metaOffset(slot, pageSize)); err != nil && err != io.EOF {
		return Meta{}, err
	}
	return DecodeMeta(page)
}

func (p *Pager) metaOffset(slot int, pageSize uint32) int64 {
	return int64(slot) * int64(pageSize)
}

func (p *Pager) pageOffset(pageID uint64) int64 {
	return int64(pageID) * int64(p.pageSize)
}

func readPageSize(header []byte) uint32 {
	if len(header) < metaPageSizeOffset+4 {
		return 0
	}
	return binaryUint32(header[metaPageSizeOffset:])
}

func candidatePageSizes(header []byte, requested uint32, fileSize int64) []uint32 {
	seen := make(map[uint32]struct{})
	var candidates []uint32
	add := func(pageSize uint32) {
		if pageSize < uint32(metaHeaderSize) || int64(pageSize)*MetaPageCount > fileSize {
			return
		}
		if _, ok := seen[pageSize]; ok {
			return
		}
		seen[pageSize] = struct{}{}
		candidates = append(candidates, pageSize)
	}

	add(requested)
	add(readPageSize(header))
	for pageSize := uint32(512); pageSize <= 65536; pageSize *= 2 {
		add(pageSize)
	}

	return candidates
}

func binaryUint32(src []byte) uint32 {
	return uint32(src[0]) |
		uint32(src[1])<<8 |
		uint32(src[2])<<16 |
		uint32(src[3])<<24
}
