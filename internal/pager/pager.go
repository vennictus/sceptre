package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	ErrInvalidPageSize = fmt.Errorf("pager: page size must be at least %d bytes", metaHeaderSize)
	ErrFileTooSmall    = errors.New("pager: file too small for meta pages")
)

// Options controls how the pager initializes a database file.
type Options struct {
	PageSize uint32
}

// Pager owns the database file and the currently selected meta page.
type Pager struct {
	path       string
	file       *os.File
	pageSize   uint32
	meta       Meta
	activeSlot int
}

// Open opens an existing pager file or initializes a new one.
func Open(path string, opts Options) (*Pager, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}

	pager := &Pager{
		path: path,
		file: file,
	}
	if err := pager.open(opts); err != nil {
		file.Close()
		return nil, err
	}
	return pager, nil
}

// Close closes the backing file.
func (p *Pager) Close() error {
	if p == nil || p.file == nil {
		return nil
	}
	return p.file.Close()
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

// ActiveMetaSlot returns which reserved meta page is currently active.
func (p *Pager) ActiveMetaSlot() int {
	return p.activeSlot
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
		return p.initialize(pageSize)
	}

	return p.load()
}

func (p *Pager) initialize(pageSize uint32) error {
	if pageSize < uint32(metaHeaderSize) {
		return ErrInvalidPageSize
	}

	current := Meta{
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
	if err := p.file.Sync(); err != nil {
		return err
	}

	p.pageSize = pageSize
	p.meta = current
	p.activeSlot = 1
	return nil
}

func (p *Pager) load() error {
	header := make([]byte, metaHeaderSize)
	if _, err := p.file.ReadAt(header, 0); err != nil && err != io.EOF {
		return err
	}

	pageSize := readPageSize(header)
	if pageSize < uint32(metaHeaderSize) {
		return ErrInvalidMetaPageSize
	}

	info, err := p.file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < int64(pageSize)*MetaPageCount {
		return ErrFileTooSmall
	}

	first, err := p.readMetaPage(0, pageSize)
	if err != nil {
		return err
	}
	second, err := p.readMetaPage(1, pageSize)
	if err != nil {
		return err
	}

	p.pageSize = pageSize
	if second.Generation >= first.Generation {
		p.meta = second
		p.activeSlot = 1
	} else {
		p.meta = first
		p.activeSlot = 0
	}
	return nil
}

func (p *Pager) writeMetaPage(slot int, meta Meta) error {
	page, err := meta.Encode()
	if err != nil {
		return err
	}
	_, err = p.file.WriteAt(page, p.metaOffset(slot, meta.PageSize))
	return err
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

func readPageSize(header []byte) uint32 {
	if len(header) < metaPageSizeOffset+4 {
		return 0
	}
	return binaryUint32(header[metaPageSizeOffset:])
}

func binaryUint32(src []byte) uint32 {
	return uint32(src[0]) |
		uint32(src[1])<<8 |
		uint32(src[2])<<16 |
		uint32(src[3])<<24
}
