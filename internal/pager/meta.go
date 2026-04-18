package pager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	DefaultPageSize uint32 = 4096
	FormatVersion   uint32 = 1
	MetaPageCount          = 2

	metaMagicSize      = 8
	metaHeaderSize     = 48
	metaMagic          = "SCEPTRE\x00"
	metaVersionOffset  = 8
	metaPageSizeOffset = 12
	metaRootOffset     = 16
	metaFreeListOffset = 24
	metaPageCountOff   = 32
	metaGenerationOff  = 40
)

var (
	ErrMetaPageTooSmall    = errors.New("pager: meta page too small")
	ErrInvalidMetaMagic    = errors.New("pager: invalid meta magic")
	ErrUnsupportedVersion  = errors.New("pager: unsupported format version")
	ErrInvalidMetaPageSize = errors.New("pager: invalid meta page size")
)

// Meta describes the durable root state stored in the reserved meta pages.
type Meta struct {
	PageSize     uint32
	RootPage     uint64
	FreeListPage uint64
	PageCount    uint64
	Generation   uint64
}

// Encode serializes the meta payload into a full page-sized buffer.
func (m Meta) Encode() ([]byte, error) {
	if m.PageSize < uint32(metaHeaderSize) {
		return nil, ErrInvalidMetaPageSize
	}

	page := make([]byte, m.PageSize)
	copy(page[:metaMagicSize], metaMagic)
	putUint32(page[metaVersionOffset:], FormatVersion)
	putUint32(page[metaPageSizeOffset:], m.PageSize)
	putUint64(page[metaRootOffset:], m.RootPage)
	putUint64(page[metaFreeListOffset:], m.FreeListPage)
	putUint64(page[metaPageCountOff:], m.PageCount)
	putUint64(page[metaGenerationOff:], m.Generation)
	return page, nil
}

// DecodeMeta parses a meta page.
func DecodeMeta(page []byte) (Meta, error) {
	if len(page) < metaHeaderSize {
		return Meta{}, ErrMetaPageTooSmall
	}
	if !bytes.Equal(page[:metaMagicSize], []byte(metaMagic)) {
		return Meta{}, ErrInvalidMetaMagic
	}

	version := binary.LittleEndian.Uint32(page[metaVersionOffset:])
	if version != FormatVersion {
		return Meta{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}

	pageSize := binary.LittleEndian.Uint32(page[metaPageSizeOffset:])
	if pageSize < uint32(metaHeaderSize) || pageSize != uint32(len(page)) {
		return Meta{}, ErrInvalidMetaPageSize
	}

	return Meta{
		PageSize:     pageSize,
		RootPage:     binary.LittleEndian.Uint64(page[metaRootOffset:]),
		FreeListPage: binary.LittleEndian.Uint64(page[metaFreeListOffset:]),
		PageCount:    binary.LittleEndian.Uint64(page[metaPageCountOff:]),
		Generation:   binary.LittleEndian.Uint64(page[metaGenerationOff:]),
	}, nil
}

func putUint32(dst []byte, value uint32) {
	binary.LittleEndian.PutUint32(dst, value)
}

func putUint64(dst []byte, value uint64) {
	binary.LittleEndian.PutUint64(dst, value)
}
