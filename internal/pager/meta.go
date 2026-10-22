package pager

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	DefaultPageSize uint32 = 4096
	MetaPageCount          = 2

	// FormatV1 is the layout of the in-memory engine; FormatV2 is the layout of
	// the disk-based copy-on-write engine. Both share this meta page encoding.
	FormatV1 uint32 = 1
	FormatV2 uint32 = 2
	// FormatVersion is the format used for new files when none is requested.
	FormatVersion = FormatV1

	metaMagicSize      = 8
	metaChecksumOffset = 48
	metaHeaderSize     = 56
	metaMagic          = "SCEPTRE\x00"
	metaVersionOffset  = 8
	metaPageSizeOffset = 12
	metaRootOffset     = 16
	metaFreeListOffset = 24
	metaPageCountOff   = 32
	metaGenerationOff  = 40
)

var (
	ErrMetaPageTooSmall     = errors.New("pager: meta page too small")
	ErrInvalidMetaMagic     = errors.New("pager: invalid meta magic")
	ErrUnsupportedVersion   = errors.New("pager: unsupported format version")
	ErrInvalidMetaPageSize  = errors.New("pager: invalid meta page size")
	ErrMetaChecksumMismatch = errors.New("pager: meta checksum mismatch")
)

// Meta describes the durable root state stored in the reserved meta pages.
type Meta struct {
	Version      uint32 // file format; 0 means FormatVersion when encoding
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
	version := m.Version
	if version == 0 {
		version = FormatVersion
	}
	putUint32(page[metaVersionOffset:], version)
	putUint32(page[metaPageSizeOffset:], m.PageSize)
	putUint64(page[metaRootOffset:], m.RootPage)
	putUint64(page[metaFreeListOffset:], m.FreeListPage)
	putUint64(page[metaPageCountOff:], m.PageCount)
	putUint64(page[metaGenerationOff:], m.Generation)
	putUint32(page[metaChecksumOffset:], metaChecksum(page))
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
	if version != FormatV1 && version != FormatV2 {
		return Meta{}, fmt.Errorf("%w: %d", ErrUnsupportedVersion, version)
	}

	pageSize := binary.LittleEndian.Uint32(page[metaPageSizeOffset:])
	if pageSize < uint32(metaHeaderSize) || pageSize != uint32(len(page)) {
		return Meta{}, ErrInvalidMetaPageSize
	}
	if got, want := binary.LittleEndian.Uint32(page[metaChecksumOffset:]), metaChecksum(page); got != want {
		return Meta{}, ErrMetaChecksumMismatch
	}

	return Meta{
		Version:      version,
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

func metaChecksum(page []byte) uint32 {
	h := crc32.NewIEEE()
	h.Write(page[:metaChecksumOffset])
	var zero [4]byte
	h.Write(zero[:])
	h.Write(page[metaChecksumOffset+4:])
	return h.Sum32()
}
