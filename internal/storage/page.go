package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Every non-meta page starts with this header:
//
//	0  uint32  CRC32C of bytes 4..pageSize
//	4  uint8   page type
//	5  uint8   flags (0)
//	6  uint16  reserved (0)
//	8  uint64  page ID
//	16 uint64  ID of the transaction that wrote the page
const (
	pageHeaderSize = 24

	offChecksum = 0
	offType     = 4
	offPageID   = 8
	offTxID     = 16
)

type pageType uint8

const (
	pageBranch   pageType = 1
	pageLeaf     pageType = 2
	pageOverflow pageType = 3
	pageFreelist pageType = 4
)

func (t pageType) String() string {
	switch t {
	case pageBranch:
		return "branch"
	case pageLeaf:
		return "leaf"
	case pageOverflow:
		return "overflow"
	case pageFreelist:
		return "freelist"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

var (
	// ErrPageCorrupt reports a page that failed verification. It is never served.
	ErrPageCorrupt = errors.New("storage: page corrupt")
	// ErrSnapshotPage reports a page written after the snapshot reading it,
	// which means a page the snapshot needed was reused.
	ErrSnapshotPage = errors.New("storage: page is newer than the snapshot reading it")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// newPage returns a zeroed page buffer with its type set.
func newPage(pageSize int, typ pageType) []byte {
	page := make([]byte, pageSize)
	page[offType] = byte(typ)
	return page
}

func typeOf(page []byte) pageType { return pageType(page[offType]) }

func txIDOf(page []byte) uint64 { return binary.LittleEndian.Uint64(page[offTxID:]) }

// seal stamps the page ID and transaction ID and computes the checksum.
// It is the last change before a page is written.
func seal(page []byte, id, txid uint64) {
	binary.LittleEndian.PutUint64(page[offPageID:], id)
	binary.LittleEndian.PutUint64(page[offTxID:], txid)
	binary.LittleEndian.PutUint32(page[offChecksum:], crc32.Checksum(page[offChecksum+4:], castagnoli))
}

// verify checks a page read from disk: checksum, location, and type.
func verify(page []byte, id uint64) error {
	if got, want := binary.LittleEndian.Uint32(page[offChecksum:]), crc32.Checksum(page[offChecksum+4:], castagnoli); got != want {
		return fmt.Errorf("%w: page %d checksum %08x, want %08x", ErrPageCorrupt, id, got, want)
	}
	if got := binary.LittleEndian.Uint64(page[offPageID:]); got != id {
		return fmt.Errorf("%w: page %d holds page %d", ErrPageCorrupt, id, got)
	}
	switch typeOf(page) {
	case pageBranch, pageLeaf, pageOverflow, pageFreelist:
		return nil
	default:
		return fmt.Errorf("%w: page %d has unknown type %d", ErrPageCorrupt, id, page[offType])
	}
}
