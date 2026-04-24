package table

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var ErrScannerInvalid = errors.New("table: scanner is not positioned on a row")

// Bound describes one inclusive or exclusive primary-key scan boundary.
type Bound struct {
	Key       Record
	Inclusive bool
}

// ScanBounds constrains a table scan over primary-key order.
type ScanBounds struct {
	Lower *Bound
	Upper *Bound
}

// Inclusive returns an inclusive scan bound for key.
func Inclusive(key Record) *Bound {
	return &Bound{Key: key, Inclusive: true}
}

// Exclusive returns an exclusive scan bound for key.
func Exclusive(key Record) *Bound {
	return &Bound{Key: key}
}

// Scanner iterates table rows in primary-key order.
type Scanner struct {
	def            TableDef
	iter           interfaceIterator
	upper          []byte
	upperInclusive bool
	current        Record
	valid          bool
}

type interfaceIterator interface {
	Valid() bool
	SeekGE([]byte) error
	Next() error
	Deref() ([]byte, []byte, error)
}

// Scan creates a scanner over a table's primary-key range.
func (db *DB) Scan(tableName string, bounds ScanBounds) (*Scanner, error) {
	def, err := db.mustTable(tableName)
	if err != nil {
		return nil, err
	}

	lower := tableKeyStart(def)
	lowerInclusive := true
	if bounds.Lower != nil {
		if err := validateKeyRecord(def, bounds.Lower.Key); err != nil {
			return nil, err
		}
		lower, err = encodeRowKey(def, bounds.Lower.Key)
		if err != nil {
			return nil, err
		}
		lowerInclusive = bounds.Lower.Inclusive
	}

	upper := tableKeyEnd(def)
	upperInclusive := false
	if bounds.Upper != nil {
		if err := validateKeyRecord(def, bounds.Upper.Key); err != nil {
			return nil, err
		}
		upper, err = encodeRowKey(def, bounds.Upper.Key)
		if err != nil {
			return nil, err
		}
		upperInclusive = bounds.Upper.Inclusive
	}

	iter := db.kv.Iterator()
	if err := iter.SeekGE(lower); err != nil {
		return nil, err
	}
	if !lowerInclusive && iter.Valid() {
		key, _, err := iter.Deref()
		if err != nil {
			return nil, err
		}
		if bytes.Equal(key, lower) {
			if err := iter.Next(); err != nil {
				return nil, err
			}
		}
	}

	scanner := &Scanner{
		def:            def,
		iter:           iter,
		upper:          upper,
		upperInclusive: upperInclusive,
	}
	if err := scanner.advance(); err != nil {
		return nil, err
	}
	return scanner, nil
}

// Valid reports whether the scanner is positioned on a row.
func (s *Scanner) Valid() bool {
	return s != nil && s.valid
}

// Next advances the scanner to the next row.
func (s *Scanner) Next() error {
	if !s.Valid() {
		return nil
	}
	if err := s.iter.Next(); err != nil {
		return err
	}
	return s.advance()
}

// Deref returns the current row.
func (s *Scanner) Deref() (Record, error) {
	if !s.Valid() {
		return Record{}, ErrScannerInvalid
	}
	return NewRecord(s.current.Values), nil
}

func (s *Scanner) advance() error {
	s.valid = false
	for s.iter.Valid() {
		key, value, err := s.iter.Deref()
		if err != nil {
			return err
		}
		cmp := bytes.Compare(key, s.upper)
		if cmp > 0 || (cmp == 0 && !s.upperInclusive) {
			return nil
		}

		keyRecord, err := decodeRowKey(s.def, key)
		if err != nil {
			return err
		}
		row, err := decodeRowValue(s.def, keyRecord, value)
		if err != nil {
			return err
		}
		s.current = row
		s.valid = true
		return nil
	}
	return nil
}

func tableKeyStart(def TableDef) []byte {
	out := []byte{rowKeyPrefix, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[1:], def.Prefix)
	return out
}

func tableKeyEnd(def TableDef) []byte {
	end := tableKeyStart(def)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return []byte{0xFF}
}

func decodeRowKey(def TableDef, key []byte) (Record, error) {
	prefix := tableKeyStart(def)
	if !bytes.HasPrefix(key, prefix) {
		return Record{}, ErrCorruptRecord
	}

	values := make(map[string]Value, len(def.PrimaryKey))
	remaining := key[len(prefix):]
	for _, name := range def.PrimaryKey {
		valueType, _ := columnType(def, name)
		value, consumed, err := consumeEncodedValue(remaining, valueType)
		if err != nil {
			return Record{}, err
		}
		values[name] = value
		remaining = remaining[consumed:]
	}
	if len(remaining) != 0 {
		return Record{}, ErrCorruptRecord
	}
	return Record{Values: values}, nil
}
