package table

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/vennictus/sceptre/internal/kv"
)

// An auto-increment column keeps the largest value it has seen under a
// catalog key derived from the table prefix, so renames keep it.
var catalogSequenceStart = []byte{0x00, 'q'}

func sequenceKey(def TableDef) []byte {
	key := append([]byte(nil), catalogSequenceStart...)
	return binary.BigEndian.AppendUint32(key, def.Prefix)
}

// autoColumn returns the table's auto-increment column, if any.
func autoColumn(def TableDef) (Column, bool) {
	for _, c := range def.Columns {
		if c.AutoIncrement {
			return c, true
		}
	}
	return Column{}, false
}

func (db *DB) sequence(def TableDef) (int64, error) {
	v, ok, err := db.store.Get(sequenceKey(def))
	if err != nil || !ok {
		return 0, err
	}
	if len(v) != 8 {
		return 0, ErrCorruptRecord
	}
	return int64(binary.BigEndian.Uint64(v)), nil
}

// NextIDs reserves nothing; it returns the next n values of the table's
// auto-increment column. Inserting rows with them advances the sequence.
func (db *DB) NextIDs(tableName string, n int) ([]int64, error) {
	def, err := db.mustTable(tableName)
	if err != nil {
		return nil, err
	}
	if _, ok := autoColumn(def); !ok {
		return nil, fmt.Errorf("%w: table %s has no AUTOINCREMENT column", ErrInvalidTableDef, tableName)
	}
	last, err := db.sequence(def)
	if err != nil {
		return nil, err
	}
	if last > math.MaxInt64-int64(n) {
		return nil, fmt.Errorf("%w: AUTOINCREMENT sequence exhausted", ErrInvalidValue)
	}
	out := make([]int64, n)
	for i := range out {
		out[i] = last + int64(i) + 1
	}
	return out, nil
}

// advanceSequence returns a mutation raising the sequence to the largest
// auto-increment value among records, or false when it does not move.
func (db *DB) advanceSequence(def TableDef, records []Record) (kv.Mutation, bool, error) {
	column, ok := autoColumn(def)
	if !ok {
		return kv.Mutation{}, false, nil
	}
	last, err := db.sequence(def)
	if err != nil {
		return kv.Mutation{}, false, err
	}
	top := last
	for _, r := range records {
		if v := r.Values[column.Name]; v.Type == TypeInt64 && v.I64 > top {
			top = v.I64
		}
	}
	if top == last {
		return kv.Mutation{}, false, nil
	}
	return kv.Put(sequenceKey(def), binary.BigEndian.AppendUint64(nil, uint64(top))), true, nil
}
