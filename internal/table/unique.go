package table

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrUniqueViolation reports two rows with the same non-NULL values in a
// unique index.
var ErrUniqueViolation = errors.New("table: unique constraint violated")

// uniquePrefix returns the index key prefix holding record's values for a
// unique index, or false when any of them is NULL: NULLs never conflict.
func uniquePrefix(def TableDef, index IndexDef, record Record) ([]byte, bool) {
	out := indexKeyStart(index)
	for _, name := range index.Columns {
		v := record.Values[name]
		if v.IsNull() {
			return nil, false
		}
		out = appendColumnValue(out, v, def.tagged())
	}
	return out, true
}

// checkUnique verifies that writing records (each a full row; the batch
// replaces any stored rows with the same primary keys) keeps every unique
// index unique. Stored rows outside the batch keep their values.
func (db *DB) checkUnique(def TableDef, records []Record) error {
	var unique []IndexDef
	for _, index := range def.Indexes {
		if index.Unique {
			unique = append(unique, index)
		}
	}
	if len(unique) == 0 {
		return nil
	}
	batch := make(map[string]struct{}, len(records))
	rowKeys := make([]string, len(records))
	for i, record := range records {
		key, err := encodeRowKey(def, primaryKeyRecord(def, record))
		if err != nil {
			return err
		}
		rowKeys[i] = string(key)
		batch[string(key)] = struct{}{}
	}
	for _, index := range unique {
		claimed := make(map[string]string, len(records))
		for i, record := range records {
			prefix, ok := uniquePrefix(def, index, record)
			if !ok {
				continue
			}
			if other, dup := claimed[string(prefix)]; dup && other != rowKeys[i] {
				return fmt.Errorf("%w: %s", ErrUniqueViolation, index.Name)
			}
			claimed[string(prefix)] = rowKeys[i]
			holder, err := db.uniqueHolder(def, index, prefix, batch)
			if err != nil {
				return err
			}
			if holder != "" && holder != rowKeys[i] {
				return fmt.Errorf("%w: %s", ErrUniqueViolation, index.Name)
			}
		}
	}
	return nil
}

// uniqueHolder returns the row key of a stored row outside the batch whose
// index entry starts with prefix, or "" if there is none.
func (db *DB) uniqueHolder(def TableDef, index IndexDef, prefix []byte, batch map[string]struct{}) (string, error) {
	iter := db.store.ScanIterator()
	defer iter.Close()
	if err := iter.SeekGE(prefix); err != nil {
		return "", err
	}
	for iter.Valid() {
		key, _, err := iter.Deref()
		if err != nil {
			return "", err
		}
		if !bytes.HasPrefix(key, prefix) {
			return "", nil
		}
		pk, err := decodeIndexPrimaryKey(def, index, key, len(prefix))
		if err != nil {
			return "", err
		}
		rowKey, err := encodeRowKey(def, pk)
		if err != nil {
			return "", err
		}
		if _, rewritten := batch[string(rowKey)]; !rewritten {
			return string(rowKey), nil
		}
		if err := iter.Next(); err != nil {
			return "", err
		}
	}
	return "", nil
}
