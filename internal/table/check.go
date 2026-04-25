package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

// CheckIssue describes one consistency problem found in a table database.
type CheckIssue struct {
	Code   string
	Detail string
}

// TableCheck summarizes the rows and indexes validated for one table.
type TableCheck struct {
	Name    string
	Rows    int
	Indexes int
}

// CheckReport is the result of a consistency validation pass.
type CheckReport struct {
	Tables []TableCheck
	Issues []CheckIssue
}

// OK reports whether the validation pass found no consistency issues.
func (r CheckReport) OK() bool {
	return len(r.Issues) == 0
}

// Check validates table schemas, row records, and secondary index entries.
func (db *DB) Check() (CheckReport, error) {
	if db == nil || db.kv == nil {
		return CheckReport{}, ErrTableNotFound
	}

	defs, err := db.tables()
	if err != nil {
		return CheckReport{}, err
	}

	report := CheckReport{Tables: make([]TableCheck, 0, len(defs))}
	prefixes := collectPrefixes(defs)
	expectedIndexKeys := make(map[string]string)
	seenIndexKeys := make(map[string]struct{})

	nextPrefix, hasNextPrefix, err := db.loadNextTablePrefix()
	if err != nil {
		report.add("catalog_meta_invalid", err.Error())
	}
	if hasNextPrefix {
		for _, def := range defs {
			if def.Prefix >= nextPrefix {
				report.add("table_prefix_out_of_range", fmt.Sprintf("table %s prefix %d >= next prefix %d", def.Name, def.Prefix, nextPrefix))
			}
			for _, index := range def.Indexes {
				if index.Prefix >= nextPrefix {
					report.add("index_prefix_out_of_range", fmt.Sprintf("index %s prefix %d >= next prefix %d", index.Name, index.Prefix, nextPrefix))
				}
			}
		}
	}

	for _, def := range defs {
		rows, issues, err := db.checkRows(def, expectedIndexKeys)
		if err != nil {
			return CheckReport{}, err
		}
		report.Issues = append(report.Issues, issues...)
		report.Tables = append(report.Tables, TableCheck{
			Name:    def.Name,
			Rows:    rows,
			Indexes: len(def.Indexes),
		})
	}

	if err := db.checkRawKeys(defs, prefixes, expectedIndexKeys, seenIndexKeys, &report); err != nil {
		return CheckReport{}, err
	}
	for key, detail := range expectedIndexKeys {
		if _, ok := seenIndexKeys[key]; !ok {
			report.add("missing_index_entry", detail)
		}
	}

	return report, nil
}

// Tables returns all persisted table definitions sorted by name.
func (db *DB) Tables() ([]TableDef, error) {
	if db == nil || db.kv == nil {
		return nil, ErrTableNotFound
	}
	return db.tables()
}

func (db *DB) tables() ([]TableDef, error) {
	iter := db.kv.Iterator()
	if err := iter.SeekGE(catalogTableStart); err != nil {
		return nil, err
	}

	var defs []TableDef
	for iter.Valid() {
		key, value, err := iter.Deref()
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(key, catalogTableStart) {
			break
		}
		def, err := decodeTableDef(value)
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
		if err := iter.Next(); err != nil {
			return nil, err
		}
	}
	sort.Slice(defs, func(i, j int) bool {
		return defs[i].Name < defs[j].Name
	})
	return defs, nil
}

func (db *DB) loadNextTablePrefix() (uint32, bool, error) {
	value, ok, err := db.kv.Get(catalogMetaKey)
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(value) != 4 {
		return 0, true, ErrInvalidTableDef
	}
	next := binary.BigEndian.Uint32(value)
	if next == 0 {
		return 0, true, ErrInvalidTableDef
	}
	return next, true, nil
}

func (db *DB) checkRows(def TableDef, expectedIndexKeys map[string]string) (int, []CheckIssue, error) {
	var issues []CheckIssue
	scanner, err := db.Scan(def.Name, ScanBounds{})
	if err != nil {
		return 0, nil, err
	}

	rows := 0
	for scanner.Valid() {
		row, err := scanner.Deref()
		if err != nil {
			return 0, nil, err
		}
		if err := validateFullRecord(def, row); err != nil {
			issues = append(issues, CheckIssue{
				Code:   "invalid_row",
				Detail: fmt.Sprintf("table %s row %s: %v", def.Name, recordString(def.Columns, row), err),
			})
		}
		for _, index := range def.Indexes {
			key, err := encodeIndexKey(def, index, row)
			if err != nil {
				issues = append(issues, CheckIssue{
					Code:   "invalid_index_key",
					Detail: fmt.Sprintf("table %s index %s row %s: %v", def.Name, index.Name, recordString(def.Columns, row), err),
				})
				continue
			}
			expectedIndexKeys[string(key)] = fmt.Sprintf("table %s index %s row %s", def.Name, index.Name, recordString(def.Columns, row))
		}
		rows++
		if err := scanner.Next(); err != nil {
			return 0, nil, err
		}
	}
	return rows, issues, nil
}

func (db *DB) checkRawKeys(defs []TableDef, prefixes prefixSet, expectedIndexKeys map[string]string, seenIndexKeys map[string]struct{}, report *CheckReport) error {
	iter := db.kv.Iterator()
	if err := iter.SeekFirst(); err != nil {
		return err
	}
	for iter.Valid() {
		key, _, err := iter.Deref()
		if err != nil {
			return err
		}
		if len(key) >= 5 {
			prefix := binary.BigEndian.Uint32(key[1:5])
			switch key[0] {
			case rowKeyPrefix:
				if _, ok := prefixes.row[prefix]; !ok {
					report.add("unknown_row_prefix", fmt.Sprintf("row key has unknown table prefix %d", prefix))
				}
			case indexKeyPrefix:
				indexRef, ok := prefixes.index[prefix]
				if !ok {
					report.add("unknown_index_prefix", fmt.Sprintf("index key has unknown index prefix %d", prefix))
					break
				}
				db.checkIndexEntry(defs, indexRef, key, expectedIndexKeys, seenIndexKeys, report)
			}
		}
		if err := iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) checkIndexEntry(defs []TableDef, ref indexRef, key []byte, expectedIndexKeys map[string]string, seenIndexKeys map[string]struct{}, report *CheckReport) {
	def := defs[ref.table]
	index := def.Indexes[ref.index]
	seenIndexKeys[string(key)] = struct{}{}
	if _, ok := expectedIndexKeys[string(key)]; !ok {
		report.add("unexpected_index_entry", fmt.Sprintf("table %s index %s has stale or mismatched key", def.Name, index.Name))
	}
	primaryKey, err := decodeIndexPrimaryKey(def, index, key, len(indexKeyStart(index))+encodedIndexValueLen(def, index, key))
	if err != nil {
		report.add("corrupt_index_entry", fmt.Sprintf("table %s index %s: %v", def.Name, index.Name, err))
		return
	}
	if _, ok, err := db.Get(def.Name, primaryKey); err != nil {
		report.add("corrupt_index_entry", fmt.Sprintf("table %s index %s primary key %s: %v", def.Name, index.Name, recordString(def.Columns, primaryKey), err))
	} else if !ok {
		report.add("orphan_index_entry", fmt.Sprintf("table %s index %s primary key %s", def.Name, index.Name, recordString(def.Columns, primaryKey)))
	}
}

func encodedIndexValueLen(def TableDef, index IndexDef, key []byte) int {
	remaining := key[len(indexKeyStart(index)):]
	consumed := 0
	for _, name := range index.Columns {
		valueType, _ := columnType(def, name)
		_, n, err := consumeEncodedValue(remaining, valueType)
		if err != nil {
			return len(remaining)
		}
		remaining = remaining[n:]
		consumed += n
	}
	return consumed
}

type indexRef struct {
	table int
	index int
}

type prefixSet struct {
	row   map[uint32]string
	index map[uint32]indexRef
}

func collectPrefixes(defs []TableDef) prefixSet {
	out := prefixSet{
		row:   make(map[uint32]string, len(defs)),
		index: make(map[uint32]indexRef),
	}
	for i, def := range defs {
		if other, exists := out.row[def.Prefix]; exists {
			out.row[def.Prefix] = other + "," + def.Name
		} else {
			out.row[def.Prefix] = def.Name
		}
		for j, index := range def.Indexes {
			out.index[index.Prefix] = indexRef{table: i, index: j}
		}
	}
	return out
}

func (r *CheckReport) add(code, detail string) {
	r.Issues = append(r.Issues, CheckIssue{Code: code, Detail: detail})
}

func recordString(columns []Column, record Record) string {
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		value, ok := record.Values[column.Name]
		if !ok {
			continue
		}
		parts = append(parts, column.Name+"="+valueString(value))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func valueString(value Value) string {
	switch value.Type {
	case TypeInt64:
		return fmt.Sprintf("%d", value.I64)
	case TypeBytes:
		return fmt.Sprintf("%q", string(value.Bytes))
	default:
		return "<invalid>"
	}
}
