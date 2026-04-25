package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sceptre/internal/btree"
	"sceptre/internal/freelist"
	"sceptre/internal/pager"
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

	report.Issues = append(report.Issues, checkPrefixUniqueness(defs)...)
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
	if err := db.checkStoragePages(&report); err != nil {
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

func (db *DB) checkStoragePages(report *CheckReport) error {
	p := db.kv.Pager()
	meta := p.Meta()
	snapshot := db.kv.Tree().Snapshot()
	reachable := make(map[uint64]struct{}, len(snapshot.Pages))
	for pageID := range snapshot.Pages {
		reachable[pageID] = struct{}{}
	}

	if snapshot.Root != 0 {
		if _, ok := snapshot.Pages[snapshot.Root]; !ok {
			report.add("missing_root_page", fmt.Sprintf("root page %d is not loaded in tree snapshot", snapshot.Root))
		}
	}
	for pageID, page := range snapshot.Pages {
		if pageID < pager.MetaPageCount || pageID >= meta.PageCount {
			report.add("tree_page_out_of_range", fmt.Sprintf("tree page %d outside page count %d", pageID, meta.PageCount))
			continue
		}
		checkBTreePage(pageID, page, reachable, report)
	}

	state, err := freelist.Load(p, meta.FreeListPage)
	if err != nil {
		report.add("freelist_invalid", err.Error())
		return nil
	}
	checkPageIDBounds("freelist_head", state.HeadPage, meta.PageCount, report)
	seenFree := make(map[uint64]string)
	for _, pageID := range state.PageIDs {
		checkPageIDBounds("freelist_page", pageID, meta.PageCount, report)
		if _, ok := reachable[pageID]; ok {
			report.add("freelist_reachable_overlap", fmt.Sprintf("freelist page %d is reachable from tree root", pageID))
		}
		checkDuplicatePage("duplicate_freelist_page", pageID, "freelist page", seenFree, report)
	}
	for _, pageID := range state.FreePages {
		checkPageIDBounds("free_page", pageID, meta.PageCount, report)
		if _, ok := reachable[pageID]; ok {
			report.add("free_page_reachable_overlap", fmt.Sprintf("free page %d is reachable from tree root", pageID))
		}
		checkDuplicatePage("duplicate_free_page", pageID, "free page", seenFree, report)
	}
	return nil
}

func checkBTreePage(pageID uint64, page []byte, reachable map[uint64]struct{}, report *CheckReport) {
	node, err := btree.WrapNode(page)
	if err != nil {
		report.add("btree_page_invalid", fmt.Sprintf("page %d: %v", pageID, err))
		return
	}
	var previous []byte
	for i := 0; i < node.Count(); i++ {
		var key []byte
		switch node.Type() {
		case btree.NodeTypeLeaf:
			cell, err := node.LeafCell(i)
			if err != nil {
				report.add("btree_page_invalid", fmt.Sprintf("leaf page %d cell %d: %v", pageID, i, err))
				continue
			}
			key = cell.Key
		case btree.NodeTypeInternal:
			cell, err := node.InternalCell(i)
			if err != nil {
				report.add("btree_page_invalid", fmt.Sprintf("internal page %d cell %d: %v", pageID, i, err))
				continue
			}
			key = cell.Key
			if _, ok := reachable[cell.Child]; !ok {
				report.add("btree_dangling_child", fmt.Sprintf("internal page %d references child %d", pageID, cell.Child))
			}
		default:
			report.add("btree_page_invalid", fmt.Sprintf("page %d has unknown node type %d", pageID, node.Type()))
			return
		}
		if previous != nil && bytes.Compare(previous, key) >= 0 {
			report.add("btree_key_order", fmt.Sprintf("page %d cell %d key is not strictly increasing", pageID, i))
		}
		previous = append(previous[:0], key...)
	}
}

func checkPageIDBounds(code string, pageID, pageCount uint64, report *CheckReport) {
	if pageID == 0 {
		return
	}
	if pageID < pager.MetaPageCount || pageID >= pageCount {
		report.add(code+"_out_of_range", fmt.Sprintf("page %d outside page count %d", pageID, pageCount))
	}
}

func checkDuplicatePage(code string, pageID uint64, label string, seen map[uint64]string, report *CheckReport) {
	if pageID == 0 {
		return
	}
	if previous, ok := seen[pageID]; ok {
		report.add(code, fmt.Sprintf("%s %d duplicates %s", label, pageID, previous))
		return
	}
	seen[pageID] = label
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

func checkPrefixUniqueness(defs []TableDef) []CheckIssue {
	var issues []CheckIssue
	rowPrefixes := make(map[uint32]string)
	indexPrefixes := make(map[uint32]string)
	for _, def := range defs {
		if previous, ok := rowPrefixes[def.Prefix]; ok {
			issues = append(issues, CheckIssue{
				Code:   "duplicate_table_prefix",
				Detail: fmt.Sprintf("table %s shares prefix %d with table %s", def.Name, def.Prefix, previous),
			})
		}
		rowPrefixes[def.Prefix] = def.Name
		if previous, ok := indexPrefixes[def.Prefix]; ok {
			issues = append(issues, CheckIssue{
				Code:   "duplicate_table_index_prefix",
				Detail: fmt.Sprintf("table %s prefix %d overlaps index %s", def.Name, def.Prefix, previous),
			})
		}
		for _, index := range def.Indexes {
			if previous, ok := indexPrefixes[index.Prefix]; ok {
				issues = append(issues, CheckIssue{
					Code:   "duplicate_index_prefix",
					Detail: fmt.Sprintf("index %s shares prefix %d with %s", index.Name, index.Prefix, previous),
				})
			}
			indexPrefixes[index.Prefix] = index.Name
			if previous, ok := rowPrefixes[index.Prefix]; ok {
				issues = append(issues, CheckIssue{
					Code:   "duplicate_table_index_prefix",
					Detail: fmt.Sprintf("index %s prefix %d overlaps table %s", index.Name, index.Prefix, previous),
				})
			}
		}
	}
	return issues
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
