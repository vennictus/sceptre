package table

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/vennictus/sceptre/internal/kv"
)

// Schema changes rewrite what they must in one Apply, so each is atomic.
// Prefixes are never reused, so dropped data cannot reappear under a new
// table or index.

// DropTable removes a table, its rows, and its indexes.
func (db *DB) DropTable(name string) error {
	def, err := db.mustTable(name)
	if err != nil {
		return err
	}
	mutations, err := db.deletePrefix(rowPrefix(def.Prefix))
	if err != nil {
		return err
	}
	for _, index := range def.Indexes {
		dels, err := db.deletePrefix(indexKeyStart(index))
		if err != nil {
			return err
		}
		mutations = append(mutations, dels...)
	}
	mutations = append(mutations, kv.Delete(catalogTableKey(name)))
	return db.store.Apply(mutations)
}

// DropIndex removes a secondary index and its entries.
func (db *DB) DropIndex(tableName, indexName string) error {
	def, err := db.mustTable(tableName)
	if err != nil {
		return err
	}
	at := -1
	for i, index := range def.Indexes {
		if index.Name == indexName {
			at = i
		}
	}
	if at < 0 {
		return fmt.Errorf("%w: %s", ErrIndexNotFound, indexName)
	}
	mutations, err := db.deletePrefix(indexKeyStart(def.Indexes[at]))
	if err != nil {
		return err
	}
	def.Indexes = append(def.Indexes[:at], def.Indexes[at+1:]...)
	put, err := putTableDef(def)
	if err != nil {
		return err
	}
	return db.store.Apply(append(mutations, put))
}

// RenameTable changes a table's name. Its data keeps its prefix.
func (db *DB) RenameTable(oldName, newName string) error {
	def, err := db.mustTable(oldName)
	if err != nil {
		return err
	}
	if _, ok, err := db.Table(newName); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("%w: %s", ErrTableExists, newName)
	}
	def.Name = newName
	put, err := putTableDef(def)
	if err != nil {
		return err
	}
	return db.store.Apply([]kv.Mutation{kv.Delete(catalogTableKey(oldName)), put})
}

// RenameColumn changes a column's name in the table, its primary key, and
// its indexes. Rows store columns by position, so no data changes.
func (db *DB) RenameColumn(tableName, oldName, newName string) error {
	def, err := db.mustTable(tableName)
	if err != nil {
		return err
	}
	if _, ok := findColumn(def, oldName); !ok {
		return fmt.Errorf("%w: unknown column %s", ErrInvalidTableDef, oldName)
	}
	if _, ok := findColumn(def, newName); ok {
		return fmt.Errorf("%w: column %s already exists", ErrInvalidTableDef, newName)
	}
	rename := func(names []string) {
		for i, n := range names {
			if n == oldName {
				names[i] = newName
			}
		}
	}
	for i := range def.Columns {
		if def.Columns[i].Name == oldName {
			def.Columns[i].Name = newName
		}
	}
	rename(def.PrimaryKey)
	for i := range def.Indexes {
		rename(def.Indexes[i].Columns)
	}
	put, err := putTableDef(def)
	if err != nil {
		return err
	}
	return db.store.Apply([]kv.Mutation{put})
}

// SetChecks replaces a table's check constraints.
func (db *DB) SetChecks(tableName string, checks []string) error {
	def, err := db.mustTable(tableName)
	if err != nil {
		return err
	}
	def.Checks = append([]string(nil), checks...)
	put, err := putTableDef(def)
	if err != nil {
		return err
	}
	return db.store.Apply([]kv.Mutation{put})
}

// AddColumn appends a column that accepts NULL or has a default; existing rows
// read the default, or NULL. A table in the legacy row format is rewritten in
// the tagged format first.
func (db *DB) AddColumn(tableName string, column Column) error {
	def, err := db.mustTable(tableName)
	if err != nil {
		return err
	}
	if !column.Nullable && column.Default == nil {
		return fmt.Errorf("%w: an added NOT NULL column needs a DEFAULT", ErrInvalidTableDef)
	}
	if _, ok := findColumn(def, column.Name); ok {
		return fmt.Errorf("%w: column %s already exists", ErrInvalidTableDef, column.Name)
	}
	next := cloneTableDef(def)
	next.RowFormat = RowFormatTagged
	next.Columns = append(next.Columns, column)
	if err := validateTableDef(next, true); err != nil {
		return err
	}
	var mutations []kv.Mutation
	if !def.tagged() {
		if mutations, err = db.rewriteRows(def, next, func(r Record) Record { return r }); err != nil {
			return err
		}
	}
	put, err := putTableDef(next)
	if err != nil {
		return err
	}
	return db.store.Apply(append(mutations, put))
}

// DropColumn removes a column and rewrites every row without it. Primary
// key columns and indexed columns cannot be dropped; drop the index first.
func (db *DB) DropColumn(tableName, name string) error {
	def, err := db.mustTable(tableName)
	if err != nil {
		return err
	}
	if _, ok := findColumn(def, name); !ok {
		return fmt.Errorf("%w: unknown column %s", ErrInvalidTableDef, name)
	}
	if isPrimaryKey(def, name) {
		return fmt.Errorf("%w: cannot drop primary key column %s", ErrInvalidTableDef, name)
	}
	for _, index := range def.Indexes {
		if indexHasColumn(index, name) {
			return fmt.Errorf("%w: column %s is used by index %s", ErrInvalidTableDef, name, index.Name)
		}
	}
	next := cloneTableDef(def)
	next.Columns = next.Columns[:0]
	for _, c := range def.Columns {
		if c.Name != name {
			next.Columns = append(next.Columns, c)
		}
	}
	if err := validateTableDef(next, true); err != nil {
		return err
	}
	mutations, err := db.rewriteRows(def, next, func(r Record) Record {
		delete(r.Values, name)
		return r
	})
	if err != nil {
		return err
	}
	put, err := putTableDef(next)
	if err != nil {
		return err
	}
	return db.store.Apply(append(mutations, put))
}

// rewriteRows re-encodes every row of from as to, after change. Index entries
// are rewritten too, since their encoding depends on the row format.
func (db *DB) rewriteRows(from, to TableDef, change func(Record) Record) ([]kv.Mutation, error) {
	scanner, err := db.Scan(from.Name, ScanBounds{})
	if err != nil {
		return nil, err
	}
	defer scanner.Close()
	var mutations []kv.Mutation
	for scanner.Valid() {
		row, err := scanner.Deref()
		if err != nil {
			return nil, err
		}
		dels, err := deleteIndexMutations(from, row)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, dels...)
		row = change(row)
		for _, c := range to.Columns {
			if _, ok := row.Values[c.Name]; !ok {
				row.Values[c.Name] = missingValue(c)
			}
		}
		key, err := encodeRowKey(to, primaryKeyRecord(to, row))
		if err != nil {
			return nil, err
		}
		value, err := encodeRowValue(to, row)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, kv.Put(key, value))
		puts, err := putIndexMutations(to, row)
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, puts...)
		if err := scanner.Next(); err != nil {
			return nil, err
		}
	}
	return mutations, nil
}

// deletePrefix returns deletes for every key starting with prefix.
func (db *DB) deletePrefix(prefix []byte) ([]kv.Mutation, error) {
	iter := db.store.ScanIterator()
	defer iter.Close()
	if err := iter.SeekGE(prefix); err != nil {
		return nil, err
	}
	var out []kv.Mutation
	for iter.Valid() {
		key, _, err := iter.Deref()
		if err != nil {
			return nil, err
		}
		if !bytes.HasPrefix(key, prefix) {
			break
		}
		out = append(out, kv.Delete(key))
		if err := iter.Next(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func rowPrefix(prefix uint32) []byte {
	out := []byte{rowKeyPrefix, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[1:], prefix)
	return out
}

func putTableDef(def TableDef) (kv.Mutation, error) {
	encoded, err := encodeTableDef(def)
	if err != nil {
		return kv.Mutation{}, err
	}
	return kv.Put(catalogTableKey(def.Name), encoded), nil
}
