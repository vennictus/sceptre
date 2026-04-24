package table

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sceptre/internal/kv"
)

type Type uint32

const (
	TypeBytes Type = 1
	TypeInt64 Type = 2

	rowKeyPrefix       byte   = 0x10
	indexKeyPrefix     byte   = 0x11
	initialTablePrefix uint32 = 1
)

var (
	ErrInvalidTableDef = errors.New("table: invalid table definition")
	ErrTableExists     = errors.New("table: table already exists")
	ErrTableNotFound   = errors.New("table: table not found")
	ErrInvalidRecord   = errors.New("table: invalid record")
	ErrInvalidValue    = errors.New("table: invalid value")
	ErrRowExists       = errors.New("table: row already exists")
	ErrRowNotFound     = errors.New("table: row not found")
	ErrInvalidIndexDef = errors.New("table: invalid index definition")
	ErrIndexExists     = errors.New("table: index already exists")
	ErrIndexNotFound   = errors.New("table: index not found")
)

var (
	catalogMetaKey    = []byte{0x00, 'm'}
	catalogTableStart = []byte{0x00, 't'}
)

// Options controls the table database's backing KV store.
type Options struct {
	PageSize uint32
}

// Column defines one named table column.
type Column struct {
	Name string `json:"name"`
	Type Type   `json:"type"`
}

// TableDef is the persisted schema for a table.
type TableDef struct {
	Name       string     `json:"name"`
	Columns    []Column   `json:"columns"`
	PrimaryKey []string   `json:"primary_key"`
	Prefix     uint32     `json:"prefix"`
	Indexes    []IndexDef `json:"indexes,omitempty"`
}

// IndexDef is the persisted schema for a secondary index.
type IndexDef struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Prefix  uint32   `json:"prefix"`
}

// Value stores one typed scalar value.
type Value struct {
	Type  Type
	Bytes []byte
	I64   int64
}

// Record is a row or primary-key tuple keyed by column name.
type Record struct {
	Values map[string]Value
}

// DB maps table definitions and rows onto the durable KV layer.
type DB struct {
	kv    *kv.KV
	ownKV bool
}

// Open opens a table database backed by the durable KV layer.
func Open(path string, opts Options) (*DB, error) {
	store, err := kv.Open(path, kv.Options{PageSize: opts.PageSize})
	if err != nil {
		return nil, err
	}
	return &DB{kv: store, ownKV: true}, nil
}

// New wraps an existing KV store with table operations.
func New(store *kv.KV) *DB {
	return &DB{kv: store}
}

// Close closes the backing KV store if this DB opened it.
func (db *DB) Close() error {
	if db == nil || db.kv == nil || !db.ownKV {
		return nil
	}
	return db.kv.Close()
}

// BytesValue constructs a byte-string table value.
func BytesValue(value []byte) Value {
	return Value{Type: TypeBytes, Bytes: cloneBytes(value)}
}

// Int64Value constructs a signed integer table value.
func Int64Value(value int64) Value {
	return Value{Type: TypeInt64, I64: value}
}

// NewRecord creates a defensive copy of the provided column values.
func NewRecord(values map[string]Value) Record {
	out := Record{Values: make(map[string]Value, len(values))}
	for name, value := range values {
		out.Values[name] = cloneValue(value)
	}
	return out
}

// CreateTable stores a new table definition and assigns it a unique key prefix.
func (db *DB) CreateTable(def TableDef) error {
	if db == nil || db.kv == nil {
		return ErrTableNotFound
	}
	if err := validateTableDef(def, false); err != nil {
		return err
	}
	if _, ok, err := db.Table(def.Name); err != nil {
		return err
	} else if ok {
		return fmt.Errorf("%w: %s", ErrTableExists, def.Name)
	}

	prefix, err := db.nextTablePrefix()
	if err != nil {
		return err
	}
	def.Prefix = prefix

	// Publish the next prefix first. A crash can waste a prefix, but cannot
	// cause two tables to share one before Chapter 11 transactions exist.
	if err := db.storeNextTablePrefix(prefix + 1); err != nil {
		return err
	}

	encoded, err := encodeTableDef(def)
	if err != nil {
		return err
	}
	return db.kv.Set(catalogTableKey(def.Name), encoded)
}

// Table loads a table definition by name.
func (db *DB) Table(name string) (TableDef, bool, error) {
	if db == nil || db.kv == nil {
		return TableDef{}, false, ErrTableNotFound
	}
	value, ok, err := db.kv.Get(catalogTableKey(name))
	if err != nil || !ok {
		return TableDef{}, ok, err
	}

	def, err := decodeTableDef(value)
	if err != nil {
		return TableDef{}, false, err
	}
	return cloneTableDef(def), true, nil
}

// CreateIndex adds a secondary index and backfills entries for existing rows.
func (db *DB) CreateIndex(tableName string, index IndexDef) error {
	def, err := db.mustTable(tableName)
	if err != nil {
		return err
	}
	if err := validateNewIndexDef(def, index); err != nil {
		return err
	}

	prefix, err := db.nextTablePrefix()
	if err != nil {
		return err
	}
	index.Prefix = prefix
	if err := db.storeNextTablePrefix(prefix + 1); err != nil {
		return err
	}

	scanner, err := db.Scan(tableName, ScanBounds{})
	if err != nil {
		return err
	}
	for scanner.Valid() {
		row, err := scanner.Deref()
		if err != nil {
			return err
		}
		key, err := encodeIndexKey(def, index, row)
		if err != nil {
			return err
		}
		if err := db.kv.Set(key, nil); err != nil {
			return err
		}
		if err := scanner.Next(); err != nil {
			return err
		}
	}

	def.Indexes = append(def.Indexes, index)
	encoded, err := encodeTableDef(def)
	if err != nil {
		return err
	}
	return db.kv.Set(catalogTableKey(def.Name), encoded)
}

// Index loads an index definition by name.
func (db *DB) Index(tableName, indexName string) (IndexDef, bool, error) {
	def, err := db.mustTable(tableName)
	if err != nil {
		return IndexDef{}, false, err
	}
	index, ok := findIndex(def, indexName)
	return cloneIndexDef(index), ok, nil
}

// LookupIndex returns rows matching all columns of a secondary index key.
func (db *DB) LookupIndex(tableName, indexName string, values Record) ([]Record, error) {
	def, err := db.mustTable(tableName)
	if err != nil {
		return nil, err
	}
	index, ok := findIndex(def, indexName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrIndexNotFound, indexName)
	}
	if err := validateIndexRecord(def, index, values); err != nil {
		return nil, err
	}

	prefix, err := encodeIndexLookupPrefix(def, index, values)
	if err != nil {
		return nil, err
	}
	upper, hasUpper := prefixEnd(prefix)

	iter := db.kv.Iterator()
	if err := iter.SeekGE(prefix); err != nil {
		return nil, err
	}

	var rows []Record
	for iter.Valid() {
		key, _, err := iter.Deref()
		if err != nil {
			return nil, err
		}
		if hasUpper {
			if bytes.Compare(key, upper) >= 0 {
				break
			}
		} else if !bytes.HasPrefix(key, prefix) {
			break
		}

		primaryKey, err := decodeIndexPrimaryKey(def, index, key, len(prefix))
		if err != nil {
			return nil, err
		}
		row, ok, err := db.Get(tableName, primaryKey)
		if err != nil {
			return nil, err
		}
		if ok {
			rows = append(rows, row)
		}
		if err := iter.Next(); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// Insert writes a new row and fails if the primary key already exists.
func (db *DB) Insert(tableName string, record Record) error {
	return db.writeRow(tableName, record, writeInsertOnly)
}

// Update overwrites an existing row and fails if the primary key is missing.
func (db *DB) Update(tableName string, record Record) error {
	return db.writeRow(tableName, record, writeUpdateOnly)
}

// Upsert writes a row regardless of whether it already exists.
func (db *DB) Upsert(tableName string, record Record) error {
	return db.writeRow(tableName, record, writeUpsert)
}

// Get loads a row by primary key.
func (db *DB) Get(tableName string, key Record) (Record, bool, error) {
	def, err := db.mustTable(tableName)
	if err != nil {
		return Record{}, false, err
	}
	if err := validateKeyRecord(def, key); err != nil {
		return Record{}, false, err
	}

	rowKey, err := encodeRowKey(def, key)
	if err != nil {
		return Record{}, false, err
	}
	data, ok, err := db.kv.Get(rowKey)
	if err != nil || !ok {
		return Record{}, ok, err
	}

	record, err := decodeRowValue(def, key, data)
	if err != nil {
		return Record{}, false, err
	}
	return record, true, nil
}

// Delete removes a row by primary key.
func (db *DB) Delete(tableName string, key Record) (bool, error) {
	def, err := db.mustTable(tableName)
	if err != nil {
		return false, err
	}
	if err := validateKeyRecord(def, key); err != nil {
		return false, err
	}

	rowKey, err := encodeRowKey(def, key)
	if err != nil {
		return false, err
	}
	old, ok, err := db.Get(tableName, key)
	if err != nil || !ok {
		return ok, err
	}
	removed, err := db.kv.Del(rowKey)
	if err != nil || !removed {
		return removed, err
	}
	if err := db.deleteIndexEntries(def, old); err != nil {
		return false, err
	}
	return true, nil
}

type writeMode int

const (
	writeInsertOnly writeMode = iota
	writeUpdateOnly
	writeUpsert
)

func (db *DB) writeRow(tableName string, record Record, mode writeMode) error {
	def, err := db.mustTable(tableName)
	if err != nil {
		return err
	}
	if err := validateFullRecord(def, record); err != nil {
		return err
	}

	keyRecord := primaryKeyRecord(def, record)
	rowKey, err := encodeRowKey(def, keyRecord)
	if err != nil {
		return err
	}
	_, exists, err := db.kv.Get(rowKey)
	if err != nil {
		return err
	}
	var old Record
	if exists {
		var ok bool
		old, ok, err = db.Get(tableName, keyRecord)
		if err != nil {
			return err
		}
		if !ok {
			return ErrRowNotFound
		}
	}
	switch {
	case mode == writeInsertOnly && exists:
		return ErrRowExists
	case mode == writeUpdateOnly && !exists:
		return ErrRowNotFound
	}

	rowValue, err := encodeRowValue(def, record)
	if err != nil {
		return err
	}
	if err := db.kv.Set(rowKey, rowValue); err != nil {
		return err
	}
	if exists {
		if err := db.deleteIndexEntries(def, old); err != nil {
			return err
		}
	}
	return db.putIndexEntries(def, record)
}

func (db *DB) mustTable(name string) (TableDef, error) {
	def, ok, err := db.Table(name)
	if err != nil {
		return TableDef{}, err
	}
	if !ok {
		return TableDef{}, fmt.Errorf("%w: %s", ErrTableNotFound, name)
	}
	return def, nil
}

func (db *DB) nextTablePrefix() (uint32, error) {
	value, ok, err := db.kv.Get(catalogMetaKey)
	if err != nil {
		return 0, err
	}
	if !ok {
		return initialTablePrefix, nil
	}
	if len(value) != 4 {
		return 0, ErrInvalidTableDef
	}
	next := binary.BigEndian.Uint32(value)
	if next == 0 {
		return 0, ErrInvalidTableDef
	}
	return next, nil
}

func (db *DB) storeNextTablePrefix(next uint32) error {
	if next == 0 {
		return ErrInvalidTableDef
	}
	var value [4]byte
	binary.BigEndian.PutUint32(value[:], next)
	return db.kv.Set(catalogMetaKey, value[:])
}

func encodeTableDef(def TableDef) ([]byte, error) {
	if err := validateTableDef(def, true); err != nil {
		return nil, err
	}
	return json.Marshal(def)
}

func decodeTableDef(data []byte) (TableDef, error) {
	var def TableDef
	if err := json.Unmarshal(data, &def); err != nil {
		return TableDef{}, err
	}
	if err := validateTableDef(def, true); err != nil {
		return TableDef{}, err
	}
	return def, nil
}

func validateTableDef(def TableDef, requirePrefix bool) error {
	if def.Name == "" || len(def.Columns) == 0 || len(def.PrimaryKey) == 0 {
		return ErrInvalidTableDef
	}
	if requirePrefix && def.Prefix == 0 {
		return ErrInvalidTableDef
	}

	columns := make(map[string]Type, len(def.Columns))
	for _, column := range def.Columns {
		if column.Name == "" || !validType(column.Type) {
			return ErrInvalidTableDef
		}
		if _, exists := columns[column.Name]; exists {
			return ErrInvalidTableDef
		}
		columns[column.Name] = column.Type
	}

	seenPK := make(map[string]struct{}, len(def.PrimaryKey))
	for _, name := range def.PrimaryKey {
		if _, ok := columns[name]; !ok {
			return ErrInvalidTableDef
		}
		if _, exists := seenPK[name]; exists {
			return ErrInvalidTableDef
		}
		seenPK[name] = struct{}{}
	}
	if err := validateIndexDefs(def, columns, requirePrefix); err != nil {
		return err
	}
	return nil
}

func validateNewIndexDef(def TableDef, index IndexDef) error {
	columns := make(map[string]Type, len(def.Columns))
	for _, column := range def.Columns {
		columns[column.Name] = column.Type
	}
	if err := validateIndexDef(index, columns, false); err != nil {
		return err
	}
	if _, exists := findIndex(def, index.Name); exists {
		return fmt.Errorf("%w: %s", ErrIndexExists, index.Name)
	}
	return nil
}

func validateIndexDefs(def TableDef, columns map[string]Type, requirePrefix bool) error {
	seen := make(map[string]struct{}, len(def.Indexes))
	for _, index := range def.Indexes {
		if err := validateIndexDef(index, columns, requirePrefix); err != nil {
			return err
		}
		if _, exists := seen[index.Name]; exists {
			return ErrInvalidIndexDef
		}
		seen[index.Name] = struct{}{}
	}
	return nil
}

func validateIndexDef(index IndexDef, columns map[string]Type, requirePrefix bool) error {
	if index.Name == "" || len(index.Columns) == 0 {
		return ErrInvalidIndexDef
	}
	if requirePrefix && index.Prefix == 0 {
		return ErrInvalidIndexDef
	}

	seen := make(map[string]struct{}, len(index.Columns))
	for _, name := range index.Columns {
		if _, ok := columns[name]; !ok {
			return ErrInvalidIndexDef
		}
		if _, exists := seen[name]; exists {
			return ErrInvalidIndexDef
		}
		seen[name] = struct{}{}
	}
	return nil
}

func validateFullRecord(def TableDef, record Record) error {
	if len(record.Values) != len(def.Columns) {
		return ErrInvalidRecord
	}
	for _, column := range def.Columns {
		value, ok := record.Values[column.Name]
		if !ok {
			return ErrInvalidRecord
		}
		if !valueMatchesType(value, column.Type) {
			return ErrInvalidValue
		}
	}
	for name := range record.Values {
		if _, ok := columnType(def, name); !ok {
			return ErrInvalidRecord
		}
	}
	return nil
}

func validateKeyRecord(def TableDef, record Record) error {
	if len(record.Values) != len(def.PrimaryKey) {
		return ErrInvalidRecord
	}
	for _, name := range def.PrimaryKey {
		value, ok := record.Values[name]
		if !ok {
			return ErrInvalidRecord
		}
		valueType, _ := columnType(def, name)
		if !valueMatchesType(value, valueType) {
			return ErrInvalidValue
		}
	}
	for name := range record.Values {
		if !isPrimaryKey(def, name) {
			return ErrInvalidRecord
		}
	}
	return nil
}

func validateIndexRecord(def TableDef, index IndexDef, record Record) error {
	if len(record.Values) != len(index.Columns) {
		return ErrInvalidRecord
	}
	for _, name := range index.Columns {
		value, ok := record.Values[name]
		if !ok {
			return ErrInvalidRecord
		}
		valueType, _ := columnType(def, name)
		if !valueMatchesType(value, valueType) {
			return ErrInvalidValue
		}
	}
	for name := range record.Values {
		if !indexHasColumn(index, name) {
			return ErrInvalidRecord
		}
	}
	return nil
}

func encodeRowKey(def TableDef, key Record) ([]byte, error) {
	out := make([]byte, 0, 8)
	out = append(out, rowKeyPrefix)
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], def.Prefix)
	out = append(out, prefix[:]...)

	for _, name := range def.PrimaryKey {
		value := key.Values[name]
		out = appendEncodedValue(out, value)
	}
	return out, nil
}

func encodeRowValue(def TableDef, record Record) ([]byte, error) {
	out := make([]byte, 0)
	for _, column := range def.Columns {
		if isPrimaryKey(def, column.Name) {
			continue
		}
		out = appendEncodedValue(out, record.Values[column.Name])
	}
	return out, nil
}

func decodeRowValue(def TableDef, key Record, data []byte) (Record, error) {
	values := make(map[string]Value, len(def.Columns))
	for name, value := range key.Values {
		values[name] = cloneValue(value)
	}

	remaining := data
	for _, column := range def.Columns {
		if isPrimaryKey(def, column.Name) {
			continue
		}
		value, consumed, err := consumeEncodedValue(remaining, column.Type)
		if err != nil {
			return Record{}, err
		}
		values[column.Name] = value
		remaining = remaining[consumed:]
	}
	if len(remaining) != 0 {
		return Record{}, ErrCorruptRecord
	}
	return Record{Values: values}, nil
}

func primaryKeyRecord(def TableDef, record Record) Record {
	values := make(map[string]Value, len(def.PrimaryKey))
	for _, name := range def.PrimaryKey {
		values[name] = cloneValue(record.Values[name])
	}
	return Record{Values: values}
}

func (db *DB) putIndexEntries(def TableDef, record Record) error {
	for _, index := range def.Indexes {
		key, err := encodeIndexKey(def, index, record)
		if err != nil {
			return err
		}
		if err := db.kv.Set(key, nil); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) deleteIndexEntries(def TableDef, record Record) error {
	for _, index := range def.Indexes {
		key, err := encodeIndexKey(def, index, record)
		if err != nil {
			return err
		}
		if _, err := db.kv.Del(key); err != nil {
			return err
		}
	}
	return nil
}

func encodeIndexKey(def TableDef, index IndexDef, record Record) ([]byte, error) {
	out := indexKeyStart(index)
	for _, name := range index.Columns {
		value := record.Values[name]
		valueType, _ := columnType(def, name)
		if !valueMatchesType(value, valueType) {
			return nil, ErrInvalidValue
		}
		out = appendEncodedValue(out, value)
	}
	for _, name := range def.PrimaryKey {
		out = appendEncodedValue(out, record.Values[name])
	}
	return out, nil
}

func encodeIndexLookupPrefix(def TableDef, index IndexDef, values Record) ([]byte, error) {
	if err := validateIndexRecord(def, index, values); err != nil {
		return nil, err
	}

	out := indexKeyStart(index)
	for _, name := range index.Columns {
		out = appendEncodedValue(out, values.Values[name])
	}
	return out, nil
}

func decodeIndexPrimaryKey(def TableDef, index IndexDef, key []byte, prefixLen int) (Record, error) {
	start := indexKeyStart(index)
	if !bytes.HasPrefix(key, start) || prefixLen > len(key) {
		return Record{}, ErrCorruptRecord
	}

	values := make(map[string]Value, len(def.PrimaryKey))
	remaining := key[prefixLen:]
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

func indexKeyStart(index IndexDef) []byte {
	out := []byte{indexKeyPrefix, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[1:], index.Prefix)
	return out
}

func prefixEnd(prefix []byte) ([]byte, bool) {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1], true
		}
	}
	return nil, false
}

func catalogTableKey(name string) []byte {
	key := append([]byte(nil), catalogTableStart...)
	return append(key, []byte(name)...)
}

func validType(valueType Type) bool {
	return valueType == TypeBytes || valueType == TypeInt64
}

func valueMatchesType(value Value, valueType Type) bool {
	return value.Type == valueType && validType(value.Type)
}

func columnType(def TableDef, name string) (Type, bool) {
	for _, column := range def.Columns {
		if column.Name == name {
			return column.Type, true
		}
	}
	return 0, false
}

func isPrimaryKey(def TableDef, name string) bool {
	for _, primary := range def.PrimaryKey {
		if primary == name {
			return true
		}
	}
	return false
}

func findIndex(def TableDef, name string) (IndexDef, bool) {
	for _, index := range def.Indexes {
		if index.Name == name {
			return index, true
		}
	}
	return IndexDef{}, false
}

func indexHasColumn(index IndexDef, name string) bool {
	for _, column := range index.Columns {
		if column == name {
			return true
		}
	}
	return false
}

func cloneTableDef(def TableDef) TableDef {
	out := TableDef{
		Name:       def.Name,
		Prefix:     def.Prefix,
		Columns:    append([]Column(nil), def.Columns...),
		PrimaryKey: append([]string(nil), def.PrimaryKey...),
		Indexes:    make([]IndexDef, 0, len(def.Indexes)),
	}
	for _, index := range def.Indexes {
		out.Indexes = append(out.Indexes, cloneIndexDef(index))
	}
	return out
}

func cloneIndexDef(index IndexDef) IndexDef {
	return IndexDef{
		Name:    index.Name,
		Prefix:  index.Prefix,
		Columns: append([]string(nil), index.Columns...),
	}
}

func cloneValue(value Value) Value {
	if value.Type == TypeBytes {
		value.Bytes = cloneBytes(value.Bytes)
	}
	return value
}

func cloneBytes(src []byte) []byte {
	return append([]byte(nil), src...)
}
