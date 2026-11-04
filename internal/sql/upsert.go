package sql

import (
	"fmt"
	"slices"

	"github.com/vennictus/sceptre/internal/table"
)

// conflictTarget is the constraint an ON CONFLICT clause handles: the primary
// key (index == "") or one unique index. any handles every key and unique
// index (ON CONFLICT DO NOTHING without columns).
type conflictTarget struct {
	any   bool
	index string
}

func resolveConflictTarget(def table.TableDef, oc *OnConflict) (conflictTarget, error) {
	if oc.Columns == nil {
		return conflictTarget{any: true}, nil
	}
	for i, c := range oc.Columns {
		if slices.Contains(oc.Columns[:i], c) {
			return conflictTarget{}, fmt.Errorf("%w: ON CONFLICT names column %s twice", ErrExec, c)
		}
	}
	sameSet := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for _, x := range a {
			if !slices.Contains(b, x) {
				return false
			}
		}
		return true
	}
	if sameSet(oc.Columns, def.PrimaryKey) {
		return conflictTarget{}, nil
	}
	for _, index := range def.Indexes {
		if index.Unique && sameSet(oc.Columns, index.Columns) {
			return conflictTarget{index: index.Name}, nil
		}
	}
	return conflictTarget{}, fmt.Errorf("%w: ON CONFLICT columns match no primary key or unique index", ErrExec)
}

// findConflict returns the stored row that values would collide with under
// target, if any.
func findConflict(db *table.DB, def table.TableDef, target conflictTarget, values map[string]table.Value) (table.Record, bool, error) {
	if target.any || target.index == "" {
		key := primaryKeyRecord(def, table.NewRecord(values))
		row, ok, err := db.Get(def.Name, key)
		if err != nil || ok {
			return row, ok, err
		}
	}
	for _, index := range def.Indexes {
		if !index.Unique || !(target.any || target.index == index.Name) {
			continue
		}
		probe := make(map[string]table.Value, len(index.Columns))
		for _, c := range index.Columns {
			if values[c].IsNull() {
				probe = nil
				break
			}
			probe[c] = values[c]
		}
		if probe == nil {
			continue // NULLs never conflict
		}
		rows, err := db.LookupIndex(def.Name, index.Name, table.NewRecord(probe))
		if err != nil {
			return table.Record{}, false, err
		}
		if len(rows) > 0 {
			return rows[0], true, nil
		}
	}
	return table.Record{}, false, nil
}

// validateConflictSet checks ON CONFLICT DO UPDATE assignments.
func validateConflictSet(def table.TableDef, set []Assignment) error {
	seen := map[string]bool{}
	for _, a := range set {
		typ, ok := columnType(def, a.Column)
		if !ok {
			return fmt.Errorf("%w: unknown column %s", ErrExec, a.Column)
		}
		if slices.Contains(def.PrimaryKey, a.Column) {
			return fmt.Errorf("%w: updating primary-key column %s is unsupported", ErrExec, a.Column)
		}
		if seen[a.Column] {
			return fmt.Errorf("%w: duplicate assignment to %s", ErrExec, a.Column)
		}
		seen[a.Column] = true
		if a.Excluded == "" {
			if _, err := literalToValue(a.Value, typ); err != nil {
				return err
			}
			continue
		}
		if t, ok := columnType(def, a.Excluded); !ok || t != typ {
			return fmt.Errorf("%w: excluded.%s does not match column %s", ErrExec, a.Excluded, a.Column)
		}
	}
	return nil
}

// insertWithConflicts writes rows one at a time so each sees the ones before
// it, skipping or updating rows that hit the conflict target. It returns the
// rows written, inserted or updated.
func insertWithConflicts(db *table.DB, def table.TableDef, oc *OnConflict, target conflictTarget, records []table.Record) ([]table.Record, error) {
	var written []table.Record
	for _, record := range records {
		existing, conflict, err := findConflict(db, def, target, record.Values)
		if err != nil {
			return nil, err
		}
		if !conflict {
			if err := db.Insert(def.Name, record); err != nil {
				return nil, err
			}
			written = append(written, record)
			continue
		}
		if oc.DoNothing {
			continue
		}
		updated := table.NewRecord(existing.Values)
		for _, a := range oc.Set {
			if a.Excluded != "" {
				updated.Values[a.Column] = record.Values[a.Excluded]
				continue
			}
			typ, _ := columnType(def, a.Column)
			v, err := literalToValue(a.Value, typ)
			if err != nil {
				return nil, err
			}
			updated.Values[a.Column] = v
		}
		if err := checkRow(def, updated); err != nil {
			return nil, err
		}
		if err := db.Update(def.Name, updated); err != nil {
			return nil, err
		}
		written = append(written, updated)
	}
	return written, nil
}

// returningResult projects written rows for a RETURNING clause.
func returningResult(def table.TableDef, ret *Returning, rows []table.Record) (Result, error) {
	columns, err := selectColumns(def, &SelectStmt{AllColumns: ret.All, Columns: ret.Columns})
	if err != nil {
		return Result{}, err
	}
	out := Result{Columns: columns, RowsAffected: len(rows)}
	for _, row := range rows {
		out.Rows = append(out.Rows, projectRow(row, columns))
	}
	return out, nil
}
