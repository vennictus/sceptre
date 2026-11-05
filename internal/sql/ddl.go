package sql

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/vennictus/sceptre/internal/table"
)

func executeDropTable(db *table.DB, stmt *DropTableStmt) (Result, error) {
	err := db.DropTable(stmt.Name)
	if stmt.IfExists && errors.Is(err, table.ErrTableNotFound) {
		return Result{}, nil
	}
	return Result{}, err
}

func executeDropIndex(db *table.DB, stmt *DropIndexStmt) (Result, error) {
	tableName := stmt.Table
	if tableName == "" {
		defs, err := db.Tables()
		if err != nil {
			return Result{}, err
		}
		for _, def := range defs {
			for _, index := range def.Indexes {
				if index.Name != stmt.Name {
					continue
				}
				if tableName != "" {
					return Result{}, fmt.Errorf("%w: index %s exists on %s and %s; use DROP INDEX %s ON table", ErrExec, stmt.Name, tableName, def.Name, stmt.Name)
				}
				tableName = def.Name
			}
		}
		if tableName == "" {
			if stmt.IfExists {
				return Result{}, nil
			}
			return Result{}, fmt.Errorf("%w: %s", table.ErrIndexNotFound, stmt.Name)
		}
	}
	err := db.DropIndex(tableName, stmt.Name)
	if stmt.IfExists && errors.Is(err, table.ErrIndexNotFound) {
		return Result{}, nil
	}
	return Result{}, err
}

func executeAlterTable(db *table.DB, stmt *AlterTableStmt) (Result, error) {
	switch {
	case stmt.Add != nil:
		return executeAddColumn(db, stmt.Table, stmt.Add)
	case stmt.Drop != "":
		def, ok, err := db.Table(stmt.Table)
		if err != nil {
			return Result{}, err
		}
		if ok {
			if refs := checksReferencing(def, stmt.Drop); len(refs) > 0 {
				return Result{}, fmt.Errorf("%w: column %s is used by CHECK %s", ErrExec, stmt.Drop, refs[0])
			}
		}
		return Result{}, db.DropColumn(stmt.Table, stmt.Drop)
	case stmt.RenameFrom != "":
		def, ok, err := db.Table(stmt.Table)
		if err != nil {
			return Result{}, err
		}
		var checks []string
		if ok && len(def.Checks) > 0 {
			// Rewrite and validate the checks before changing anything.
			renamed := def
			renamed.Columns = slices.Clone(def.Columns)
			for i := range renamed.Columns {
				if renamed.Columns[i].Name == stmt.RenameFrom {
					renamed.Columns[i].Name = stmt.RenameTo
				}
			}
			if checks, err = renameInChecks(renamed, stmt.RenameFrom, stmt.RenameTo); err != nil {
				return Result{}, err
			}
		}
		if err := db.RenameColumn(stmt.Table, stmt.RenameFrom, stmt.RenameTo); err != nil {
			return Result{}, err
		}
		if checks == nil {
			return Result{}, nil
		}
		return Result{}, db.SetChecks(stmt.Table, checks)
	default:
		return Result{}, db.RenameTable(stmt.Table, stmt.RenameTo)
	}
}

// executeAddColumn adds a column with every constraint its definition names,
// or fails without changing the table. Existing rows read the column's
// default, so they must satisfy its CHECKs and UNIQUE like new rows.
func executeAddColumn(db *table.DB, tableName string, add *ColumnDef) (Result, error) {
	if add.PrimaryKey {
		return Result{}, fmt.Errorf("%w: ALTER TABLE cannot add a PRIMARY KEY column", ErrExec)
	}
	column, err := tableColumn(*add, false)
	if err != nil {
		return Result{}, err
	}
	if column.DefaultNow {
		// Existing rows read the time of the ALTER, as in PostgreSQL.
		now := table.TimestampValue(time.Now())
		column.Default = &now
	}
	if !column.Nullable && column.Default == nil {
		return Result{}, fmt.Errorf("%w: an added NOT NULL column needs a DEFAULT", ErrExec)
	}
	def, ok, err := db.Table(tableName)
	if err != nil {
		return Result{}, err
	}
	if !ok {
		return Result{}, fmt.Errorf("%w: %s", table.ErrTableNotFound, tableName)
	}
	next := def
	next.Columns = append(slices.Clone(def.Columns), column)
	added, err := checkStrings(next, add.Checks)
	if err != nil {
		return Result{}, err
	}
	if err := db.AddColumn(tableName, column); err != nil {
		return Result{}, err
	}
	if add.Unique {
		index := table.IndexDef{Name: tableName + "_" + add.Name + "_key", Columns: []string{add.Name}, Unique: true}
		if err := db.CreateIndex(tableName, index); err != nil {
			return Result{}, err
		}
	}
	if len(added) == 0 {
		return Result{}, nil
	}
	if err := db.SetChecks(tableName, append(slices.Clone(def.Checks), added...)); err != nil {
		return Result{}, err
	}
	rows, err := scanRows(db, tableName, table.ScanBounds{})
	if err != nil {
		return Result{}, err
	}
	only := next
	only.Checks = added
	checks, err := prepareChecks(only)
	if err != nil {
		return Result{}, err
	}
	for _, row := range rows {
		if err := checks.check(row); err != nil {
			return Result{}, err
		}
	}
	return Result{}, nil
}
