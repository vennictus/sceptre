package sql

import (
	"errors"
	"fmt"
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
		column, err := tableColumn(*stmt.Add, false)
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
		if err := db.AddColumn(stmt.Table, column); err != nil {
			return Result{}, err
		}
		if stmt.Add.Unique {
			index := table.IndexDef{Name: stmt.Table + "_" + stmt.Add.Name + "_key", Columns: []string{stmt.Add.Name}, Unique: true}
			return Result{}, db.CreateIndex(stmt.Table, index)
		}
		return Result{}, nil
	case stmt.Drop != "":
		return Result{}, db.DropColumn(stmt.Table, stmt.Drop)
	case stmt.RenameFrom != "":
		return Result{}, db.RenameColumn(stmt.Table, stmt.RenameFrom, stmt.RenameTo)
	default:
		return Result{}, db.RenameTable(stmt.Table, stmt.RenameTo)
	}
}
