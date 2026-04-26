package sql

import (
	"fmt"
	"github.com/vennictus/sceptre/internal/table"
	"time"
)

type AnalyzeStage struct {
	Name     string
	RowsIn   int
	RowsOut  int
	Duration time.Duration
}

type AnalyzeReport struct {
	Plan         Plan
	RowsScanned  int
	RowsMatched  int
	RowsReturned int
	TotalTime    time.Duration
	Stages       []AnalyzeStage
}

// Analyze parses and runs a SELECT statement while collecting execution counters.
func Analyze(db *table.DB, input string) (AnalyzeReport, error) {
	stmt, err := Parse(input)
	if err != nil {
		return AnalyzeReport{}, err
	}
	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		return AnalyzeReport{}, fmt.Errorf("%w: analyze only supports select", ErrExec)
	}
	return AnalyzeSelect(db, selectStmt)
}

// AnalyzeSelect runs a parsed SELECT statement and reports the chosen path and row flow.
func AnalyzeSelect(db *table.DB, stmt *SelectStmt) (AnalyzeReport, error) {
	totalStart := time.Now()
	plan, def, err := planQuery(db, "select", stmt.Table, stmt.Where)
	if err != nil {
		return AnalyzeReport{}, err
	}
	plan.Limit = stmt.Limit
	plan.Offset = stmt.Offset
	if _, err := selectColumns(def, stmt); err != nil {
		return AnalyzeReport{}, err
	}

	candidateStart := time.Now()
	rows, err := candidateRows(db, def, plan)
	candidateDuration := time.Since(candidateStart)
	if err != nil {
		return AnalyzeReport{}, err
	}

	filterStart := time.Now()
	offset := int64(0)
	if stmt.Offset != nil {
		offset = *stmt.Offset
	}
	limit := int64(-1)
	if stmt.Limit != nil {
		limit = *stmt.Limit
	}

	matched := 0
	returned := 0
	for _, row := range rows {
		matches, err := evalWhere(stmt.Where, row)
		if err != nil {
			return AnalyzeReport{}, err
		}
		if !matches {
			continue
		}
		matched++
		if offset > 0 {
			offset--
			continue
		}
		if limit == 0 {
			continue
		}
		returned++
		if limit > 0 {
			limit--
		}
	}
	filterDuration := time.Since(filterStart)

	report := AnalyzeReport{
		Plan:         plan,
		RowsScanned:  len(rows),
		RowsMatched:  matched,
		RowsReturned: returned,
		TotalTime:    time.Since(totalStart),
		Stages: []AnalyzeStage{
			{Name: string(plan.Access), RowsIn: -1, RowsOut: len(rows), Duration: candidateDuration},
			{Name: "filter_project", RowsIn: len(rows), RowsOut: returned, Duration: filterDuration},
		},
	}
	return report, nil
}
