package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sceptre/internal/debug"
	"sceptre/internal/sql"
	"sceptre/internal/table"
	"sort"
	"strconv"
	"strings"
	"time"
)

const usage = `sceptre is an embedded relational database engine.

Usage:
  sceptre
  sceptre help
  sceptre demo [db-path] [--rows <n>] [--force]
  sceptre sql <db-path> "<statement>"
  sceptre shell <db-path>
  sceptre explain <db-path> "<statement>"
  sceptre explain-analyze <db-path> "<select>"
  sceptre trace <db-path> "<select>"
  sceptre check <db-path>
  sceptre crash-test <db-path> [--random <n>] [--seed <n>]
  sceptre inspect meta <db-path>
  sceptre inspect tree <db-path>
  sceptre inspect freelist <db-path>
  sceptre inspect schema <db-path>
  sceptre inspect table <db-path> <table>
  sceptre inspect index <db-path> <index>
  sceptre inspect pages <db-path>
  sceptre inspect page <db-path> <page-id>
`

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stdout, usage)
		return 0
	}

	switch args[0] {
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	case "demo":
		return runDemo(args[1:], stdout, stderr)
	case "sql":
		return runSQL(args[1:], stdout, stderr)
	case "shell":
		return runShell(args[1:], os.Stdin, stdout, stderr)
	case "explain":
		return runExplain(args[1:], stdout, stderr)
	case "explain-analyze":
		return runExplainAnalyze(args[1:], stdout, stderr)
	case "trace":
		return runTrace(args[1:], stdout, stderr)
	case "check":
		return runCheck(args[1:], stdout, stderr)
	case "crash-test":
		return runCrashTest(args[1:], stdout, stderr)
	case "inspect":
		return runInspect(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "sceptre: unknown command %q\n\n", args[0])
		fmt.Fprint(stderr, usage)
		return 2
	}
}

type demoOptions struct {
	path  string
	rows  int
	force bool
}

func runDemo(args []string, stdout, stderr io.Writer) int {
	opts, err := parseDemoArgs(args)
	if err != nil {
		fmt.Fprintf(stderr, "sceptre demo: %v\n\n", err)
		fmt.Fprint(stderr, usage)
		return 2
	}
	if _, err := os.Stat(opts.path); err == nil && !opts.force {
		fmt.Fprintf(stderr, "sceptre demo: %s already exists; pass --force to replace it\n", opts.path)
		return 2
	} else if err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(stderr, "sceptre demo: stat database: %v\n", err)
		return 1
	}
	if opts.force {
		if err := os.Remove(opts.path); err != nil && !os.IsNotExist(err) {
			fmt.Fprintf(stderr, "sceptre demo: remove old database: %v\n", err)
			return 1
		}
	}

	fmt.Fprintln(stdout, "Sceptre guided demo")
	fmt.Fprintf(stdout, "database=%s\n", opts.path)
	fmt.Fprintf(stdout, "rows=%d\n", opts.rows)
	fmt.Fprintln(stdout)

	db, err := table.Open(opts.path, table.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "sceptre demo: open: %v\n", err)
		return 1
	}

	printDemoSection(stdout, "1. create schema")
	if err := db.CreateTable(table.TableDef{
		Name: "users",
		Columns: []table.Column{
			{Name: "id", Type: table.TypeInt64},
			{Name: "name", Type: table.TypeBytes},
			{Name: "age", Type: table.TypeInt64},
			{Name: "city", Type: table.TypeBytes},
		},
		PrimaryKey: []string{"id"},
	}); err != nil {
		db.Close()
		fmt.Fprintf(stderr, "sceptre demo: create table: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "created table users(id, name, age, city)")

	printDemoSection(stdout, "2. load data")
	insertStart := time.Now()
	progressEvery := demoProgressEvery(opts.rows)
	batchSize := demoBatchSize(opts.rows)
	fmt.Fprintf(stdout, "loading generated rows; no input required\n")
	fmt.Fprintf(stdout, "batch_size=%d\n", batchSize)
	fmt.Fprintf(stdout, "loaded=0/%d\n", opts.rows)
	for start := 1; start <= opts.rows; start += batchSize {
		end := start + batchSize - 1
		if end > opts.rows {
			end = opts.rows
		}
		batch := make([]table.Record, 0, end-start+1)
		for i := start; i <= end; i++ {
			batch = append(batch, demoUserRecord(i))
		}
		if err := db.InsertMany("users", batch); err != nil {
			db.Close()
			fmt.Fprintf(stderr, "sceptre demo: insert rows %d-%d: %v\n", start, end, err)
			return 1
		}
		if end%progressEvery == 0 || end == opts.rows {
			fmt.Fprintf(stdout, "loaded=%d/%d\n", end, opts.rows)
		}
	}
	insertDuration := time.Since(insertStart)
	fmt.Fprintf(stdout, "inserted=%d time=%s\n", opts.rows, insertDuration)

	query := "select id, name, age from users where age = 42"
	printDemoSection(stdout, "3. query before index")
	before, err := sql.Analyze(db, query)
	if err != nil {
		db.Close()
		fmt.Fprintf(stderr, "sceptre demo: analyze before index: %v\n", err)
		return 1
	}
	printDemoAnalyze(stdout, before)

	printDemoSection(stdout, "4. create index")
	indexStart := time.Now()
	if err := db.CreateIndex("users", table.IndexDef{Name: "users_age", Columns: []string{"age"}}); err != nil {
		db.Close()
		fmt.Fprintf(stderr, "sceptre demo: create index: %v\n", err)
		return 1
	}
	indexDuration := time.Since(indexStart)
	fmt.Fprintf(stdout, "created index users_age(age) time=%s\n", indexDuration)

	printDemoSection(stdout, "5. query after index")
	after, err := sql.Analyze(db, query)
	if err != nil {
		db.Close()
		fmt.Fprintf(stderr, "sceptre demo: analyze after index: %v\n", err)
		return 1
	}
	printDemoAnalyze(stdout, after)

	printDemoSection(stdout, "6. performance comparison")
	printStringTable(stdout, []string{"query", "access", "rows_scanned", "time"}, [][]string{
		{"without_index", string(before.Plan.Access), fmt.Sprintf("%d", before.RowsScanned), before.TotalTime.String()},
		{"with_index", string(after.Plan.Access), fmt.Sprintf("%d", after.RowsScanned), after.TotalTime.String()},
	})
	fmt.Fprintf(stdout, "takeaway: index reduced scanned rows from %d to %d (%s fewer rows scanned)\n", before.RowsScanned, after.RowsScanned, formatReduction(before.RowsScanned, after.RowsScanned))
	fmt.Fprintf(stdout, "takeaway: query time changed from %s to %s (%s faster)\n", before.TotalTime, after.TotalTime, formatDurationRatio(before.TotalTime, after.TotalTime))

	printDemoSection(stdout, "7. delete data and show freelist")
	deleteCount := opts.rows * 30 / 100
	deleteStart := time.Now()
	deleted := 0
	for start := 1; start <= deleteCount; start += batchSize {
		end := start + batchSize - 1
		if end > deleteCount {
			end = deleteCount
		}
		keys := make([]table.Record, 0, end-start+1)
		for i := start; i <= end; i++ {
			keys = append(keys, table.NewRecord(map[string]table.Value{"id": table.Int64Value(int64(i))}))
		}
		removed, err := db.DeleteMany("users", keys)
		if err != nil {
			db.Close()
			fmt.Fprintf(stderr, "sceptre demo: delete rows %d-%d: %v\n", start, end, err)
			return 1
		}
		deleted += removed
	}
	deleteDuration := time.Since(deleteStart)
	fmt.Fprintf(stdout, "deleted=%d time=%s\n", deleted, deleteDuration)
	fmt.Fprintln(stdout, "freelist: deleted rows retire old pages; committed free pages can be reused by future inserts.")
	fmt.Fprintln(stdout, "freelist: this keeps the file reusable instead of treating every delete as lost space.")
	if err := db.Close(); err != nil {
		fmt.Fprintf(stderr, "sceptre demo: close: %v\n", err)
		return 1
	}

	free, err := debug.InspectFreeList(opts.path)
	if err != nil {
		fmt.Fprintf(stderr, "sceptre demo: inspect freelist: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "head_page=%d\n", free.HeadPage)
	fmt.Fprintf(stdout, "freelist_pages=%d\n", len(free.PageIDs))
	fmt.Fprintf(stdout, "free_page_count=%d\n", len(free.FreePages))
	if len(free.FreePages) > 0 {
		fmt.Fprintf(stdout, "free_pages_sample=%v\n", sampleUint64s(free.FreePages, 12))
	}

	printDemoSection(stdout, "8. inspect storage")
	pages, err := debug.InspectPages(opts.path)
	if err != nil {
		fmt.Fprintf(stderr, "sceptre demo: inspect pages: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "page_size=%d\n", pages.PageSize)
	fmt.Fprintf(stdout, "page_count=%d\n", pages.PageCount)
	for _, page := range samplePages(pages.Pages, 8) {
		fmt.Fprintf(stdout, "page=%d kind=%s cells=%d free_bytes=%d\n", page.ID, page.Kind, page.Cells, page.FreeBytes)
	}
	pageID := firstInspectablePage(pages.Pages)
	detail, err := debug.InspectPage(opts.path, pageID)
	if err != nil {
		fmt.Fprintf(stderr, "sceptre demo: inspect page: %v\n", err)
		return 1
	}
	printDemoPageDetail(stdout, detail, 8)
	if err := printDemoLogicalRows(stdout, opts.path, 5); err != nil {
		fmt.Fprintf(stderr, "sceptre demo: inspect logical rows: %v\n", err)
		return 1
	}

	printDemoSection(stdout, "9. validate and recover")
	checkDB, err := table.Open(opts.path, table.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "sceptre demo: reopen for check: %v\n", err)
		return 1
	}
	check, err := checkDB.Check()
	closeErr := checkDB.Close()
	if err != nil {
		fmt.Fprintf(stderr, "sceptre demo: check: %v\n", err)
		return 1
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "sceptre demo: close after check: %v\n", closeErr)
		return 1
	}
	printCheckResult(stdout, check)
	crashPath := filepath.Join(filepath.Dir(opts.path), strings.TrimSuffix(filepath.Base(opts.path), filepath.Ext(opts.path))+".crash.db")
	crash, err := debug.CrashTest(crashPath)
	if err != nil {
		fmt.Fprintf(stderr, "sceptre demo: crash-test: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "crash_recovery=%s cases=%d\n", demoStatus(crash.OK()), len(crash.Cases))

	printDemoSection(stdout, "summary")
	fmt.Fprintf(stdout, "data: loaded %d rows, then deleted %d rows\n", opts.rows, deleted)
	fmt.Fprintf(stdout, "index: %s scanned %d rows; %s scanned %d rows (%s reduction)\n", before.Plan.Access, before.RowsScanned, after.Plan.Access, after.RowsScanned, formatReduction(before.RowsScanned, after.RowsScanned))
	fmt.Fprintf(stdout, "performance: query time improved from %s to %s (%s faster)\n", before.TotalTime, after.TotalTime, formatDurationRatio(before.TotalTime, after.TotalTime))
	fmt.Fprintf(stdout, "storage: freelist has %d metadata page(s) tracking %d reusable page(s)\n", len(free.PageIDs), len(free.FreePages))
	fmt.Fprintf(stdout, "reliability: consistency check %s; crash recovery %s across %d cases\n", demoStatus(check.OK()), demoStatus(crash.OK()), len(crash.Cases))
	return 0
}

func runSQL(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprint(stderr, "sceptre sql: expected <db-path> and <statement>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}

	db, err := table.Open(args[0], table.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "sceptre sql: open: %v\n", err)
		return 1
	}
	defer db.Close()

	result, err := sql.Execute(db, strings.Join(args[1:], " "))
	if err != nil {
		fmt.Fprintf(stderr, "sceptre sql: execute: %v\n", err)
		return 1
	}
	printSQLResult(stdout, result)
	return 0
}

func parseDemoArgs(args []string) (demoOptions, error) {
	opts := demoOptions{
		path: "sceptre-demo.db",
		rows: 100000,
	}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--force":
			opts.force = true
		case arg == "--rows":
			if i+1 >= len(args) {
				return demoOptions{}, fmt.Errorf("--rows expects a value")
			}
			rows, err := strconv.Atoi(args[i+1])
			if err != nil || rows <= 0 {
				return demoOptions{}, fmt.Errorf("--rows must be a positive integer")
			}
			opts.rows = rows
			i++
		case strings.HasPrefix(arg, "--rows="):
			rows, err := strconv.Atoi(strings.TrimPrefix(arg, "--rows="))
			if err != nil || rows <= 0 {
				return demoOptions{}, fmt.Errorf("--rows must be a positive integer")
			}
			opts.rows = rows
		case strings.HasPrefix(arg, "-"):
			return demoOptions{}, fmt.Errorf("unknown option %q", arg)
		default:
			if opts.path != "sceptre-demo.db" {
				return demoOptions{}, fmt.Errorf("expected at most one database path")
			}
			opts.path = arg
		}
	}
	return opts, nil
}

func demoUserRecord(id int) table.Record {
	cities := []string{"delhi", "mumbai", "blr", "pune", "nyc", "london", "tokyo", "berlin"}
	return table.NewRecord(map[string]table.Value{
		"id":   table.Int64Value(int64(id)),
		"name": table.BytesValue([]byte(fmt.Sprintf("user_%06d", id))),
		"age":  table.Int64Value(int64(18 + id%70)),
		"city": table.BytesValue([]byte(cities[id%len(cities)])),
	})
}

func demoProgressEvery(rows int) int {
	switch {
	case rows <= 100:
		return rows
	case rows <= 1000:
		return 100
	case rows <= 10000:
		return 500
	default:
		return rows / 10
	}
}

func demoBatchSize(rows int) int {
	switch {
	case rows <= 100:
		return rows
	case rows <= 1000:
		return 100
	default:
		return 10000
	}
}

func printDemoSection(stdout io.Writer, title string) {
	fmt.Fprintln(stdout)
	fmt.Fprintf(stdout, "== %s ==\n", title)
}

func printDemoPageDetail(stdout io.Writer, info debug.PageDetailInfo, cellLimit int) {
	fmt.Fprintf(stdout, "page=%d\n", info.ID)
	fmt.Fprintf(stdout, "kind=%s\n", info.Kind)
	fmt.Fprintf(stdout, "page_size=%d\n", info.PageSize)
	if info.Meta != nil {
		fmt.Fprintf(stdout, "root_page=%d\n", info.Meta.RootPage)
		fmt.Fprintf(stdout, "freelist_page=%d\n", info.Meta.FreeListPage)
		fmt.Fprintf(stdout, "page_count=%d\n", info.Meta.PageCount)
		fmt.Fprintf(stdout, "generation=%d\n", info.Meta.Generation)
		return
	}
	if info.Kind == "freelist_head" || info.Kind == "freelist" {
		fmt.Fprintf(stdout, "next_page=%d\n", info.NextPage)
		fmt.Fprintf(stdout, "free_page_count=%d\n", len(info.FreePages))
		fmt.Fprintf(stdout, "free_pages_sample=%v\n", sampleUint64s(info.FreePages, cellLimit))
		return
	}
	if info.Kind != "btree_leaf" && info.Kind != "btree_internal" {
		return
	}
	fmt.Fprintf(stdout, "cells=%d\n", info.Cells)
	fmt.Fprintf(stdout, "lower=%d\n", info.Lower)
	fmt.Fprintf(stdout, "upper=%d\n", info.Upper)
	fmt.Fprintf(stdout, "free_bytes=%d\n", info.FreeBytes)
	limit := cellLimit
	if len(info.BTreeCells) < limit {
		limit = len(info.BTreeCells)
	}
	for _, cell := range info.BTreeCells[:limit] {
		if info.Kind == "btree_internal" {
			fmt.Fprintf(stdout, "cell=%d child=%d key=%s\n", cell.Index, cell.Child, formatBytes(cell.Key))
			continue
		}
		fmt.Fprintf(stdout, "cell=%d key=%s value=%s\n", cell.Index, formatBytes(cell.Key), formatBytes(cell.Value))
	}
	if len(info.BTreeCells) > limit {
		fmt.Fprintf(stdout, "cells_omitted=%d\n", len(info.BTreeCells)-limit)
	}
	fmt.Fprintln(stdout, "note: raw cells are encoded storage records; decoded logical rows are shown below.")
}

func printDemoAnalyze(stdout io.Writer, report sql.AnalyzeReport) {
	fmt.Fprintf(stdout, "access=%s\n", report.Plan.Access)
	if report.Plan.Index != "" {
		fmt.Fprintf(stdout, "index=%s\n", report.Plan.Index)
	}
	fmt.Fprintf(stdout, "rows_scanned=%d\n", report.RowsScanned)
	fmt.Fprintf(stdout, "rows_matched=%d\n", report.RowsMatched)
	fmt.Fprintf(stdout, "rows_returned=%d\n", report.RowsReturned)
	fmt.Fprintf(stdout, "total_time=%s\n", report.TotalTime)
	rows := make([][]string, 0, len(report.Stages))
	for _, stage := range report.Stages {
		rows = append(rows, []string{stage.Name, formatStageRowsIn(stage), fmt.Sprintf("%d", stage.RowsOut), stage.Duration.String()})
	}
	printStringTable(stdout, []string{"stage", "rows_in", "rows_out", "time"}, rows)
}

func printDemoLogicalRows(stdout io.Writer, path string, limit int) error {
	info, err := debug.InspectTable(path, "users")
	if err != nil {
		return err
	}
	fmt.Fprintln(stdout, "decoded logical row sample")
	count := limit
	if len(info.Rows) < count {
		count = len(info.Rows)
	}
	for i := 0; i < count; i++ {
		fmt.Fprintf(stdout, "row=%s\n", formatRecord(info.Columns, info.Rows[i]))
	}
	if len(info.Rows) > count {
		fmt.Fprintf(stdout, "rows_omitted=%d\n", len(info.Rows)-count)
	}
	return nil
}

func sampleUint64s(values []uint64, limit int) []uint64 {
	if len(values) <= limit {
		return append([]uint64(nil), values...)
	}
	return append([]uint64(nil), values[:limit]...)
}

func samplePages(pages []debug.PageInfo, limit int) []debug.PageInfo {
	if len(pages) <= limit {
		return append([]debug.PageInfo(nil), pages...)
	}
	return append([]debug.PageInfo(nil), pages[:limit]...)
}

func firstInspectablePage(pages []debug.PageInfo) uint64 {
	for _, page := range pages {
		if page.Kind == "btree_leaf" || page.Kind == "btree_internal" || page.Kind == "freelist_head" || page.Kind == "freelist" {
			return page.ID
		}
	}
	return 0
}

func demoStatus(ok bool) string {
	if ok {
		return "ok"
	}
	return "failed"
}

func formatStageRowsIn(stage sql.AnalyzeStage) string {
	if stage.RowsIn < 0 {
		return "-"
	}
	return fmt.Sprintf("%d", stage.RowsIn)
}

func formatReduction(before, after int) string {
	if before <= 0 || after <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1fx", float64(before)/float64(after))
}

func formatDurationRatio(before, after time.Duration) string {
	if before <= 0 || after <= 0 {
		return "n/a"
	}
	return fmt.Sprintf("%.1fx", float64(before)/float64(after))
}

func runShell(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprint(stderr, "sceptre shell: expected <db-path>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}

	db, err := table.Open(args[0], table.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "sceptre shell: open: %v\n", err)
		return 1
	}
	defer db.Close()

	scanner := bufio.NewScanner(stdin)
	showPrompt := isTerminalInput(stdin)
	for {
		if showPrompt {
			fmt.Fprint(stdout, "sceptre> ")
		}
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == ".quit" || line == ".exit" {
			return 0
		}
		if strings.HasPrefix(line, ".") {
			if err := runShellCommand(db, line, stdout); err != nil {
				fmt.Fprintf(stderr, "sceptre shell: %v\n", err)
			}
			continue
		}

		result, err := sql.Execute(db, trimSQLTerminator(line))
		if err != nil {
			fmt.Fprintf(stderr, "sceptre shell: execute: %v\n", err)
			continue
		}
		printSQLResult(stdout, result)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(stderr, "sceptre shell: read: %v\n", err)
		return 1
	}
	return 0
}

func runExplain(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprint(stderr, "sceptre explain: expected <db-path> and <statement>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}

	db, err := table.Open(args[0], table.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "sceptre explain: open: %v\n", err)
		return 1
	}
	defer db.Close()

	plan, err := sql.Explain(db, strings.Join(args[1:], " "))
	if err != nil {
		fmt.Fprintf(stderr, "sceptre explain: %v\n", err)
		return 1
	}
	printExplainResult(stdout, plan)
	return 0
}

func runExplainAnalyze(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprint(stderr, "sceptre explain-analyze: expected <db-path> and <select>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}

	db, err := table.Open(args[0], table.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "sceptre explain-analyze: open: %v\n", err)
		return 1
	}
	defer db.Close()

	report, err := sql.Analyze(db, strings.Join(args[1:], " "))
	if err != nil {
		fmt.Fprintf(stderr, "sceptre explain-analyze: %v\n", err)
		return 1
	}
	printAnalyzeReport(stdout, report)
	return 0
}

func runTrace(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprint(stderr, "sceptre trace: expected <db-path> and <select>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}

	db, err := table.Open(args[0], table.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "sceptre trace: open: %v\n", err)
		return 1
	}
	defer db.Close()

	report, err := sql.Analyze(db, strings.Join(args[1:], " "))
	if err != nil {
		fmt.Fprintf(stderr, "sceptre trace: %v\n", err)
		return 1
	}
	printTraceReport(stdout, report)
	return 0
}

func runShellCommand(db *table.DB, line string, stdout io.Writer) error {
	switch line {
	case ".help":
		printShellHelp(stdout)
		return nil
	case ".tables":
		tables, err := db.Tables()
		if err != nil {
			return err
		}
		for _, def := range tables {
			fmt.Fprintln(stdout, def.Name)
		}
		return nil
	case ".schema":
		tables, err := db.Tables()
		if err != nil {
			return err
		}
		for _, def := range tables {
			printSchema(stdout, def)
		}
		return nil
	case ".indexes":
		tables, err := db.Tables()
		if err != nil {
			return err
		}
		rows := [][]string{}
		for _, def := range tables {
			for _, index := range def.Indexes {
				rows = append(rows, []string{index.Name, def.Name, strings.Join(index.Columns, ",")})
			}
		}
		printStringTable(stdout, []string{"index", "table", "columns"}, rows)
		return nil
	default:
		return fmt.Errorf("unknown shell command %q", line)
	}
}

func runCheck(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprint(stderr, "sceptre check: expected <db-path>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}

	db, err := table.Open(args[0], table.Options{})
	if err != nil {
		fmt.Fprintf(stderr, "sceptre check: open: %v\n", err)
		return 1
	}
	defer db.Close()

	report, err := db.Check()
	if err != nil {
		fmt.Fprintf(stderr, "sceptre check: %v\n", err)
		return 1
	}
	printCheckResult(stdout, report)
	if !report.OK() {
		return 1
	}
	return 0
}

func runCrashTest(args []string, stdout, stderr io.Writer) int {
	opts, err := parseCrashTestArgs(args)
	if err != nil {
		fmt.Fprintf(stderr, "sceptre crash-test: %v\n\n", err)
		fmt.Fprint(stderr, usage)
		return 2
	}

	var report debug.CrashReport
	if opts.randomCases > 0 {
		report, err = debug.RandomCrashTest(opts.path, opts.randomCases, opts.seed)
	} else {
		report, err = debug.CrashTest(opts.path)
	}
	if err != nil {
		fmt.Fprintf(stderr, "sceptre crash-test: %v\n", err)
		return 1
	}
	printCrashReport(stdout, report)
	if !report.OK() {
		return 1
	}
	return 0
}

type crashTestOptions struct {
	path        string
	randomCases int
	seed        int64
}

func parseCrashTestArgs(args []string) (crashTestOptions, error) {
	if len(args) == 0 {
		return crashTestOptions{}, fmt.Errorf("expected <db-path>")
	}
	opts := crashTestOptions{path: args[0]}
	for i := 1; i < len(args); i++ {
		switch args[i] {
		case "--random":
			if i+1 >= len(args) {
				return crashTestOptions{}, fmt.Errorf("--random expects a case count")
			}
			cases, err := strconv.Atoi(args[i+1])
			if err != nil || cases <= 0 {
				return crashTestOptions{}, fmt.Errorf("--random must be a positive integer")
			}
			opts.randomCases = cases
			i++
		case "--seed":
			if i+1 >= len(args) {
				return crashTestOptions{}, fmt.Errorf("--seed expects a value")
			}
			seed, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return crashTestOptions{}, fmt.Errorf("--seed must be an integer")
			}
			opts.seed = seed
			i++
		default:
			return crashTestOptions{}, fmt.Errorf("unknown option %q", args[i])
		}
	}
	if opts.seed != 0 && opts.randomCases == 0 {
		return crashTestOptions{}, fmt.Errorf("--seed requires --random")
	}
	return opts, nil
}

func runInspect(args []string, stdout, stderr io.Writer) int {
	if len(args) < 2 {
		fmt.Fprint(stderr, "sceptre inspect: expected target and <db-path>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}

	switch args[0] {
	case "meta":
		if len(args) != 2 {
			fmt.Fprint(stderr, "sceptre inspect meta: expected <db-path>\n\n")
			fmt.Fprint(stderr, usage)
			return 2
		}
		info, err := debug.InspectMeta(args[1])
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect meta: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "path=%s\n", info.Path)
		fmt.Fprintf(stdout, "page_size=%d\n", info.PageSize)
		fmt.Fprintf(stdout, "root_page=%d\n", info.RootPage)
		fmt.Fprintf(stdout, "freelist_page=%d\n", info.FreeListPage)
		fmt.Fprintf(stdout, "page_count=%d\n", info.PageCount)
		fmt.Fprintf(stdout, "generation=%d\n", info.Generation)
		fmt.Fprintf(stdout, "active_meta_slot=%d\n", info.ActiveSlot)
		return 0
	case "tree":
		if len(args) != 2 {
			fmt.Fprint(stderr, "sceptre inspect tree: expected <db-path>\n\n")
			fmt.Fprint(stderr, usage)
			return 2
		}
		info, err := debug.InspectTree(args[1])
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect tree: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "root_page=%d\n", info.RootPage)
		fmt.Fprintf(stdout, "page_count=%d\n", info.PageCount)
		fmt.Fprintf(stdout, "entries=%d\n", len(info.Entries))
		for _, entry := range info.Entries {
			fmt.Fprintf(stdout, "%s\t%s\n", formatBytes(entry.Key), formatBytes(entry.Value))
		}
		return 0
	case "freelist":
		if len(args) != 2 {
			fmt.Fprint(stderr, "sceptre inspect freelist: expected <db-path>\n\n")
			fmt.Fprint(stderr, usage)
			return 2
		}
		info, err := debug.InspectFreeList(args[1])
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect freelist: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "head_page=%d\n", info.HeadPage)
		fmt.Fprintf(stdout, "freelist_pages=%v\n", info.PageIDs)
		fmt.Fprintf(stdout, "free_pages=%v\n", info.FreePages)
		return 0
	case "schema":
		if len(args) != 2 {
			fmt.Fprint(stderr, "sceptre inspect schema: expected <db-path>\n\n")
			fmt.Fprint(stderr, usage)
			return 2
		}
		db, err := table.Open(args[1], table.Options{})
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect schema: %v\n", err)
			return 1
		}
		defer db.Close()
		tables, err := db.Tables()
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect schema: %v\n", err)
			return 1
		}
		for _, def := range tables {
			printSchema(stdout, def)
		}
		return 0
	case "table":
		if len(args) != 3 {
			fmt.Fprint(stderr, "sceptre inspect table: expected <db-path> and <table>\n\n")
			fmt.Fprint(stderr, usage)
			return 2
		}
		info, err := debug.InspectTable(args[1], args[2])
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect table: %v\n", err)
			return 1
		}
		printTableInfo(stdout, info)
		return 0
	case "index":
		if len(args) != 3 {
			fmt.Fprint(stderr, "sceptre inspect index: expected <db-path> and <index>\n\n")
			fmt.Fprint(stderr, usage)
			return 2
		}
		info, err := debug.InspectIndex(args[1], args[2])
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect index: %v\n", err)
			return 1
		}
		printIndexInfo(stdout, info)
		return 0
	case "pages":
		if len(args) != 2 {
			fmt.Fprint(stderr, "sceptre inspect pages: expected <db-path>\n\n")
			fmt.Fprint(stderr, usage)
			return 2
		}
		info, err := debug.InspectPages(args[1])
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect pages: %v\n", err)
			return 1
		}
		printPagesInfo(stdout, info)
		return 0
	case "page":
		if len(args) != 3 {
			fmt.Fprint(stderr, "sceptre inspect page: expected <db-path> and <page-id>\n\n")
			fmt.Fprint(stderr, usage)
			return 2
		}
		pageID, err := strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect page: invalid page id %q\n", args[2])
			return 2
		}
		info, err := debug.InspectPage(args[1], pageID)
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect page: %v\n", err)
			return 1
		}
		printPageDetail(stdout, info)
		return 0
	default:
		fmt.Fprintf(stderr, "sceptre inspect: unknown target %q\n\n", args[0])
		fmt.Fprint(stderr, usage)
		return 2
	}
}

func printTableInfo(stdout io.Writer, info debug.TableInfo) {
	fmt.Fprintf(stdout, "table=%s\n", info.Name)
	fmt.Fprintf(stdout, "columns=%d\n", len(info.Columns))
	for _, column := range info.Columns {
		fmt.Fprintf(stdout, "column=%s type=%s\n", column.Name, formatType(column.Type))
	}
	fmt.Fprintf(stdout, "primary_key=%s\n", strings.Join(info.PrimaryKey, ","))
	fmt.Fprintf(stdout, "indexes=%d\n", len(info.Indexes))
	for _, index := range info.Indexes {
		fmt.Fprintf(stdout, "index=%s columns=%s\n", index.Name, strings.Join(index.Columns, ","))
	}
	fmt.Fprintf(stdout, "rows=%d\n", len(info.Rows))
	for _, row := range info.Rows {
		fmt.Fprintf(stdout, "row=%s\n", formatRecord(info.Columns, row))
	}
}

func printIndexInfo(stdout io.Writer, info debug.IndexInfo) {
	fmt.Fprintf(stdout, "index=%s\n", info.Name)
	fmt.Fprintf(stdout, "table=%s\n", info.Table)
	fmt.Fprintf(stdout, "columns=%s\n", strings.Join(info.Columns, ","))
	fmt.Fprintf(stdout, "entries=%d\n", len(info.Entries))
	for _, entry := range info.Entries {
		fmt.Fprintf(stdout, "entry=%s primary_key=%s\n", formatRecordNames(info.Columns, entry.Values), formatRecordNames(recordNames(entry.PrimaryKey), entry.PrimaryKey))
	}
}

func printPagesInfo(stdout io.Writer, info debug.PagesInfo) {
	fmt.Fprintf(stdout, "page_size=%d\n", info.PageSize)
	fmt.Fprintf(stdout, "page_count=%d\n", info.PageCount)
	for _, page := range info.Pages {
		fmt.Fprintf(stdout, "page=%d kind=%s cells=%d free_bytes=%d\n", page.ID, page.Kind, page.Cells, page.FreeBytes)
	}
}

func printPageDetail(stdout io.Writer, info debug.PageDetailInfo) {
	fmt.Fprintf(stdout, "page=%d\n", info.ID)
	fmt.Fprintf(stdout, "kind=%s\n", info.Kind)
	fmt.Fprintf(stdout, "page_size=%d\n", info.PageSize)

	if info.Meta != nil {
		fmt.Fprintf(stdout, "root_page=%d\n", info.Meta.RootPage)
		fmt.Fprintf(stdout, "freelist_page=%d\n", info.Meta.FreeListPage)
		fmt.Fprintf(stdout, "page_count=%d\n", info.Meta.PageCount)
		fmt.Fprintf(stdout, "generation=%d\n", info.Meta.Generation)
		fmt.Fprintf(stdout, "active_meta_slot=%d\n", info.Meta.ActiveSlot)
		return
	}

	if info.Kind == "freelist_head" || info.Kind == "freelist" {
		fmt.Fprintf(stdout, "next_page=%d\n", info.NextPage)
		fmt.Fprintf(stdout, "free_pages=%v\n", info.FreePages)
		return
	}

	if info.Kind == "btree_leaf" || info.Kind == "btree_internal" {
		fmt.Fprintf(stdout, "cells=%d\n", info.Cells)
		fmt.Fprintf(stdout, "lower=%d\n", info.Lower)
		fmt.Fprintf(stdout, "upper=%d\n", info.Upper)
		fmt.Fprintf(stdout, "free_bytes=%d\n", info.FreeBytes)
		for _, cell := range info.BTreeCells {
			if info.Kind == "btree_internal" {
				fmt.Fprintf(stdout, "cell=%d child=%d key=%s\n", cell.Index, cell.Child, formatBytes(cell.Key))
				continue
			}
			fmt.Fprintf(stdout, "cell=%d key=%s value=%s\n", cell.Index, formatBytes(cell.Key), formatBytes(cell.Value))
		}
	}
}

func printSQLResult(stdout io.Writer, result sql.Result) {
	if len(result.Columns) == 0 {
		fmt.Fprintf(stdout, "OK rows_affected=%d\n", result.RowsAffected)
		return
	}

	rows := make([][]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		cells := make([]string, 0, len(row))
		for _, value := range row {
			cells = append(cells, formatValue(value))
		}
		rows = append(rows, cells)
	}
	printStringTable(stdout, result.Columns, rows)
}

func printExplainResult(stdout io.Writer, plan sql.Plan) {
	fmt.Fprintf(stdout, "statement=%s\n", plan.Statement)
	fmt.Fprintf(stdout, "table=%s\n", plan.Table)
	fmt.Fprintf(stdout, "access=%s\n", plan.Access)
	if plan.Index != "" {
		fmt.Fprintf(stdout, "index=%s\n", plan.Index)
	}
	if len(plan.Lookup) > 0 {
		parts := make([]string, 0, len(plan.Lookup))
		for _, condition := range plan.Lookup {
			parts = append(parts, sqlFormatCondition(condition))
		}
		fmt.Fprintf(stdout, "lookup=%s\n", strings.Join(parts, ", "))
	}
	if plan.Lower != nil {
		fmt.Fprintf(stdout, "lower=%s\n", sqlFormatCondition(*plan.Lower))
	}
	if plan.Upper != nil {
		fmt.Fprintf(stdout, "upper=%s\n", sqlFormatCondition(*plan.Upper))
	}
	if plan.Limit != nil {
		fmt.Fprintf(stdout, "limit=%d\n", *plan.Limit)
	}
	if plan.Offset != nil {
		fmt.Fprintf(stdout, "offset=%d\n", *plan.Offset)
	}
	fmt.Fprintf(stdout, "residual=%s\n", sql.FormatExpr(plan.Residual))
}

func printAnalyzeReport(stdout io.Writer, report sql.AnalyzeReport) {
	fmt.Fprintln(stdout, "plan")
	fmt.Fprintf(stdout, "  statement: %s\n", report.Plan.Statement)
	fmt.Fprintf(stdout, "  table: %s\n", report.Plan.Table)
	fmt.Fprintf(stdout, "  access: %s\n", report.Plan.Access)
	if report.Plan.Index != "" {
		fmt.Fprintf(stdout, "  index: %s\n", report.Plan.Index)
	}
	if len(report.Plan.Lookup) > 0 {
		parts := make([]string, 0, len(report.Plan.Lookup))
		for _, condition := range report.Plan.Lookup {
			parts = append(parts, sqlFormatCondition(condition))
		}
		fmt.Fprintf(stdout, "  lookup: %s\n", strings.Join(parts, ", "))
	}
	if report.Plan.Lower != nil {
		fmt.Fprintf(stdout, "  lower: %s\n", sqlFormatCondition(*report.Plan.Lower))
	}
	if report.Plan.Upper != nil {
		fmt.Fprintf(stdout, "  upper: %s\n", sqlFormatCondition(*report.Plan.Upper))
	}
	fmt.Fprintf(stdout, "  residual: %s\n", sql.FormatExpr(report.Plan.Residual))
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "execution")
	fmt.Fprintf(stdout, "  rows_scanned: %d\n", report.RowsScanned)
	fmt.Fprintf(stdout, "  rows_matched: %d\n", report.RowsMatched)
	fmt.Fprintf(stdout, "  rows_returned: %d\n", report.RowsReturned)
	fmt.Fprintf(stdout, "  total_time: %s\n", report.TotalTime)
	fmt.Fprintln(stdout)
	fmt.Fprintln(stdout, "stages")
	for _, stage := range report.Stages {
		fmt.Fprintf(stdout, "  %s\n", stage.Name)
		if stage.RowsIn >= 0 {
			fmt.Fprintf(stdout, "    rows_in: %d\n", stage.RowsIn)
		}
		fmt.Fprintf(stdout, "    rows_out: %d\n", stage.RowsOut)
		fmt.Fprintf(stdout, "    time: %s\n", stage.Duration)
	}
}

func printTraceReport(stdout io.Writer, report sql.AnalyzeReport) {
	fmt.Fprintf(stdout, "trace table=%s access=%s\n", report.Plan.Table, report.Plan.Access)
	if report.Plan.Index != "" {
		fmt.Fprintf(stdout, "using index=%s\n", report.Plan.Index)
	}
	for i, stage := range report.Stages {
		if stage.RowsIn < 0 {
			fmt.Fprintf(stdout, "%d. %s -> %d row(s) in %s\n", i+1, stage.Name, stage.RowsOut, stage.Duration)
			continue
		}
		fmt.Fprintf(stdout, "%d. %s: %d row(s) in -> %d row(s) out in %s\n", i+1, stage.Name, stage.RowsIn, stage.RowsOut, stage.Duration)
	}
	fmt.Fprintf(stdout, "result: scanned %d, matched %d, returned %d in %s\n", report.RowsScanned, report.RowsMatched, report.RowsReturned, report.TotalTime)
}

func printCheckResult(stdout io.Writer, report table.CheckReport) {
	status := "ok"
	if !report.OK() {
		status = "failed"
	}
	fmt.Fprintf(stdout, "status=%s\n", status)
	fmt.Fprintf(stdout, "tables=%d\n", len(report.Tables))
	for _, table := range report.Tables {
		fmt.Fprintf(stdout, "table=%s rows=%d indexes=%d\n", table.Name, table.Rows, table.Indexes)
	}
	fmt.Fprintf(stdout, "issues=%d\n", len(report.Issues))
	for _, issue := range report.Issues {
		fmt.Fprintf(stdout, "issue=%s detail=%s\n", issue.Code, issue.Detail)
	}
}

func printShellHelp(stdout io.Writer) {
	rows := [][]string{
		{".help", "show shell commands"},
		{".tables", "list tables"},
		{".schema", "show schema statements"},
		{".indexes", "list secondary indexes"},
		{".quit", "exit the shell"},
	}
	printStringTable(stdout, []string{"command", "description"}, rows)
}

func printSchema(stdout io.Writer, def table.TableDef) {
	parts := make([]string, 0, len(def.Columns)+1)
	for _, column := range def.Columns {
		parts = append(parts, column.Name+" "+formatType(column.Type))
	}
	parts = append(parts, "primary key ("+strings.Join(def.PrimaryKey, ", ")+")")
	fmt.Fprintf(stdout, "create table %s (%s)\n", def.Name, strings.Join(parts, ", "))
	for _, index := range def.Indexes {
		fmt.Fprintf(stdout, "create index %s on %s (%s)\n", index.Name, def.Name, strings.Join(index.Columns, ", "))
	}
}

func formatType(valueType table.Type) string {
	switch valueType {
	case table.TypeInt64:
		return "int64"
	case table.TypeBytes:
		return "bytes"
	default:
		return "unknown"
	}
}

func trimSQLTerminator(input string) string {
	return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(input), ";"))
}

func printCrashReport(stdout io.Writer, report debug.CrashReport) {
	status := "ok"
	if !report.OK() {
		status = "failed"
	}
	fmt.Fprintf(stdout, "status=%s\n", status)
	if report.Mode != "" {
		fmt.Fprintf(stdout, "mode=%s\n", report.Mode)
	}
	if report.Seed != 0 {
		fmt.Fprintf(stdout, "seed=%d\n", report.Seed)
	}
	fmt.Fprintf(stdout, "work_dir=%s\n", report.WorkDir)
	fmt.Fprintf(stdout, "cases=%d\n", len(report.Cases))
	for _, crashCase := range report.Cases {
		fmt.Fprintf(
			stdout,
			"case=%s operation=%s recovered=%t check_ok=%t expected_new=%t observed_new=%t issues=%d path=%s\n",
			crashCase.Stage,
			crashCase.Operation,
			crashCase.Recovered,
			crashCase.CheckOK,
			crashCase.ExpectedNew,
			crashCase.ObservedNew,
			crashCase.Issues,
			crashCase.Path,
		)
	}
}

func formatValue(value table.Value) string {
	switch value.Type {
	case table.TypeInt64:
		return fmt.Sprintf("%d", value.I64)
	case table.TypeBytes:
		return string(value.Bytes)
	default:
		return "<invalid>"
	}
}

func formatRecord(columns []table.Column, record table.Record) string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.Name)
	}
	return formatRecordNames(names, record)
}

func formatRecordNames(names []string, record table.Record) string {
	parts := make([]string, 0, len(names))
	for _, name := range names {
		value, ok := record.Values[name]
		if !ok {
			continue
		}
		parts = append(parts, name+"="+formatValue(value))
	}
	return strings.Join(parts, ",")
}

func recordNames(record table.Record) []string {
	names := make([]string, 0, len(record.Values))
	for name := range record.Values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func formatBytes(value []byte) string {
	if len(value) == 0 {
		return `""`
	}
	return fmt.Sprintf("%q", string(value))
}

func printStringTable(stdout io.Writer, columns []string, rows [][]string) {
	widths := make([]int, len(columns))
	for i, column := range columns {
		widths[i] = len(column)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	writeRow := func(values []string) {
		for i := range columns {
			if i > 0 {
				fmt.Fprint(stdout, "  ")
			}
			value := ""
			if i < len(values) {
				value = values[i]
			}
			fmt.Fprintf(stdout, "%-*s", widths[i], value)
		}
		fmt.Fprintln(stdout)
	}

	writeRow(columns)
	for i, width := range widths {
		if i > 0 {
			fmt.Fprint(stdout, "  ")
		}
		fmt.Fprint(stdout, strings.Repeat("-", width))
	}
	fmt.Fprintln(stdout)
	for _, row := range rows {
		writeRow(row)
	}
}

func isTerminalInput(input io.Reader) bool {
	file, ok := input.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

func sqlFormatCondition(condition sql.Condition) string {
	return fmt.Sprintf("%s %s %s", condition.Column, condition.Op, sql.FormatLiteral(condition.Literal))
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
