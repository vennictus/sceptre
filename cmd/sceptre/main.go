package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sceptre/internal/debug"
	"sceptre/internal/sql"
	"sceptre/internal/table"
	"sort"
	"strings"
)

const usage = `sceptre is an embedded relational database engine.

Usage:
  sceptre
  sceptre help
  sceptre sql <db-path> "<statement>"
  sceptre shell <db-path>
  sceptre explain <db-path> "<statement>"
  sceptre check <db-path>
  sceptre crash-test <db-path>
  sceptre inspect meta <db-path>
  sceptre inspect tree <db-path>
  sceptre inspect freelist <db-path>
  sceptre inspect schema <db-path>
  sceptre inspect table <db-path> <table>
  sceptre inspect index <db-path> <index>
  sceptre inspect pages <db-path>
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
	case "sql":
		return runSQL(args[1:], stdout, stderr)
	case "shell":
		return runShell(args[1:], os.Stdin, stdout, stderr)
	case "explain":
		return runExplain(args[1:], stdout, stderr)
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
	for {
		fmt.Fprint(stdout, "sceptre> ")
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
		for _, def := range tables {
			for _, index := range def.Indexes {
				fmt.Fprintf(stdout, "%s\t%s\t%s\n", index.Name, def.Name, strings.Join(index.Columns, ","))
			}
		}
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
	if len(args) != 1 {
		fmt.Fprint(stderr, "sceptre crash-test: expected <db-path>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}

	report, err := debug.CrashTest(args[0])
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

func printSQLResult(stdout io.Writer, result sql.Result) {
	if len(result.Columns) == 0 {
		fmt.Fprintf(stdout, "OK rows_affected=%d\n", result.RowsAffected)
		return
	}

	fmt.Fprintln(stdout, strings.Join(result.Columns, "\t"))
	for _, row := range result.Rows {
		cells := make([]string, 0, len(row))
		for _, value := range row {
			cells = append(cells, formatValue(value))
		}
		fmt.Fprintln(stdout, strings.Join(cells, "\t"))
	}
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
	fmt.Fprintln(stdout, ".help")
	fmt.Fprintln(stdout, ".tables")
	fmt.Fprintln(stdout, ".schema")
	fmt.Fprintln(stdout, ".indexes")
	fmt.Fprintln(stdout, ".quit")
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

func sqlFormatCondition(condition sql.Condition) string {
	return fmt.Sprintf("%s %s %s", condition.Column, condition.Op, sql.FormatLiteral(condition.Literal))
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
