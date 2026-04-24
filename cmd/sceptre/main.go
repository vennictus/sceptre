package main

import (
	"fmt"
	"io"
	"os"
	"sceptre/internal/debug"
	"sceptre/internal/sql"
	"sceptre/internal/table"
	"strings"
)

const usage = `sceptre is an embedded relational database engine.

Usage:
  sceptre
  sceptre help
  sceptre sql <db-path> "<statement>"
  sceptre inspect meta <db-path>
  sceptre inspect tree <db-path>
  sceptre inspect freelist <db-path>
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

func runInspect(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 {
		fmt.Fprint(stderr, "sceptre inspect: expected <meta|tree|freelist> and <db-path>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}

	switch args[0] {
	case "meta":
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
		info, err := debug.InspectFreeList(args[1])
		if err != nil {
			fmt.Fprintf(stderr, "sceptre inspect freelist: %v\n", err)
			return 1
		}
		fmt.Fprintf(stdout, "head_page=%d\n", info.HeadPage)
		fmt.Fprintf(stdout, "freelist_pages=%v\n", info.PageIDs)
		fmt.Fprintf(stdout, "free_pages=%v\n", info.FreePages)
		return 0
	default:
		fmt.Fprintf(stderr, "sceptre inspect: unknown target %q\n\n", args[0])
		fmt.Fprint(stderr, usage)
		return 2
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

func formatBytes(value []byte) string {
	if len(value) == 0 {
		return `""`
	}
	return fmt.Sprintf("%q", string(value))
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
