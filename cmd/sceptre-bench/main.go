package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/vennictus/sceptre/internal/bench"
)

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("sceptre-bench", flag.ContinueOnError)
	flags.SetOutput(stderr)
	rows := flags.String("rows", "100,1000,10000", "comma-separated initial row counts")
	payloads := flags.String("payloads", "128", "comma-separated payload byte lengths (0..2048)")
	operations := flags.Int("operations", 100, "measured operations per workload")
	batch := flags.Int("batch", 32, "rows per insert batch and maximum range/result rows")
	seed := flags.Int64("seed", 42, "deterministic operation seed")
	dir := flags.String("dir", "", "parent directory for disposable databases (default OS temp directory)")
	storage := flags.String("storage", "unspecified", "filesystem/device description for this run")
	revision := flags.String("revision", "unspecified", "tested Git revision or working-tree description")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintln(stderr, "unexpected positional arguments")
		return 2
	}
	rowCounts, err := integers(*rows)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	payloadSizes, err := integers(*payloads)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	report, err := bench.Run(bench.Options{Rows: rowCounts, Payloads: payloadSizes, Operations: *operations, Batch: *batch, Seed: *seed, Directory: *dir, Storage: *storage, Revision: *revision}, func(r bench.Result) {
		fmt.Fprintf(stderr, "%s rows=%d payload=%d complete\n", r.Workload, r.Rows, r.Payload)
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func integers(input string) ([]int, error) {
	var out []int
	seen := make(map[int]bool)
	for _, part := range strings.Split(input, ",") {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || seen[value] {
			return nil, fmt.Errorf("invalid or repeated integer %q", part)
		}
		seen[value] = true
		out = append(out, value)
	}
	return out, nil
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
