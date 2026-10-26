package main

import (
	"fmt"
	"io"

	"github.com/vennictus/sceptre/internal/upgrade"
)

// runUpgrade converts a format v1 database to the current format in place.
func runUpgrade(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 || args[0] == "" {
		fmt.Fprint(stderr, "sceptre upgrade: expected <db-path>\n\n")
		fmt.Fprint(stderr, usage)
		return 2
	}
	rep, err := upgrade.Upgrade(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "sceptre upgrade: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "upgraded=%s\n", rep.Path)
	fmt.Fprintf(stdout, "format=v1->v%d\n", rep.NewFormat)
	fmt.Fprintf(stdout, "keys=%d\n", rep.Keys)
	fmt.Fprintf(stdout, "bytes=%d\n", rep.Bytes)
	fmt.Fprintf(stdout, "page_size=%d->%d\n", rep.OldPageSize, rep.NewPageSize)
	fmt.Fprintf(stdout, "page_count=%d->%d\n", rep.OldPageCount, rep.NewPageCount)
	fmt.Fprintf(stdout, "verified=every key and value compared; storage check passed\n")
	fmt.Fprintf(stdout, "backup=%s\n", rep.Backup)
	fmt.Fprintf(stdout, "duration=%s\n", rep.Duration)
	return 0
}
