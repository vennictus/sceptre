//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package pager

import (
	"fmt"
	"os"
)

func lockDatabase(*os.File) error {
	return fmt.Errorf("pager: database locking is unsupported on this platform")
}
