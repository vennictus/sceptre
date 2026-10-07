//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package pager

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

func lockDatabase(file *os.File) error {
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return fmt.Errorf("%w: %s", ErrDatabaseLocked, file.Name())
	}
	return err
}
