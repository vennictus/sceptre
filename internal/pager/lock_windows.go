package pager

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var lockFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("LockFileEx")

func lockDatabase(file *os.File) error {
	var overlapped syscall.Overlapped
	// Exclusive, nonblocking byte-range ownership on the database inode itself.
	// Windows releases it when the handle closes or its process terminates.
	ok, _, err := lockFileEx.Call(file.Fd(), 3, 0, 0xffffffff, 0xffffffff, uintptr(unsafe.Pointer(&overlapped)))
	if ok != 0 {
		return nil
	}
	if err == syscall.Errno(33) {
		return fmt.Errorf("%w: %s", ErrDatabaseLocked, file.Name())
	}
	return fmt.Errorf("pager: lock database: %w", err)
}
