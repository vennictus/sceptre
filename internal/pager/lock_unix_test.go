//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package pager

import (
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// A subprocess started by another goroutine briefly inherits the owner's
// descriptor until it execs. A duplicate descriptor reproduces that window:
// the lock stays held after Close until the duplicate is closed.
func TestReopenWaitsForInheritedDescriptorToClose(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db")
	p := mustOpenPager(t, path)
	// Close-on-exec keeps parallel tests' subprocesses from inheriting the
	// duplicate for their whole lifetime; ForkLock closes the race window.
	syscall.ForkLock.RLock()
	inherited, err := syscall.Dup(int(p.file.Fd()))
	if err == nil {
		syscall.CloseOnExec(inherited)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		syscall.Close(inherited)
		close(released)
	}()
	reopened, err := Open(path, Options{})
	<-released
	if err != nil {
		t.Fatalf("reopen after owner closed: %v", err)
	}
	reopened.Close()
}
