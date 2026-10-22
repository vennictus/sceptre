package pager

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestOwnershipIsExclusiveAcrossProcesses(t *testing.T) {
	if os.Getenv("SCEPTRE_LOCK_CHILD") == "1" {
		p, err := Open(os.Getenv("SCEPTRE_LOCK_DB"), Options{})
		if errors.Is(err, ErrDatabaseLocked) {
			os.Exit(0)
		}
		if p != nil {
			p.Close()
		}
		os.Exit(2)
	}
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db")
	p := mustOpenPager(t, path)
	defer p.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestOwnershipIsExclusiveAcrossProcesses$")
	cmd.Env = append(os.Environ(), "SCEPTRE_LOCK_CHILD=1", "SCEPTRE_LOCK_DB="+path)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("live owner was not excluded: %v %s", err, out)
	}
}

func TestOwnershipUsesFileIdentity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path, alias := filepath.Join(dir, "db"), filepath.Join(dir, "alias")
	p := mustOpenPager(t, path)
	defer p.Close()
	if err := os.Link(path, alias); err != nil {
		t.Skipf("hard links unavailable in test environment: %v", err)
	}
	other, err := Open(alias, Options{})
	if other != nil {
		other.Close()
	}
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("alias escaped ownership: %v", err)
	}
}

func TestLiveOwnerIsRejectedAfterBoundedWait(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db")
	p := mustOpenPager(t, path)
	defer p.Close()
	start := time.Now()
	other, err := Open(path, Options{})
	if other != nil {
		other.Close()
	}
	if !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("second owner was not rejected: %v", err)
	}
	if elapsed := time.Since(start); elapsed < ownershipWait || elapsed > ownershipWait+2*time.Second {
		t.Fatalf("rejection took %v, want about %v", elapsed, ownershipWait)
	}
}

func TestLegacySidecarDoesNotPreventOpen(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "db")
	if err := os.WriteFile(path+".lock", []byte("pid=old\n"), 0600); err != nil {
		t.Fatal(err)
	}
	p := mustOpenPager(t, path)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".lock"); err != nil {
		t.Fatalf("unowned legacy sidecar changed: %v", err)
	}
}
