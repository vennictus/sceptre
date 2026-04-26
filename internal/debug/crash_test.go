package debug

import (
	"path/filepath"
	"testing"
)

func TestCrashTestRunsRecoveryCases(t *testing.T) {
	t.Parallel()

	report, err := CrashTest(filepath.Join(t.TempDir(), "sceptre.db"))
	if err != nil {
		t.Fatalf("CrashTest() error = %v", err)
	}
	if !report.OK() {
		t.Fatalf("CrashTest().OK() = false, report = %+v", report)
	}
	if len(report.Cases) != 9 {
		t.Fatalf("case count = %d, want 9", len(report.Cases))
	}
	for _, crashCase := range report.Cases {
		if crashCase.Path == "" {
			t.Fatalf("case %+v has empty path", crashCase)
		}
		if crashCase.Stage == "meta-published" && !crashCase.ObservedNew {
			t.Fatalf("meta-published observed_new = false, want true")
		}
		if crashCase.Stage != "meta-published" && crashCase.ObservedNew {
			t.Fatalf("%s observed_new = true, want false", crashCase.Stage)
		}
		if crashCase.Operation == "" {
			t.Fatalf("case %+v has empty operation", crashCase)
		}
	}
}

func TestRandomCrashTestRunsSeededCases(t *testing.T) {
	t.Parallel()

	report, err := RandomCrashTest(filepath.Join(t.TempDir(), "sceptre.db"), 5, 99)
	if err != nil {
		t.Fatalf("RandomCrashTest() error = %v", err)
	}
	if !report.OK() {
		t.Fatalf("RandomCrashTest().OK() = false, report = %+v", report)
	}
	if report.Mode != "random" {
		t.Fatalf("Mode = %q, want random", report.Mode)
	}
	if report.Seed != 99 {
		t.Fatalf("Seed = %d, want 99", report.Seed)
	}
	if len(report.Cases) != 5 {
		t.Fatalf("case count = %d, want 5", len(report.Cases))
	}
}
