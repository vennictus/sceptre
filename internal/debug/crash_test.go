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
