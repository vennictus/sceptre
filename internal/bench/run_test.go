package bench

import (
	"os"
	"testing"
)

func TestReportAccountsForDurableWorkAndCleansOnlyItsDirectories(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/keep", []byte("user file"), 0600); err != nil {
		t.Fatal(err)
	}
	report, err := Run(Options{Rows: []int{8}, Payloads: []int{32}, Operations: 5, Batch: 3, Seed: 42, Directory: dir, Storage: "test filesystem", Revision: "test"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Results) != len(workloads) || report.Environment.PageSize != 4096 {
		t.Fatalf("report: %+v", report)
	}
	for _, r := range report.Results {
		writes := uint64(0)
		rows := 8
		switch r.Workload {
		case "single_insert":
			writes = 5
			rows += 5
		case "batch_insert":
			writes = 15
			rows += 15
		case "indexed_update":
			writes = 5
		case "indexed_delete_insert":
			writes = 10
		case "mixed_80_20":
			writes = 1
		case "sql_limited_scan":
			if r.AccessPath != "table_scan" {
				t.Fatalf("scan path %s", r.AccessPath)
			}
		}
		if r.RowMutations != writes || r.FinalRows != rows {
			t.Fatalf("mutation accounting: %+v", r)
		}
		if writes == 0 {
			if r.WriteCalls != 0 || r.SyncCalls != 0 || r.PhysicalWriteBytes != 0 || r.FileBytesBefore != r.FileBytesAfter {
				t.Fatalf("read workload writes: %+v", r)
			}
		} else {
			commits := uint64(5)
			if r.Workload == "indexed_delete_insert" {
				commits = 10
			}
			if r.Workload == "mixed_80_20" {
				commits = 1
			}
			if r.SyncCalls != 2*commits || r.PhysicalWriteBytes == 0 || r.WriteAmplification <= 0 {
				t.Fatalf("durability accounting: %+v", r)
			}
		}
		if r.P50NS > r.P95NS || r.P95NS > r.P99NS || r.P50NS <= 0 || r.SampledPeakHeapBytes == 0 {
			t.Fatalf("invalid measurements: %+v", r)
		}
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 1 || files[0].Name() != "keep" {
		t.Fatalf("unexpected retained/deleted files: %v %v", files, err)
	}
}

func TestRejectsInvalidWorkBeforeCreatingDatabases(t *testing.T) {
	for _, opts := range []Options{{}, {Rows: []int{0}, Payloads: []int{1}, Operations: 1, Batch: 1}, {Rows: []int{1}, Payloads: []int{2049}, Operations: 1, Batch: 1}, {Rows: []int{1, 1}, Payloads: []int{1}, Operations: 1, Batch: 1}} {
		if _, err := Run(opts, nil); err == nil {
			t.Fatalf("accepted invalid options %+v", opts)
		}
	}
}

func TestQuantilesUseNearestRankWithoutMutatingSamples(t *testing.T) {
	samples := []int64{10, 1, 9, 2, 8, 3, 7, 4, 6, 5}
	if Quantile(samples, .5) != 5 || Quantile(samples, .95) != 10 || Quantile(samples, .99) != 10 || samples[0] != 10 {
		t.Fatal("incorrect quantile")
	}
	if Quantile(nil, .5) != 0 {
		t.Fatal("empty quantile")
	}
}
