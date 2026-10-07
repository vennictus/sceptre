package table

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

type workloadOperation struct {
	Kind string `json:"kind"`
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

type workloadHistory struct {
	Version    int                 `json:"version"`
	Seed       int64               `json:"seed"`
	PageSize   uint32              `json:"page_size"`
	Operations []workloadOperation `json:"operations"`
}

func generatedHistory(seed int64) workloadHistory {
	rng := rand.New(rand.NewSource(seed))
	h := workloadHistory{Version: 1, Seed: seed, PageSize: 512}
	for step := 0; step < 300; step++ {
		id := int64(rng.Intn(50))
		body := fmt.Sprintf("v%03d-%s", step, strings.Repeat("q", rng.Intn(140)))
		h.Operations = append(h.Operations, workloadOperation{Kind: []string{"upsert", "delete", "insert"}[rng.Intn(3)], ID: id, Body: body})
	}
	return h
}

func readHistory(path string) (workloadHistory, error) {
	f, err := os.Open(path)
	if err != nil {
		return workloadHistory{}, err
	}
	defer f.Close()
	var h workloadHistory
	decoder := json.NewDecoder(io.LimitReader(f, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&h); err != nil {
		return h, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return h, fmt.Errorf("history contains trailing or oversized data")
	}
	if h.Version != 1 || h.PageSize != 512 || len(h.Operations) == 0 || len(h.Operations) > 10000 {
		return h, fmt.Errorf("invalid workload history header")
	}
	for _, op := range h.Operations {
		if op.ID < 0 || op.ID >= 50 || len(op.Body) > 150 || (op.Kind != "upsert" && op.Kind != "delete" && op.Kind != "insert") {
			return h, fmt.Errorf("invalid workload operation")
		}
	}
	return h, nil
}

func persistFailure(t *testing.T, h *workloadHistory) {
	t.Helper()
	if !t.Failed() {
		return
	}
	data, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		t.Logf("history encoding failed: %v", err)
		return
	}
	dir := os.Getenv("SCEPTRE_TEST_ARTIFACT_DIR")
	if dir == "" {
		t.Logf("replay history: %s", data)
		return
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Logf("artifact directory: %v", err)
		return
	}
	path := filepath.Join(dir, fmt.Sprintf("indexed-workload-%d.json", h.Seed))
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Logf("persist history: %v", err)
	} else {
		t.Logf("replay history saved to %s", path)
	}
}

func TestHistoryRoundTripPreservesConcreteOperations(t *testing.T) {
	h := generatedHistory(42)
	data, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "history.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := readHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.Operations) != 300 || loaded.Operations[299] != h.Operations[299] || loaded.Seed != 42 {
		t.Fatal("history lost operation inputs")
	}
	if err := os.WriteFile(path, append(data, []byte(` {}`)...), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readHistory(path); err == nil {
		t.Fatal("trailing history accepted")
	}
}

func TestFailureArtifactCanBeReplayed(t *testing.T) {
	if os.Getenv("SCEPTRE_HISTORY_CHILD") == "1" {
		h := generatedHistory(42)
		h.Operations = h.Operations[:5]
		t.Cleanup(func() { persistFailure(t, &h) })
		t.Fatal("deliberate failure to verify artifact capture")
	}
	t.Parallel()
	dir := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestFailureArtifactCanBeReplayed$")
	child.Env = append(os.Environ(), "SCEPTRE_HISTORY_CHILD=1", "SCEPTRE_TEST_ARTIFACT_DIR="+dir)
	if out, err := child.CombinedOutput(); err == nil {
		t.Fatalf("expected deliberate child failure: %s", out)
	}
	path := filepath.Join(dir, "indexed-workload-42.json")
	h, err := readHistory(path)
	if err != nil || len(h.Operations) != 5 {
		t.Fatalf("missing failure history: %+v %v", h, err)
	}
	replay := exec.Command(os.Args[0], "-test.run=^TestIndexedWorkloadAcrossPageReuseAndReopen$")
	replay.Env = append(os.Environ(), "SCEPTRE_WORKLOAD_REPLAY="+path, "SCEPTRE_HISTORY_CHILD=0")
	if out, err := replay.CombinedOutput(); err != nil {
		t.Fatalf("artifact replay failed: %v %s", err, out)
	}
}
