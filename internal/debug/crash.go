package debug

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vennictus/sceptre/internal/kv"
	"github.com/vennictus/sceptre/internal/table"
)

type CrashCase struct {
	Stage       string
	Operation   string
	Path        string
	Recovered   bool
	CheckOK     bool
	Issues      int
	ExpectedNew bool
	ObservedNew bool
}

type CrashReport struct {
	WorkDir string
	Mode    string
	Seed    int64
	Cases   []CrashCase
}

func (r CrashReport) OK() bool {
	if len(r.Cases) == 0 {
		return false
	}
	for _, c := range r.Cases {
		if !c.Recovered || !c.CheckOK || c.ExpectedNew != c.ObservedNew {
			return false
		}
	}
	return true
}

func CrashTest(path string) (CrashReport, error) {
	parent := filepath.Dir(path)
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	if base == "" || base == "." {
		base = "sceptre"
	}
	workDir, err := os.MkdirTemp(parent, base+".crash-*")
	if err != nil {
		return CrashReport{}, err
	}

	report := CrashReport{WorkDir: workDir, Mode: "matrix"}
	for _, stage := range kv.CommitStageNames() {
		for _, operation := range []string{"insert", "update", "delete"} {
			casePath := filepath.Join(workDir, sanitizeStage(stage)+"_"+operation+".db")
			crashCase, err := runCrashCase(casePath, stage, operation)
			if err != nil {
				return report, err
			}
			report.Cases = append(report.Cases, crashCase)
		}
	}
	return report, nil
}

func RandomCrashTest(path string, cases int, seed int64) (CrashReport, error) {
	if cases <= 0 {
		return CrashReport{}, fmt.Errorf("random crash cases must be positive")
	}
	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	parent := filepath.Dir(path)
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	if base == "" || base == "." {
		base = "sceptre"
	}
	workDir, err := os.MkdirTemp(parent, base+".crash-random-*")
	if err != nil {
		return CrashReport{}, err
	}

	stages := kv.CommitStageNames()
	operations := []string{"insert", "update", "delete"}
	rng := rand.New(rand.NewSource(seed))
	report := CrashReport{WorkDir: workDir, Mode: "random", Seed: seed}
	for i := 0; i < cases; i++ {
		stage := stages[rng.Intn(len(stages))]
		operation := operations[rng.Intn(len(operations))]
		casePath := filepath.Join(workDir, fmt.Sprintf("case_%03d_%s_%s.db", i+1, sanitizeStage(stage), operation))
		crashCase, err := runCrashCase(casePath, stage, operation)
		if err != nil {
			return report, err
		}
		report.Cases = append(report.Cases, crashCase)
	}
	return report, nil
}

func runCrashCase(path, stage, operation string) (CrashCase, error) {
	if err := seedCrashDatabase(path); err != nil {
		return CrashCase{}, err
	}

	db, err := table.Open(path, table.Options{FailAfterCommitStage: stage})
	if err != nil {
		return CrashCase{}, err
	}
	opErr := runCrashOperation(db, operation)
	closeErr := db.Close()
	if opErr == nil {
		return CrashCase{}, fmt.Errorf("crash case %s/%s: operation unexpectedly succeeded", stage, operation)
	}
	if closeErr != nil {
		return CrashCase{}, closeErr
	}

	reopened, err := table.Open(path, table.Options{})
	if err != nil {
		return CrashCase{Stage: stage, Operation: operation, Path: path}, nil
	}
	defer reopened.Close()

	check, err := reopened.Check()
	if err != nil {
		return CrashCase{}, err
	}
	observedNew, recovered, err := observeCrashState(reopened, operation)
	if err != nil {
		return CrashCase{}, err
	}

	expectedNew := stage == "meta-published"
	return CrashCase{
		Stage:       stage,
		Operation:   operation,
		Path:        path,
		Recovered:   recovered,
		CheckOK:     check.OK(),
		Issues:      len(check.Issues),
		ExpectedNew: expectedNew,
		ObservedNew: observedNew,
	}, nil
}

func runCrashOperation(db *table.DB, operation string) error {
	switch operation {
	case "insert":
		return db.Insert("users", table.NewRecord(map[string]table.Value{
			"id":   table.Int64Value(2),
			"name": table.BytesValue([]byte("Grace")),
			"age":  table.Int64Value(40),
		}))
	case "update":
		return db.Update("users", table.NewRecord(map[string]table.Value{
			"id":   table.Int64Value(1),
			"name": table.BytesValue([]byte("Ada")),
			"age":  table.Int64Value(40),
		}))
	case "delete":
		_, err := db.Delete("users", table.NewRecord(map[string]table.Value{"id": table.Int64Value(1)}))
		return err
	default:
		return fmt.Errorf("unknown crash operation %q", operation)
	}
}

func observeCrashState(db *table.DB, operation string) (bool, bool, error) {
	switch operation {
	case "insert":
		_, oldOK, err := db.Get("users", table.NewRecord(map[string]table.Value{"id": table.Int64Value(1)}))
		if err != nil {
			return false, false, err
		}
		_, newOK, err := db.Get("users", table.NewRecord(map[string]table.Value{"id": table.Int64Value(2)}))
		return newOK, oldOK, err
	case "update":
		row, ok, err := db.Get("users", table.NewRecord(map[string]table.Value{"id": table.Int64Value(1)}))
		if err != nil || !ok {
			return false, ok, err
		}
		return row.Values["age"].I64 == 40, true, nil
	case "delete":
		_, ok, err := db.Get("users", table.NewRecord(map[string]table.Value{"id": table.Int64Value(1)}))
		if err != nil {
			return false, false, err
		}
		return !ok, true, nil
	default:
		return false, false, fmt.Errorf("unknown crash operation %q", operation)
	}
}

func seedCrashDatabase(path string) error {
	db, err := table.Open(path, table.Options{})
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.CreateTable(table.TableDef{
		Name: "users",
		Columns: []table.Column{
			{Name: "id", Type: table.TypeInt64},
			{Name: "name", Type: table.TypeBytes},
			{Name: "age", Type: table.TypeInt64},
		},
		PrimaryKey: []string{"id"},
	}); err != nil {
		return err
	}
	if err := db.CreateIndex("users", table.IndexDef{Name: "users_age", Columns: []string{"age"}}); err != nil {
		return err
	}
	return db.Insert("users", table.NewRecord(map[string]table.Value{
		"id":   table.Int64Value(1),
		"name": table.BytesValue([]byte("Ada")),
		"age":  table.Int64Value(31),
	}))
}

func sanitizeStage(stage string) string {
	return strings.ReplaceAll(stage, "-", "_")
}
