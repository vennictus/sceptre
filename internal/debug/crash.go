package debug

import (
	"fmt"
	"os"
	"path/filepath"
	"sceptre/internal/kv"
	"sceptre/internal/table"
	"strings"
)

type CrashCase struct {
	Stage       string
	Path        string
	Recovered   bool
	CheckOK     bool
	Issues      int
	ExpectedNew bool
	ObservedNew bool
}

type CrashReport struct {
	WorkDir string
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

	report := CrashReport{WorkDir: workDir}
	for _, stage := range kv.CommitStageNames() {
		casePath := filepath.Join(workDir, sanitizeStage(stage)+".db")
		crashCase, err := runCrashCase(casePath, stage)
		if err != nil {
			return report, err
		}
		report.Cases = append(report.Cases, crashCase)
	}
	return report, nil
}

func runCrashCase(path, stage string) (CrashCase, error) {
	if err := seedCrashDatabase(path); err != nil {
		return CrashCase{}, err
	}

	db, err := table.Open(path, table.Options{FailAfterCommitStage: stage})
	if err != nil {
		return CrashCase{}, err
	}
	insertErr := db.Insert("users", table.NewRecord(map[string]table.Value{
		"id":   table.Int64Value(2),
		"name": table.BytesValue([]byte("Grace")),
		"age":  table.Int64Value(40),
	}))
	closeErr := db.Close()
	if insertErr == nil {
		return CrashCase{}, fmt.Errorf("crash case %s: insert unexpectedly succeeded", stage)
	}
	if closeErr != nil {
		return CrashCase{}, closeErr
	}

	reopened, err := table.Open(path, table.Options{})
	if err != nil {
		return CrashCase{Stage: stage, Path: path}, nil
	}
	defer reopened.Close()

	check, err := reopened.Check()
	if err != nil {
		return CrashCase{}, err
	}
	_, oldOK, err := reopened.Get("users", table.NewRecord(map[string]table.Value{"id": table.Int64Value(1)}))
	if err != nil {
		return CrashCase{}, err
	}
	_, newOK, err := reopened.Get("users", table.NewRecord(map[string]table.Value{"id": table.Int64Value(2)}))
	if err != nil {
		return CrashCase{}, err
	}

	expectedNew := stage == "meta-published"
	return CrashCase{
		Stage:       stage,
		Path:        path,
		Recovered:   oldOK,
		CheckOK:     check.OK(),
		Issues:      len(check.Issues),
		ExpectedNew: expectedNew,
		ObservedNew: newOK,
	}, nil
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
