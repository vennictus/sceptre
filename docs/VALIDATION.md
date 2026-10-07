# Reproducing validation

Run from the repository root with Go 1.22 or newer:

```bash
go vet ./...
go test -count=1 ./...
go test -race -count=1 ./...
go test -count=1 -covermode=atomic -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
go test ./internal/btree -run='^$' -fuzz=FuzzValidateTreeNeverPanics -fuzztime=10s -parallel=2
go test ./internal/sql -run='^$' -fuzz=FuzzParse -fuzztime=10s -parallel=2
go test ./internal/sql -run='^$' -fuzz=FuzzParameterBytes -fuzztime=10s -parallel=2
```

Race tests require a supported Go race platform and C compiler. On Windows,
Application Control may block generated executables; the existing Ubuntu WSL
toolchain can run the same tests. This exercises Linux locks and I/O, so native
Windows verification remains a separate result.

CI configures regular uncached tests, vet, coverage, and JSON test results on
Linux, Windows, and macOS with the go.mod toolchain. A Linux job uses current
stable Go for race checks and bounded fuzz runs. Uploaded artifacts contain only
test outputs, coverage, concrete failing workloads, and generated fuzz failure
inputs. These jobs run on pull requests, development-branch pushes, and manual
dispatch. A configured job is not a verified remote result.

## Targeted recovery regressions

```bash
go test -count=1 ./internal/kv -run='Test(OpenRejects|Fallback|Iterator|PhysicalIO|AbruptProcess)'
go test -count=1 ./internal/tx -run='Test(PublishedError|UncertainPublication|ExternalCommit|HistoryRetained)'
go test -count=1 ./internal/table -run='TestCatalogAndIndexBackfillRemainAtomicOnPhysicalIOFailures'
go test -count=1 ./internal/sql -run='TestSQL(UpdateFailure|MultiRowMutation)'
go test -count=1 . -run='TestPublicCommitOutcomes'
```

The physical I/O seam writes a real partial prefix or returns a sync error at
the specified call. Tests verify selected generations, row/index/catalog
invariants, close/reopen, and continued use. The subprocess tests deliberately
exit without Close. Neither model simulates every power-loss or device behavior.

## Application transaction validation

```bash
go test -count=1 .
go test -race -count=1 . ./internal/tx
go test -count=1 . -run=TestTransactionalIndexedWorkloadAgainstModel
go test -count=1 ./internal/tx -run='Test(ApplyFailure|ManagedScan)'
```

The public tests cover catalog/row/index commit and rollback, pending schema and
index visibility, failed-statement preservation, typed byte and int64 parameters,
result/request bounds, empty and closed transactions, canceled ownership waits,
canceled commits, close with open work, and concurrent read-modify-write callers.
The external-package example compiles and runs without importing internal packages.

The indexed transaction model uses seeds 7, 42, and 2026 at 512-byte pages. Each
seed executes 24 groups of five mutations, checking pending scans and index results
against a map before commit or rollback. Committed contents and integrity are
checked after every group, with reopen checkpoints. Failures identify seed/group;
the existing private workload replay format covers the lower-level table workload.
Parameter fuzzing checks exact preservation of arbitrary bytes without changing
SQL grammar. Tests establish serialized execution; they do not establish concurrent
snapshot isolation or in-flight query cancellation.

## Randomized workload histories

The indexed workload uses seeds 1, 7, 42, and 2026 at 512-byte pages. Each history
contains 300 concrete inserts, upserts, and deletes; checkpoints compare scans,
point reads, and secondary-index reads with an independent map and run integrity
checks. Reopen checkpoints repeat the checks against persisted data.

On failure, the executed prefix includes the triggering operation. It is printed
to the test log unless an artifact directory is configured. For local capture:

```bash
mkdir -p test-artifacts
SCEPTRE_TEST_ARTIFACT_DIR="$PWD/test-artifacts" go test -count=1 ./internal/table -run=TestIndexedWorkloadAcrossPageReuseAndReopen
```

For a downloaded or locally recorded history:

```bash
SCEPTRE_WORKLOAD_REPLAY="$PWD/test-artifacts/indexed-workload-42.json" go test -count=1 ./internal/table -run=TestIndexedWorkloadAcrossPageReuseAndReopen
```

PowerShell uses `$env:SCEPTRE_TEST_ARTIFACT_DIR` and
`$env:SCEPTRE_WORKLOAD_REPLAY` instead of the shell prefixes. Clear these variables
after the run. Replays validate the history version, page size, operation kinds,
IDs, body sizes, and input length. A regression test captures an intentional
subprocess failure and replays its saved history successfully.

Retain the failing revision and Go version alongside an artifact. A single seed
can also be selected with `-run='TestIndexedWorkloadAcrossPageReuseAndReopen/42$'`.
These small workloads cover invariants and page reuse; they are not sustained-load
or production-scale evidence.
