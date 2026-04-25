# Sceptre

Sceptre is an embedded, single-file relational database engine written in Go.

It is intentionally small, but it is not just a SQL parser wrapped around a map.
The project implements the stack from disk pages up to a small SQL layer:

- durable pager with dual meta pages
- B+ tree storage with ordered iteration
- durable key/value commits
- freelist-backed page reuse
- table schemas and typed row encoding
- primary-key CRUD and range scans
- secondary indexes
- transaction buffering with snapshot-style reads and conflict detection
- SQL parsing and execution for core statements
- CLI tools for inspection, query explanation, consistency checks, and crash testing

The goal is a compact database engine that is useful to run, easy to inspect,
and clear enough to study end to end.

## Current Capabilities

Sceptre currently supports:

- single-file database storage
- `CREATE TABLE`
- `CREATE INDEX`
- `INSERT`
- `SELECT`
- `UPDATE`
- `DELETE`
- interactive SQL shell
- primary-key lookups and scans
- secondary-index lookups
- atomic KV apply operations
- transaction commit/abort and optimistic conflict detection
- storage inspection commands
- `EXPLAIN` for query access paths
- `EXPLAIN ANALYZE`-style execution counters through `explain-analyze`
- `check` for table/index consistency validation
- `crash-test` for commit-boundary recovery verification

Sceptre is focused on the embedded single-file database path: storage,
indexing, transactions, SQL execution, inspection, and recovery verification.

## Quick Demo

Run SQL against a database file:

```powershell
go run ./cmd/sceptre sql app.db "create table users (id int64, name bytes, age int64, primary key (id))"
go run ./cmd/sceptre sql app.db "create index users_age on users (age)"
go run ./cmd/sceptre sql app.db "insert into users (id, name, age) values (1, 'Ada', 31)"
go run ./cmd/sceptre sql app.db "select id, name from users where age = 31"
go run ./cmd/sceptre shell app.db
```

Inspect and validate it:

```powershell
go run ./cmd/sceptre explain app.db "select * from users where age = 31"
go run ./cmd/sceptre explain-analyze app.db "select * from users where age = 31"
go run ./cmd/sceptre inspect meta app.db
go run ./cmd/sceptre inspect tree app.db
go run ./cmd/sceptre inspect freelist app.db
go run ./cmd/sceptre inspect table app.db users
go run ./cmd/sceptre inspect index app.db users_age
go run ./cmd/sceptre inspect pages app.db
go run ./cmd/sceptre inspect page app.db 0
go run ./cmd/sceptre check app.db
go run ./cmd/sceptre crash-test scratch.db
```

For a full walkthrough, see [docs/demo.md](docs/demo.md).

## CLI

```text
sceptre
sceptre help
sceptre sql <db-path> "<statement>"
sceptre shell <db-path>
sceptre explain <db-path> "<statement>"
sceptre explain-analyze <db-path> "<select>"
sceptre check <db-path>
sceptre crash-test <db-path>
sceptre inspect meta <db-path>
sceptre inspect tree <db-path>
sceptre inspect freelist <db-path>
sceptre inspect table <db-path> <table>
sceptre inspect index <db-path> <index>
sceptre inspect pages <db-path>
sceptre inspect page <db-path> <page-id>
```

`crash-test` creates a scratch directory beside the provided path and runs
commit interruption scenarios there. It does not overwrite the provided
database file.

## Architecture

Sceptre is organized as a vertical engine stack.

```text
cmd/sceptre        CLI
internal/sql       lexer, parser, planner, executor
internal/tx        transaction buffering and conflict detection
internal/table     schemas, rows, primary keys, secondary indexes, checks
internal/kv        durable key/value API and commit orchestration
internal/freelist  reusable page tracking
internal/btree     ordered page-based tree
internal/pager     file pages, meta pages, checksums, fsync
internal/debug     inspection and crash/recovery tooling
```

See [docs/architecture.md](docs/architecture.md) for the layer-by-layer design.

## Focused Scope

Sceptre is intentionally centered on the embedded database engine itself:

- local single-file storage
- one process owns the database file
- transparent page, tree, table, index, and freelist inspection
- deterministic recovery checks
- a small SQL layer over the storage engine

That focus keeps the project about database internals instead of turning it
into a network server or broad SQL compatibility project.

## What Makes It Different

Sceptre is built around transparent internals. The interesting part is not only
that data can be inserted and queried, but that the engine can explain what it
did and validate whether the file still makes sense.

Important debugging surfaces:

- `inspect meta` shows the active meta page state.
- `inspect tree` dumps the ordered KV entries reachable from the root.
- `inspect freelist` shows reusable pages.
- `inspect table` shows schema, indexes, and rows for one table.
- `inspect index` shows derived index entries and primary-key targets.
- `inspect pages` shows the page inventory and B+ tree page summaries.
- `inspect page` decodes one meta, freelist, or B+ tree page.
- `explain` reports the chosen query access path and residual filters.
- `explain-analyze` reports the chosen access path with row counters and stage timings.
- `check` validates table rows and secondary-index entries.
- `crash-test` interrupts commits at known stages and verifies recovery.

## Testing

CI runs on Linux with:

```powershell
go vet ./...
go test ./...
```

Run the storage benchmarks with:

```powershell
go test -bench=. ./internal/table
```

On some locked-down Windows machines, Application Control may block generated
Go test executables. In that environment, compiled test binaries still provide
a useful verification pass:

```powershell
go test -c ./...
go vet ./...
```

## Project Status

The core stack is implemented and demoable end to end. The repository is
organized around the same identity throughout the code, tests, CI, docs,
benchmarks, and CLI tooling: a transparent embedded database.
