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

The goal is not to replace SQLite or Postgres. The goal is to be a compact
database engine that is useful to run, easy to inspect, and clear enough to
study end to end.

## Current Capabilities

Sceptre currently supports:

- single-file database storage
- `CREATE TABLE`
- `CREATE INDEX`
- `INSERT`
- `SELECT`
- `UPDATE`
- `DELETE`
- primary-key lookups and scans
- secondary-index lookups
- atomic KV apply operations
- transaction commit/abort and optimistic conflict detection
- storage inspection commands
- `EXPLAIN` for query access paths
- `check` for table/index consistency validation
- `crash-test` for commit-boundary recovery verification

The SQL layer is deliberately small. V1 does not aim to support joins,
aggregates, subqueries, replication, networking, or full SQL compatibility.

## Quick Demo

Run SQL against a database file:

```powershell
go run ./cmd/sceptre sql app.db "create table users (id int64, name bytes, age int64, primary key (id))"
go run ./cmd/sceptre sql app.db "create index users_age on users (age)"
go run ./cmd/sceptre sql app.db "insert into users (id, name, age) values (1, 'Ada', 31)"
go run ./cmd/sceptre sql app.db "select id, name from users where age = 31"
```

Inspect and validate it:

```powershell
go run ./cmd/sceptre explain app.db "select * from users where age = 31"
go run ./cmd/sceptre inspect meta app.db
go run ./cmd/sceptre inspect tree app.db
go run ./cmd/sceptre inspect freelist app.db
go run ./cmd/sceptre check app.db
go run ./cmd/sceptre crash-test scratch.db
```

## CLI

```text
sceptre
sceptre help
sceptre sql <db-path> "<statement>"
sceptre explain <db-path> "<statement>"
sceptre check <db-path>
sceptre crash-test <db-path>
sceptre inspect meta <db-path>
sceptre inspect tree <db-path>
sceptre inspect freelist <db-path>
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

## What Makes It Different

Sceptre is built around transparent internals. The interesting part is not only
that data can be inserted and queried, but that the engine can explain what it
did and validate whether the file still makes sense.

Important debugging surfaces:

- `inspect meta` shows the active meta page state.
- `inspect tree` dumps the ordered KV entries reachable from the root.
- `inspect freelist` shows reusable pages.
- `explain` reports the chosen query access path and residual filters.
- `check` validates table rows and secondary-index entries.
- `crash-test` interrupts commits at known stages and verifies recovery.

## Testing

Use the normal Go test command on machines that allow generated test binaries:

```powershell
go test ./...
go vet ./...
```

On some locked-down Windows machines, Application Control may block generated
Go test executables. In that environment, compiled test binaries still provide
a useful verification pass:

```powershell
go test -c ./...
go vet ./...
```

## Project Status

The core V1 stack is implemented. The remaining polish work is mostly around
interactive ergonomics, broader validation, documentation depth, and benchmarks.

Near-term improvements:

- interactive SQL shell
- richer table/index inspection commands
- broader crash tests above the table and transaction layers
- benchmark commands for insert, lookup, scan, and index lookup paths
- deeper docs for file format, commit protocol, and isolation
