# Sceptre

Sceptre is an embedded, single-file relational database engine written in Go.

The project is being built from first principles to cover the full stack from storage engine to query layer:

- copy-on-write B+ tree storage
- durable key-value operations
- table and index abstractions on top of KV
- atomic transactions with snapshot-style reads
- a small SQL layer

Sceptre is deliberately not trying to become a full production SQL database. Its goal is to be small enough to understand, strong enough to be technically serious, and clear enough to inspect from the inside.

## What Makes Sceptre Different

Beyond implementing the usual educational database milestones, Sceptre is being designed around transparency and verification.

That means the finished project is expected to include:

- built-in storage inspection commands
- deterministic crash-injection and recovery testing
- query access-path explanation
- transaction and commit tracing

The aim is not just to store data, but to make the engine explain itself.

## Scope

Sceptre is planned to include:

- B+ tree storage
- durable KV operations
- free-list based page reuse
- tables on top of KV
- ordered scans and iterators
- secondary indexes
- transactions
- concurrency basics
- a small SQL parser and execution layer

The first complete version is not intended to include:

- joins
- aggregates
- subqueries
- replication
- network server mode
- multi-process writers
- advanced optimizer work
- full SQL compatibility

## Architecture Overview

Sceptre is organized as a vertical engine stack.

### Pager

Responsible for:

- fixed-size pages
- dual meta pages
- page allocation
- `fsync` ordering
- checksum validation

### B+ Tree

Responsible for:

- ordered storage
- point lookups
- range scans
- insert, update, delete
- split, merge, and rebalance

### KV Layer

Responsible for:

- `Get`, `Set`, `Del`
- copy-on-write commit paths
- durable reopen behavior

### Relational Layer

Responsible for:

- table definitions
- row encoding
- primary keys
- secondary indexes
- internal catalog storage

### Transaction Layer

Responsible for:

- snapshot reads
- buffered writes
- atomic commit and rollback
- conflict detection for concurrent writers

### SQL and Debug Layer

Responsible for:

- parsing and execution
- `EXPLAIN`
- inspection commands
- recovery and tracing utilities

## Repository Layout

The intended repository structure is:

```text
sceptre/
  cmd/
    sceptre/
      main.go
  internal/
    pager/
    btree/
    kv/
    freelist/
    catalog/
    table/
    tx/
    sql/
    debug/
    testutil/
  docs/
    file-format.md
    commit-protocol.md
    isolation.md
  raw.md
  plan.md
  README.md
```

### Package Responsibilities

`cmd/sceptre`

- CLI entrypoint

`internal/pager`

- file format, meta pages, reads, writes, checksums

`internal/btree`

- node encoding, search, splits, merges, iterators

`internal/kv`

- durable key-value API and commit orchestration

`internal/freelist`

- page reuse and reclamation rules

`internal/catalog`

- metadata, internal tables, and index definitions

`internal/table`

- row encoding, table CRUD helpers, index entry generation

`internal/tx`

- snapshots, pending writes, commit logic, conflict detection

`internal/sql`

- lexer, parser, AST, planner, execution, `EXPLAIN`

`internal/debug`

- page inspection, tree dumps, traces, recovery diagnostics

`internal/testutil`

- reference models, crash harnesses, randomized test helpers

## Testing Strategy

Sceptre is only interesting if it is heavily tested. The test suite is part of the product, not a later polish pass.

The project is expected to include:

- unit tests for encoders, page layout, and tree operations
- randomized invariant tests for insert, delete, split, and merge behavior
- reopen tests for durability
- crash-injection and recovery tests around commit boundaries
- transaction visibility and rollback tests
- concurrency and conflict-detection tests
- parser tests for valid and invalid SQL input
- differential tests against simple reference models where practical
- fuzz tests for storage and parser edge cases
- benchmarks for lookup, insert, delete, and scan paths

## Development Status

The repository is currently in the planning and setup stage.

The implementation order is:

1. repo scaffold and Go module
2. in-memory B+ tree
3. durable pager and meta pages
4. durable KV layer
5. free list and page reuse
6. catalog and tables
7. range scans and iterators
8. secondary indexes
9. transactions
10. concurrency basics
11. SQL layer
12. inspection tooling and crash harness

## Build Roadmap

The project is intentionally being broken into commit-sized blocks so the history reflects the architecture growing layer by layer.

The planned build sequence is:

1. repo scaffold
2. B+ tree node format
3. B+ tree search, insert, split, and root growth
4. B+ tree delete, merge, and iteration
5. durable pager and dual meta pages
6. durable KV core
7. crash-safety harness
8. free list and page reuse
9. catalog and table definitions
10. table CRUD on primary keys
11. range encoding and scanners
12. secondary indexes
13. transaction core
14. concurrency basics
15. SQL parser
16. SQL execution and `EXPLAIN`
17. inspection and debug tooling
18. hardening, fuzzing, differential testing, and benchmarks

The intended outcome is a commit history that reads cleanly from storage primitives up to the query layer, with tests landing alongside each major capability.

## Getting Started

The codebase is not fully scaffolded yet, so the immediate starting point is repository setup and package layout.

The first implementation pass should create:

```text
cmd/sceptre/main.go
internal/btree/node.go
internal/btree/insert.go
internal/btree/delete.go
internal/btree/iter.go
internal/btree/btree_test.go
internal/pager/pager.go
internal/pager/meta.go
internal/kv/kv.go
internal/kv/kv_test.go
```

Once the scaffold exists, the primary development loop should be:

```text
go test ./...
go run ./cmd/sceptre
```

## Planned CLI Surface

The public interface is expected to center on a single `sceptre` binary with commands such as:

```text
sceptre db init <path>
sceptre kv get <path> <key>
sceptre kv set <path> <key> <value>
sceptre sql <path>
sceptre inspect meta <path>
sceptre inspect page <path> <page-id>
sceptre inspect tree <path>
sceptre inspect freelist <path>
sceptre explain <path> "<query>"
```

## Design Principles

The project is guided by a small set of rules:

- correctness before breadth
- inspectability over opacity
- explicit invariants over vague behavior
- deterministic tests over manual confidence
- narrow scope over feature sprawl

## End State

If Sceptre is completed as planned, it should be fair to describe it as:

"An embedded relational database engine in Go with a copy-on-write B+ tree storage engine, crash-safe commits, snapshot reads, secondary indexes, a small SQL layer, and built-in tooling to inspect on-disk state and verify recovery behavior."
