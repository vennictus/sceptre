# Product Spec

Sceptre is a transparent embedded relational database engine. The product goal
is not broad SQL compatibility; it is a readable vertical implementation of
database internals with first-class inspection and recovery tooling.

## Goals

- Store tables in a single local database file.
- Keep the engine small enough to understand end to end.
- Make query planning, execution, storage pages, indexes, freelist state, and
  recovery behavior observable from the CLI.
- Favor explicit correctness checks over hidden magic.

## Background

Sceptre started from the bottom-up database-building path popularized by James
Smith's *Build Your Own Database From Scratch in Go: From B+Tree To SQL*:
B+ tree storage, durability, tables, indexes, transactions, and a SQL-like query
language. This codebase uses that learning path as foundation and emphasizes
inspectability, consistency checking, query tracing, and a polished CLI demo.

## Non-Goals

- Network server mode.
- Multi-process write sharing.
- Full SQL compatibility.
- Joins, aggregation, sorting, SQL transaction statements, or MVCC isolation
  levels.
- Competing with SQLite/Postgres on performance or feature breadth.

## Engine Stack

```text
CLI
SQL parser / planner / executor
tables / rows / secondary indexes
internal transaction primitives
durable key/value apply
copy-on-write B+ tree
freelist
pager / dual meta pages / fsync / file lock
single database file
```

## File Ownership

The pager creates a sidecar lock file beside the database. A second Sceptre
process opening the same database receives a lock error. This matches the
current design assumption: one process owns the database file.

If a process is killed abruptly, a stale `.lock` file may remain and must be
removed manually after confirming no process still owns the database.

## File Format

Sceptre stores fixed-size pages. The first two pages are reserved meta pages:

- page `0`: meta slot 0
- page `1`: meta slot 1
- page `2+`: B+ tree and freelist pages

The active meta page stores:

- page size
- root page
- freelist head page
- page count
- generation
- checksum

On open, both meta pages are validated and the valid page with the highest
generation is selected.

## Commit Protocol

A KV mutation batch commits by publishing a new durable root:

1. Apply mutations to the in-memory B+ tree.
2. Build a durable tree snapshot.
3. Allocate page IDs, reusing freelist pages when possible.
4. Write tree and freelist pages.
5. `fsync` page writes.
6. Publish the next meta page with a higher generation.
7. Install the committed in-process tree and freelist state.

The meta page is the visibility point. If the new meta page is not published,
reopen selects the previous durable state. If it is published, reopen selects
the new state.

## Storage Model

The table layer maps relational structures onto ordered key/value keys:

- catalog keys store table definitions
- row keys store primary-key ordered rows
- index keys store secondary-index values followed by primary-key values

Integers use order-preserving encoding. Byte strings use escaping so composite
keys remain sortable.

## Query Model

Supported SQL:

- `CREATE TABLE`
- `CREATE INDEX`
- `INSERT`
- `SELECT`
- `UPDATE`
- `DELETE`

Planner access paths:

- table scan
- primary-key lookup
- primary-key range
- exact secondary-index lookup

`explain` reports the planned access path. `explain-analyze` executes a select
and reports measured row counters and stage timings. `trace` prints compact row
flow through the execution stages.

## Transactions

`internal/tx` provides local transaction primitives:

- commit
- abort
- read-your-own-write
- snapshot-style reads
- serialized commit application
- optimistic conflict detection

These primitives are internal. Sceptre does not currently expose SQL
`BEGIN`/`COMMIT`/`ROLLBACK`.

## Consistency Checking

`sceptre check` validates:

- table schemas
- row encoding
- secondary-index entries
- B+ tree page shape
- page bounds
- freelist validity
- reachable/free-page overlap
- duplicate page references

## Recovery Testing

`sceptre crash-test` injects commit interruptions at deterministic stages:

- `pages-written`
- `pages-synced`
- `meta-published`

Each case reopens the database and runs the consistency checker. Random mode
samples the same interruption model with reproducible seeds:

```bash
sceptre crash-test scratch.db --random 20 --seed 42
```

This is not a process-kill harness. It is a deterministic commit-boundary
recovery test.

## Public CLI Surface

```text
sceptre demo [db-path] [--rows <n>] [--force]
sceptre sql <db-path> "<statement>"
sceptre shell <db-path>
sceptre explain <db-path> "<statement>"
sceptre explain-analyze <db-path> "<select>"
sceptre trace <db-path> "<select>"
sceptre check <db-path>
sceptre crash-test <db-path> [--random <n>] [--seed <n>]
sceptre inspect meta <db-path>
sceptre inspect tree <db-path>
sceptre inspect freelist <db-path>
sceptre inspect schema <db-path>
sceptre inspect table <db-path> <table>
sceptre inspect index <db-path> <index>
sceptre inspect pages <db-path>
sceptre inspect page <db-path> <page-id>
```
