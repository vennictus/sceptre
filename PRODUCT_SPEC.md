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

The pager locks the database file itself: an exclusive nonblocking byte-range
lock on Windows, or an advisory flock on supported Unix platforms. A second
Sceptre owner, including a hard-link alias, receives a lock error. Closing the
handle or terminating its process releases ownership automatically. Legacy
sidecar `.lock` files are ignored and are not removed by the engine.

This assumes a local filesystem with working OS locks. Unix advisory locking
does not prevent unrelated programs from ignoring the lock. Renaming or replacing
an open database file, network filesystems, and mixed old/new binaries operating
on the same file are outside the supported ownership contract.

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

Before serving queries, the selected tree is checked for valid cells and slots,
ordered disjoint key ranges, exact child-maximum separators, consistent leaf
depth, and cycles or repeated children. Its freelist is checked for reserved or
out-of-bounds IDs, duplicates, and overlap with live tree or freelist pages.
Invalid selected state is rejected rather than exposed to queries.

## Commit Protocol

A KV mutation batch commits by publishing a new durable root:

1. Apply mutations to a private in-memory B+ tree, preserving existing iterators.
2. Build a durable tree snapshot.
3. Allocate page IDs, withholding pages needed by either valid metadata slot.
4. Write tree and freelist pages.
5. `fsync` page writes.
6. Publish the next meta page with a higher generation.
7. Install the committed in-process tree and freelist state.

The meta page is the visibility point. If the new meta page is not published,
reopen selects the previous durable state. If it is published, reopen selects
the new state.

Tree and freelist pages of the other valid metadata generation remain protected
until the next publication replaces its slot. Retired pages may appear in the
freelist but are withheld from allocation while that generation needs them.
Repeated updates still reuse space after this retention window. Deletion can
split internal pages when variable-length replacement separators grow.

Data-write and data-sync failures before metadata publication roll back the
in-process state. A short or failed metadata write, or a metadata-sync error,
has an uncertain outcome: the KV handle returns `ErrRecoveryRequired` and rejects
new reads/writes until it is closed and reopened. Reopen selects a complete valid
generation. The after-publication interruption hook returns `ErrCommitCommitted`;
the complete batch is already visible and must not be retried as an aborted write.

The file layout remains version 1. These fixes do not repair corruption or undo
incorrect writes produced by older binaries. The retention guarantee applies to
commits made by this implementation; existing files must pass open-time checks.
Data pages do not yet carry checksums, so structurally valid arbitrary byte
corruption is not guaranteed to be detected.

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

Before selecting an access path, predicates are validated for known columns,
compatible value types, and boolean WHERE/logical expressions. UPDATE targets
and assignment types are validated before scanning. EXPLAIN and ANALYZE use the
same semantic checks as execution, so an empty table or a no-match access path
does not hide an invalid statement.

Primary-key columns cannot be assigned by SQL UPDATE, including assignments to
their existing value; atomic key relocation is not implemented. Duplicate INSERT
columns and UPDATE targets are rejected. Explicit LIMIT/OFFSET values must be
non-negative. LIMIT 0 is valid. Pagination bounds returned rows but does not yet
bound candidate materialization or execution memory.

SQL UPDATE and DELETE stage all matching row/index changes and apply them through
one KV batch. Table UpdateMany rejects repeated primary keys and stages all rows
before applying its batch; missing rows or invalid records do not publish a
commit. Zero-match mutations do not advance the metadata generation. These are
statement boundaries, not SQL BEGIN/COMMIT/ROLLBACK support. Callers must serialize
access to the table/SQL engine; concurrent application requests are not yet safe.

An error is not proof of rollback. The commit outcomes above also apply to SQL
statements. Tests exercise short writes, data and metadata sync failures, and
deterministic interruption stages; these do not model every storage device or
power-loss behavior.

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

Managed transactions record conflict history and close a writer even when a
post-publication hook reports an already committed outcome. An uncertain
publication blocks transaction operations until reopen. Serialized writes made
outside a manager conservatively conflict with its older snapshots when the
manager observes the new pager generation. History is pruned when no active
snapshot needs it; long-lived snapshots can still retain history and tree copies.
Raw KV access and multiple managers must be serialized by the caller.

## Consistency Checking

`sceptre check` validates:

- table schemas
- row encoding
- secondary-index entries
- B+ tree page shape
- routing separators, key ranges, cycles, and consistent leaf depth
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
