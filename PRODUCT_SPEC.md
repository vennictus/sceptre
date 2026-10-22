# Product Spec

Sceptre is an open-source relational database in Go. Its product goal is a
dependable general-purpose single-node engine with useful inspection and query
diagnostics. This specification describes the current bounded implementation;
the detailed [engine contract](docs/ENGINE_CONTRACT.md) defines its guarantees
and limitations.

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

## Currently Unsupported

- Network server mode (`sceptre serve` is loopback-only; see docs/SERVICE.md).
- Multi-process write sharing.
- Full SQL compatibility.
- Joins, aggregation, sorting, SQL transaction statements, or selectable
  isolation levels (the engine has snapshot reads; SQL does not expose them).
- Live backup and restore.
- Broad SQL compatibility and comparative performance superiority claims.

## Engine Stack

```text
CLI / local service and Studio / public Go API
SQL parser / planner / executor
tables / rows / secondary indexes
transaction-aware reads / scans / statement staging (Go API)
atomic key/value batches, snapshots, iterators
storage engine: copy-on-write B+ tree, overflow chains, page cache,
  transaction-tagged freelist, snapshot readers, commit protocol
pager / dual meta pages / fsync / file lock
single database file (format v2)
```

The public Go API owns a serialized execution lease and passes table/SQL execution
through `internal/tx`. CLI operations continue to use direct statement commits.
The [application API guide](docs/API.md) defines the public lifecycle and errors.

## File Ownership

The pager locks the database file itself: an exclusive nonblocking byte-range
lock on Windows, or an advisory flock on supported Unix platforms. A second
Sceptre owner, including a hard-link alias, receives a lock error after Open
retries for up to 500 ms; the retry covers a lock briefly inherited by a
subprocess that has not yet exec'd. Closing the handle or terminating its
process releases ownership automatically. Legacy sidecar `.lock` files are
ignored and are not removed by the engine.

This assumes a local filesystem with working OS locks. Unix advisory locking
does not prevent unrelated programs from ignoring the lock. Renaming or replacing
an open database file, network filesystems, and mixed old/new binaries operating
on the same file are outside the supported ownership contract.

## File Format (v2)

Sceptre stores fixed-size pages, a power of two from 1024 to 65536 bytes
(default 4096). The first two pages are meta slots; every other page is a tree,
overflow, freelist, or free page.

Each meta page stores the format version, page size, tree directory root,
freelist head, page count, generation (the transaction ID), and a checksum. Open
validates both slots and selects the valid one with the highest generation.

Every other page starts with a 24-byte header:

| Bytes | Field |
| --- | --- |
| 0–3 | CRC32C of the rest of the page |
| 4 | Page type: branch, leaf, overflow, or freelist |
| 5–7 | Reserved |
| 8–15 | The page's own ID |
| 16–23 | ID of the transaction that wrote it |

- **Tree directory:** a small B+ tree mapping tree names to root pages. Today it
  holds one tree, `data`, with the catalog, rows, and index entries; tables and
  indexes can get their own trees later without another format change.
- **Branch and leaf pages:** a slot directory followed by cells. A branch key is
  the largest key in its child's subtree. Each entry takes at most a quarter of
  a page, so any overfull node splits into two that fit.
- **Overflow chains:** values larger than about a quarter page are stored in
  linked overflow pages; the leaf cell keeps the length and first page.
- **Freelist pages:** a chain of `(page, freeing transaction)` records.

Open verifies the tree directory and freelist but does not walk the data tree.
Every page is verified when read from disk (checksum, page ID, type), and a
snapshot rejects pages written after it, so corruption and page-reuse bugs
surface as errors rather than wrong data.

Files written by the earlier in-memory engine use format v1. They are rejected
with `ErrUpgradeRequired` until converted with `sceptre upgrade`, which verifies
the converted copy before replacing the original and keeps the original as
`<db>.v1.bak`.

## Commit Protocol

Committed pages are never modified. A write transaction works like this:

1. Copy each page it changes to a new page ID and rewrite the path to the root
   (path copying). Pages it created itself are simply replaced.
2. Take new page IDs from free pages that are safe to reuse (below), then from
   the end of the file.
3. On commit, point the tree directory at the new root and build the next
   freelist: unused free pages, plus every page this commit replaced, tagged
   with its transaction ID.
4. Write the changed tree, overflow, directory, and freelist pages, each sealed
   with its checksum, page ID, and transaction ID.
5. `fsync` the page writes.
6. Publish the next meta page with a higher generation, then `fsync` it.
7. Install the new committed state for later snapshots.

The meta page is the visibility point. If the new meta page is not published,
reopen selects the previous durable state. If it is published, reopen selects
the new state. A commit's cost grows with the number of changed keys times the
tree height, plus the freelist, which is rewritten in full on each commit; it
does not grow with the dataset size.

A page freed by transaction T is reused only once T is at or before both the
previous generation and every open snapshot. Readers therefore never see a page
change underneath them, and the older meta slot always describes a complete
state, even after an interrupted commit has written reused pages. Repeated
updates reuse space after this retention window.

Data-write and data-sync failures before metadata publication discard the write
transaction. A short or failed metadata write, or a metadata-sync error, has an
uncertain outcome: the handle returns `ErrRecoveryRequired` and rejects new
reads, writes and snapshots until it is closed and reopened. Reopen selects a
complete valid generation. The after-publication interruption hook returns
`ErrCommitCommitted`; the complete batch is already visible and must not be
retried as an aborted write.

Reads go through a page cache bounded at 64 MiB by default; the database does
not need to fit in memory. A write transaction keeps its changed pages in memory
until commit.

## Storage Model

The table layer maps relational structures onto ordered key/value keys:

- catalog keys store table definitions
- row keys store primary-key ordered rows
- index keys store secondary-index values followed by primary-key values

Integers use order-preserving encoding. Byte strings use escaping so composite
keys remain sortable.

A stored key (an encoded primary key, or an index entry) may be at most
`(pageSize − 26) / 4 − 17` bytes: 1000 at the default 4096-byte page. A stored
row may be up to 1 GiB; rows larger than about a quarter page move to overflow
pages. Oversized keys fail the whole statement with the `limit` error category.

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
statement boundaries; SQL text does not support BEGIN/COMMIT/ROLLBACK. The public
Go API groups statements into transactions and coordinates concurrent callers.
Raw table/SQL access requires caller serialization.

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

The public Go API exposes Begin/Commit/Rollback and integrates schema, row, and
index operations with these primitives. One transaction owns execution at a time,
including reads, so public requests execute in serial order. Failed statements
preserve prior pending work; Commit ends the transaction on every outcome. Close
aborts open work. SQL transaction statements remain unsupported.

Typed positional `?` parameters bind int64 or arbitrary bytes as literal tokens.
The API caps SELECT results to MaxRows (default 1000), SQL text to 65536 bytes,
and binding count to 256. These are request/output bounds, not engine memory or
execution-work limits. Contexts stop ownership waits and are checked before work;
in-flight execution cannot yet be canceled.

Managed transactions record conflict history and close a writer even when a
post-publication hook reports an already committed outcome. An uncertain
publication blocks transaction operations until reopen. Serialized writes made
outside a manager conservatively conflict with its older snapshots when the
manager observes the new pager generation. History is pruned when no active
snapshot needs it. Snapshots are O(1) storage read transactions; a long-lived
snapshot retains conflict history and keeps the pages it reads from reuse.
Scans conservatively conflict with every later writing commit, including phantom
inserts; precise predicate conflict tracking is unsupported. Raw KV access and
multiple managers must be serialized by the caller. Public serialized transactions
do not use optimistic conflict detection.

## Consistency Checking

`sceptre check` first verifies storage, then the relational layer:

- every page read passes its checksum, page-ID, and type checks
- both trees in full: key order, separators equal to each child's maximum,
  uniform leaf depth, no page reached twice, well-formed overflow chains, and
  values stored inline exactly when they fit
- the freelist on disk matches the in-memory freelist
- page accounting: every page in the file is exactly one of tree, overflow,
  freelist, or free
- table schemas, row encoding, and table and index prefixes
- secondary-index entries against their rows, in both directions

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
sceptre serve <db-path> [--addr 127.0.0.1:7463] [--max-rows 1000] [--transaction-ttl 1m]
sceptre explain <db-path> "<statement>"
sceptre explain-analyze <db-path> "<select>"
sceptre trace <db-path> "<select>"
sceptre check <db-path>
sceptre upgrade <db-path>
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
