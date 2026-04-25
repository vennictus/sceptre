# Architecture

Sceptre is a vertical embedded database stack. Each layer is intentionally small
and has one job.

```text
CLI
SQL parser / planner / executor
tables / rows / secondary indexes
transactions
durable key/value API
B+ tree
freelist
pager / meta pages / fsync
single database file
```

## Pager

The pager owns the database file. It manages fixed-size pages, two meta pages,
checksums, page reads, page writes, and syncs.

The active meta page stores the current root page, freelist page, page count,
page size, and generation.

## B+ Tree

The B+ tree stores ordered key/value pairs. It supports point lookup, insertion,
deletion, ordered iteration, page splits, page merges, and snapshot export for
durable publication.

## KV

The KV layer applies batches of mutations atomically. It writes new tree and
freelist pages, syncs them, and then publishes a new meta page.

The meta publish is the recovery visibility point.

## Table

The table layer maps relational structures onto ordered KV keys.

- catalog keys store table definitions
- row keys store primary-key ordered rows
- index keys store secondary-index values plus primary-key targets

The table checker validates schemas, rows, indexes, prefixes, B+ tree page
shape, and freelist overlap.

## Transactions

Transactions buffer writes and commit them through one KV apply operation. The
current model supports commit, abort, read-your-own-write, snapshot-style reads,
serialized commits, and optimistic conflict detection.

## SQL

The SQL layer includes a lexer, parser, AST, planner, and executor. The planner
selects between table scans, primary-key lookups, primary-key ranges, and exact
secondary-index lookups.

## Debug Tooling

Debug commands are part of the product:

- `explain` shows query access paths.
- `inspect` exposes meta, pages, tree entries, tables, indexes, and freelist.
- `check` validates consistency.
- `crash-test` verifies recovery at deterministic commit failpoints.
