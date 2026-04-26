# Sceptre Guide

This guide walks through Sceptre from the CLI down to the storage inspection
tools. It assumes Go 1.22+ and a terminal in the repository root.

## 1. Run The Guided Demo

```bash
go run ./cmd/sceptre demo demo.db --rows 100000
```

The demo creates a `users` table, loads generated rows, compares a table scan
with a secondary-index lookup, deletes about 30% of the rows, inspects storage,
runs consistency checks, and verifies commit interruption recovery.

Sceptre will not overwrite an existing demo database unless you opt in:

```bash
go run ./cmd/sceptre demo demo.db --rows 100000 --force
```

Use a smaller row count for quick smoke runs:

```bash
go run ./cmd/sceptre demo quick.db --rows 1000 --force
```

## 2. Create A Database Manually

```bash
go run ./cmd/sceptre sql app.db "create table users (id int64, name bytes, age int64, primary key (id))"
go run ./cmd/sceptre sql app.db "insert into users (id, name, age) values (1, 'Ada', 31)"
go run ./cmd/sceptre sql app.db "insert into users (id, name, age) values (2, 'Grace', 40)"
go run ./cmd/sceptre sql app.db "select id, name, age from users"
```

Expected shape:

```text
id  name   age
--  -----  ---
1   Ada    31
2   Grace  40
```

## 3. Add And Use An Index

```bash
go run ./cmd/sceptre sql app.db "create index users_age on users (age)"
go run ./cmd/sceptre explain app.db "select id, name from users where age = 31"
```

Expected access path:

```text
statement=select
table=users
access=secondary_index_lookup
index=users_age
lookup=age = 31
residual=none
```

## 4. Measure Query Execution

`explain-analyze` executes the query and reports row counters and stage timing:

```bash
go run ./cmd/sceptre explain-analyze app.db "select id, name from users where age = 31"
```

Output shape:

```text
plan
  statement: select
  table: users
  access: secondary_index_lookup
  index: users_age

execution
  rows_scanned: 1
  rows_matched: 1
  rows_returned: 1

stages
  secondary_index_lookup
    rows_out: 1
    time: ...
```

Use `trace` for a compact row-flow view:

```bash
go run ./cmd/sceptre trace app.db "select id, name from users where age = 31"
```

```text
trace table=users access=secondary_index_lookup
using index=users_age
1. secondary_index_lookup -> 1 row(s) in ...
2. filter_project: 1 row(s) in -> 1 row(s) out in ...
result: scanned 1, matched 1, returned 1 in ...
```

## 5. Inspect Storage

List page inventory:

```bash
go run ./cmd/sceptre inspect pages app.db
```

Decode a page:

```bash
go run ./cmd/sceptre inspect page app.db 0
```

Inspect logical table rows:

```bash
go run ./cmd/sceptre inspect table app.db users
```

Inspect index entries:

```bash
go run ./cmd/sceptre inspect index app.db users_age
```

Inspect reusable pages:

```bash
go run ./cmd/sceptre inspect freelist app.db
```

Freelist pages track old pages retired by copy-on-write commits. Those pages can
be reused by future commits instead of growing the file forever.

## 6. Validate Consistency

```bash
go run ./cmd/sceptre check app.db
```

The checker validates table rows, secondary-index entries, B+ tree page shape,
freelist state, page bounds, and storage/catalog consistency.

## 7. Verify Recovery Boundaries

```bash
go run ./cmd/sceptre crash-test scratch.db
```

This runs deterministic commit interruption scenarios at:

- `pages-written`
- `pages-synced`
- `meta-published`

For broader sampled coverage:

```bash
go run ./cmd/sceptre crash-test scratch.db --random 20 --seed 42
```

These are commit interruption tests, not process-kill fuzzing. They verify that
after reopening, the database is either at the previous committed state or the
new committed state depending on whether the meta page was published.

## 8. Interactive Shell

```bash
go run ./cmd/sceptre shell app.db
```

Shell commands:

```text
.help
.tables
.schema
.indexes
.quit
```
