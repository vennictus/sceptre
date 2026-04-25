# Demo

This walkthrough shows the core Sceptre story: create data, query it, explain
the query, inspect the file, validate consistency, and run crash recovery checks.

## Create A Database

```powershell
go run ./cmd/sceptre sql app.db "create table users (id int64, name bytes, age int64, primary key (id))"
go run ./cmd/sceptre sql app.db "create index users_age on users (age)"
go run ./cmd/sceptre sql app.db "insert into users (id, name, age) values (1, 'Ada', 31)"
go run ./cmd/sceptre sql app.db "insert into users (id, name, age) values (2, 'Grace', 40)"
```

## Query It

```powershell
go run ./cmd/sceptre sql app.db "select id, name from users where age = 31"
```

Expected shape:

```text
id  name
--  ----
1   Ada
```

## Explain And Analyze The Query

```powershell
go run ./cmd/sceptre explain app.db "select * from users where age = 31"
go run ./cmd/sceptre explain-analyze app.db "select * from users where age = 31"
```

`explain` shows the planned access path:

```text
statement=select
table=users
access=secondary_index_lookup
index=users_age
lookup=age = 31
residual=none
```

`explain-analyze` runs the query and adds actual counters:

```text
rows_scanned=1
rows_matched=1
rows_returned=1
stage                   rows_in  rows_out  time
-----                   -------  --------  ----
secondary_index_lookup  0        1         ...
filter_project          1        1         ...
```

## Inspect The Database

```powershell
go run ./cmd/sceptre inspect schema app.db
go run ./cmd/sceptre inspect table app.db users
go run ./cmd/sceptre inspect index app.db users_age
go run ./cmd/sceptre inspect pages app.db
go run ./cmd/sceptre inspect page app.db 0
go run ./cmd/sceptre inspect freelist app.db
```

These commands expose schema, rows, derived index entries, page inventory, and
freelist state. Use `inspect pages` to find interesting page IDs, then
`inspect page` to decode one meta, freelist, or B+ tree page.

## Check Consistency

```powershell
go run ./cmd/sceptre check app.db
```

Expected shape:

```text
status=ok
tables=1
table=users rows=2 indexes=1
issues=0
```

## Run Crash Recovery Checks

```powershell
go run ./cmd/sceptre crash-test scratch.db
```

`crash-test` creates scratch database files beside the provided path. It tests
insert, update, and delete recovery across these commit stages:

- `pages-written`
- `pages-synced`
- `meta-published`

Each recovered file is reopened and checked for consistency.
