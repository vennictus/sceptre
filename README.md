<div align="center">

<pre>
 ____   ____ _____ ____ _____ ____  _____ 
/ ___| / ___| ____|  _ \_   _|  _ \| ____|
\___ \| |   |  _| | |_) || | | |_) |  _|  
 ___) | |___| |___|  __/ | | |  _ <| |___ 
 |____/ \____|_____|_|    |_| |_| \_\_____| 
  
</pre>

**An open-source relational database built in Go**

[![CI](https://github.com/vennictus/sceptre/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/vennictus/sceptre/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/vennictus/sceptre)](https://goreportcard.com/report/github.com/vennictus/sceptre)
[![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

<p align="center">
  <strong>Single-file storage | B+ tree indexes | Query tracing | Page inspection | Crash recovery checks</strong>
</p>

---

[Quick Start](#quick-start) |
[Installation](#installation) |
[Features](#features) |
[Command Reference](#command-reference) |
[Documentation](#documentation) |
[Development](#development)

</div>

## What Is Sceptre?

Sceptre is an open-source relational database in Go, built around a single-file
B+tree engine. The current implementation supports a bounded SQL subset through
the CLI and a public Go API, with typed tables, secondary indexes, atomic
statement mutations,
inspection, and tested recovery boundaries.

Its product direction is a dependable general-purpose database with useful
query and storage diagnostics. The current supported behavior and limits are
documented in [the engine contract](docs/ENGINE_CONTRACT.md). The
[Go API](docs/API.md) supports typed parameters and serialized multi-statement
transactions. The CLI includes a built-in interactive shell, and
`sceptre serve` runs a local HTTP service with Sceptre Studio, a browser
workbench; remote network serving is not implemented yet.

---

## Quick Start

Run the full guided demo:

```bash
go run ./cmd/sceptre demo
```

The demo loads 100,000 generated rows, compares the same query before and after
an index, deletes about 30% of the data, shows freelist/page inspection, runs a
consistency check, and verifies crash recovery boundaries.

For a smaller smoke run:

```bash
go run ./cmd/sceptre demo quick.db --rows 1000
```

If a demo database already exists, Sceptre refuses to overwrite it. Use
`--force` only when you want to replace that file:

```bash
go run ./cmd/sceptre demo quick.db --rows 1000 --force
```

---

## Installation

```bash
# Install the CLI
go install github.com/vennictus/sceptre/cmd/sceptre@latest

# Or clone and run from source
git clone https://github.com/vennictus/sceptre.git
cd sceptre
go run ./cmd/sceptre demo
```

**Requirements:** Go 1.22 or higher.

---

## Features

<table>
<tr>
<td width="50%">

### Storage Engine
- **Disk-based copy-on-write B+ tree**: commits write only changed pages
- **Snapshot readers** that never block or wait for the writer
- **Checksummed pages** with page IDs and writing-transaction IDs
- **Large values** in overflow pages, up to 1 GiB
- **Bounded page cache**: the database does not need to fit in memory

</td>
<td width="50%">

### Indexing & Tables
- **B+ tree key/value storage** with ordered iteration
- **Typed table rows** with primary-key encoding
- **Secondary indexes** used by the query planner
- **Batched inserts/deletes** for efficient demo and table workloads

</td>
</tr>
<tr>
<td width="50%">

### Query Observability
- **EXPLAIN** for chosen access paths
- **EXPLAIN ANALYZE**-style counters via `explain-analyze`
- **TRACE** for row flow through execution stages
- **Human-readable demo takeaways** for index and timing impact

</td>
<td width="50%">

### Reliability Tooling
- **Consistency checker** that verifies every page, the data and free trees, free-page accounting, and all rows and indexes
- **Commit interruption tests** at page-write, sync, and meta-publish stages
- **Randomized recovery sampling** with reproducible seeds
- **OS ownership lock** released automatically on process exit

</td>
</tr>
</table>

---

## Demo Output

A 100k-row run shows the core story:

```text
== 6. performance comparison ==
query          access                  rows_scanned  time
-------------  ----------------------  ------------  ------------
without_index  table_scan              100000        248.090315ms
with_index     secondary_index_lookup  1429          14.759564ms
takeaway: index reduced scanned rows from 100000 to 1429 (70.0x fewer rows scanned)
takeaway: query time changed from 248.090315ms to 14.759564ms (16.8x faster)

== summary ==
data: loaded 100000 rows, then deleted 30000 rows
index: table_scan scanned 100000 rows; secondary_index_lookup scanned 1429 rows
storage: freelist has 9 metadata page(s) tracking 4181 reusable page(s)
reliability: consistency check ok; crash recovery ok across 9 cases
```

---

## Command Reference

| Command | Purpose | Example |
|:--|:--|:--|
| `demo` | Run the guided engine walkthrough | `sceptre demo demo.db --rows 100000` |
| `sql` | Execute one SQL statement | `sceptre sql app.db "select * from users"` |
| `shell` | Start an interactive SQL shell | `sceptre shell app.db` |
| `serve` | Run the local HTTP service and Studio | `sceptre serve app.db` |
| `explain` | Show the planned access path | `sceptre explain app.db "select * from users where age = 42"` |
| `explain-analyze` | Run a select and show measured counters | `sceptre explain-analyze app.db "select * from users where age = 42"` |
| `trace` | Show compact execution row flow | `sceptre trace app.db "select * from users where age = 42"` |
| `check` | Validate database consistency | `sceptre check app.db` |
| `upgrade` | Convert a format v1 database to v3 | `sceptre upgrade app.db` |
| `crash-test` | Verify commit interruption recovery | `sceptre crash-test scratch.db --random 20 --seed 42` |
| `inspect meta` | Show active meta-page state | `sceptre inspect meta app.db` |
| `inspect pages` | List physical page inventory | `sceptre inspect pages app.db` |
| `inspect page` | Decode one physical page | `sceptre inspect page app.db 2` |
| `inspect freelist` | Show reusable page state | `sceptre inspect freelist app.db` |
| `inspect table` | Show schema and logical rows | `sceptre inspect table app.db users` |
| `inspect index` | Show derived index entries | `sceptre inspect index app.db users_age` |

---

## Supported SQL

Sceptre intentionally supports a small SQL subset:

```sql
CREATE TABLE users (id int64, name bytes, age int64, city bytes, PRIMARY KEY (id));
CREATE INDEX users_age ON users (age);
INSERT INTO users (id, name, age, city) VALUES (1, 'Ada', 31, 'delhi');
SELECT id, name FROM users WHERE age = 31;
UPDATE users SET age = 32 WHERE id = 1;
DELETE FROM users WHERE id = 1;
```

Supported planner paths include table scans, primary-key lookups,
primary-key ranges, and exact secondary-index lookups. Joins, aggregations,
ordering, SQL transaction syntax, and broad SQL compatibility are outside the current
scope.

SQL validates column references, predicate types, and update assignments before
scanning, including queries that match no rows. Primary-key assignments in
`UPDATE` are rejected; key relocation is not supported. Duplicate INSERT columns
and UPDATE targets are rejected. `LIMIT` and `OFFSET` must be non-negative;
`LIMIT 0` returns no rows. Limits bound returned rows, while candidate rows are
still collected in memory before filtering.

Each SQL UPDATE or DELETE stages its matching row and index changes into one KV
commit. Validation failures and oversized keys (above 1000 bytes at the default
page size) preserve the previous statement state; rows can be up to 1 GiB.
The public Go API coordinates concurrent callers and buffers multiple
statements, including schema and index changes, until Commit. Raw internal engine
access still requires serialization. A commit error after metadata publication
can still mean the complete transaction was committed.

Every page is verified by checksum when read, and `sceptre check` verifies the
whole file. Pages are reused only when neither an open snapshot nor the previous
meta generation can reach them, so snapshots never change underneath readers and
the previous generation stays intact. Uncertain metadata I/O errors require
closing and reopening the handle before further operations. Regression coverage
includes torn writes, sync failures, subprocess exits, fallback after an
interrupted commit, randomized workloads checked against a model, and seeded
indexed CRUD workloads.

Databases created before format v2 must be converted once with
`sceptre upgrade <db>`, which verifies the copy and keeps the original as a
backup. Format v2 files open normally and convert to v3 with their first commit.
Format v3 keeps free pages in a B+ tree; binaries that only know v2 reject v3
files, and there is no downgrade.

The detailed supported boundaries are in [PRODUCT_SPEC.md](PRODUCT_SPEC.md).

---

## Project Structure

```text
sceptre/
├── sceptre.go           # Public Go API and serialized SQL transactions
├── cmd/sceptre          # CLI entry point, terminal formatting, serve command
├── cmd/sceptre-bench    # Durable workload measurement runner
├── docs                # Engine contracts, validation, measured baselines
├── internal/bench      # Workloads and measurement accounting
├── internal/service     # Local HTTP service and embedded Studio
├── internal/sql         # Lexer, parser, planner, executor, analyze reports
├── internal/table       # Schemas, records, primary keys, secondary indexes, checks
├── internal/tx          # Internal transaction primitives and conflict detection
├── internal/kv          # Atomic key/value batches, snapshots, iterators
├── internal/storage     # Copy-on-write B+ tree engine: pages, cache, free tree, commits
├── internal/upgrade     # Format v1 to v3 conversion (with internal/btree for v1)
├── internal/pager       # File pages, meta slots, format versions, fsync, ownership lock
└── internal/debug       # Inspection and crash/recovery tooling
```

---

## Documentation

| Document | Description |
|:--|:--|
| **[GUIDE.md](GUIDE.md)** | Hands-on walkthrough of the CLI, demo, inspection, tracing, and recovery tools |
| **[PRODUCT_SPEC.md](PRODUCT_SPEC.md)** | Technical design, storage model, transaction scope, file format, and recovery protocol |
| **[Engine contract](docs/ENGINE_CONTRACT.md)** | Supported guarantees, concurrency and compatibility limits, and implementation map |
| **[Go API](docs/API.md)** | Typed SQL parameters, multi-statement transactions, lifecycle, bounds, and stable errors |
| **[Local service](docs/SERVICE.md)** | `sceptre serve` HTTP API, Studio, security model, transactions, and limits |
| **[Validation](docs/VALIDATION.md)** | Regression, race, coverage, fuzz, and saved-workload replay commands |
| **[Benchmarks](docs/BENCHMARKS.md)** | Reproducible durable workload methodology and measured baseline |
| **[PERFORMANCE.md](PERFORMANCE.md)** | Demo results, benchmarks, and performance tradeoffs |

---

## Development

```bash
# Run all tests
go test ./...

# Run static checks
go vet ./...

# Run table/storage benchmarks
go test -bench=. ./internal/table

# Run the guided demo
go run ./cmd/sceptre demo demo.db --rows 100000
```

Requirements: Go 1.22 or newer.

On locked-down Windows machines, Application Control may block generated Go
test executables. Running the same commands from WSL avoids that policy in most
setups.

---

## Acknowledgements

Sceptre was initially inspired by James Smith's *Build Your Own Database From
Scratch in Go: From B+Tree To SQL*. The project follows the same bottom-up
learning path of B+ tree storage, durability, tables, indexes, transactions,
and SQL-like execution, then extends it with a stronger CLI demo, inspection
commands, consistency checks, tracing, and project documentation.

---

<div align="center">

**Built with Go**

[Back to top](#what-is-sceptre)

</div>
