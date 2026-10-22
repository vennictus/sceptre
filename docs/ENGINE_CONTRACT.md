# Supported engine contract

Sceptre is an open-source relational database in Go. The current implementation
provides a single-file engine, public Go API, and CLI with a bounded SQL subset.
This document describes implemented behavior and its limits; it is not a
release-readiness claim.

## Execution and ownership

One process owns a database file. OS locks reject another Sceptre owner and release
automatically when the handle closes or the process exits. Open retries a held
lock for up to 500 ms before rejecting it: on Unix, a subprocess started by any
goroutine inherits open descriptors until it execs, so a just-closed handle can
keep its lock for that moment. Use a local filesystem with working locks. Unix
locks are advisory. Replacing or renaming an open file, network filesystems, and
mixed older/newer binaries sharing a file are unsupported.

Within the owner, the storage engine allows one writer at a time and any number
of concurrent readers. Readers use snapshots and never block, or wait for, the
writer. The KV layer is safe for concurrent use on that basis. Table and SQL
operations combine several reads and writes, so raw internal table/SQL use still
requires caller serialization. The [public Go API](API.md) serializes
transactions and coordinates all its reads, writes, and close operations; public
callers may use it concurrently. `sceptre serve` adds a loopback-only HTTP
service and Studio over the same API ([local service](SERVICE.md)); there is no
network server, user accounts, TLS, or connection pool in this version.

## Atomicity and commit outcomes

| Operation | Publication boundary |
| --- | --- |
| KV `Apply` | Entire mutation batch |
| Table `Insert`, `Update`, `Upsert`, `Delete` | Row and associated index entries |
| Table `InsertMany`, `UpdateMany`, `DeleteMany` | Entire batch and its indexes |
| `CreateTable` | Catalog entry and prefix allocation |
| `CreateIndex` | Catalog change, prefix allocation, and complete backfill |
| SQL INSERT, UPDATE, DELETE | Entire statement and associated indexes |
| Internal KV transaction commit | Buffered KV mutations |
| Public Go transaction commit | All buffered schema, row, and index changes across statements |

Catalog and index changes are assembled before the same KV publication as rows.
Validation, encoding, allocation, and supported data-write/data-sync failures
before metadata publication preserve the previous committed state. Failed bulk
staging does not publish earlier rows. SQL rejects primary-key assignments,
duplicate input columns, and duplicate update targets before execution. A mutation
matching no rows does not publish a new generation.

| Outcome | Meaning and required action |
| --- | --- |
| `nil` error | Data pages and new metadata have both completed OS file sync; the complete batch is visible. |
| Error before metadata publication | The write transaction is discarded; nothing was published and the previous generation remains authoritative. |
| `kv.ErrCommitCommitted` | The post-publication test hook reported an error after the complete batch committed; do not replay it as an aborted operation. |
| `kv.ErrRecoveryRequired` | A metadata write or sync had an uncertain result; close and reopen before new operations, then reconcile application state before retrying. |

Check wrapped categories with `errors.Is`. Internal SQL mutations returning an
error have zero RowsAffected; public automatic execution can return a statement
Result alongside a commit error. Neither result is evidence of rollback.
Metadata uncertainty blocks new KV reads, writes, snapshots and iterators, and
internal transaction operations. Existing iterators keep reading their older
committed snapshot. Low-level pager/storage access is an internal
inspection/testing escape hatch, not an application execution boundary.

Successful writes depend on the OS, filesystem, and device honoring sync.
Process-exit tests do not prove power-loss behavior. The parent directory of a
newly created file is not explicitly synced; survival of its directory entry
through power loss is not an established guarantee.

## Transactions and snapshots

SQL text does not expose BEGIN, COMMIT, or ROLLBACK. The public Go API exposes
Begin, Execute, Commit, and Rollback. Table/SQL execution within a public transaction
uses transaction-aware KV reads, scans, and atomic statement staging. Schema and
index backfill participate alongside rows. Pending changes are visible to that
transaction. The API serializes whole transactions, including read-only work,
so concurrent public requests execute in a serial order. Failed statements leave
prior successful work intact and the transaction usable. Public Commit ends it
on every outcome; Close aborts open work. See [API.md](API.md) for lifecycle,
cancellation, and outcome handling. CLI statements still commit independently.

`tx.Begin` takes a storage snapshot, an O(1) operation that copies nothing, and
buffers writes. Reads consult the buffer, then the snapshot; scans merge the
sorted buffer over a snapshot cursor. Statement validation checks key and value
sizes before buffering any mutation of the statement. Commit applies the buffer
in one write transaction. `tx.Begin` has no managed conflict detection.
`tx.Manager` adds conflict checks for point reads and writes against later
committed write sets. Scans conservatively track the whole keyspace and conflict
with any later writing commit, including phantom inserts. A conflict closes the
transaction. A definite pre-publication storage failure leaves it open for
explicit retry or abort. Success and a known committed error close it. An
uncertain publication closes the attempted writer and blocks operations on the
underlying handle until reopen.

Serialized writes outside a manager conservatively invalidate its older snapshots
when it observes a changed generation. Conflict history is retained only while
active snapshots need it. Precise predicate/range conflict tracking and
concurrent SQL transactions are not implemented. The public API uses exclusive
execution ownership rather than this optimistic manager.

A snapshot, KV iterator, or table scanner holds its committed state until it is
released or closed. Pages that state needs are never reused while it is open, so
later commits cannot change what it reads; a long-lived snapshot makes the file
grow instead. Unreachable iterators are released by the garbage collector as a
backstop, but callers should close them. Returned key/value bytes are copies.

## SQL and values

| Supported | Limits |
| --- | --- |
| CREATE TABLE, CREATE INDEX | No ALTER/DROP, migrations, unique secondary indexes, foreign keys, or checks |
| INSERT | One literal or parameter-bound row; all declared fields required; duplicate primary key rejected |
| SELECT | Projection, WHERE comparisons, AND/OR and parentheses, nonnegative LIMIT/OFFSET |
| UPDATE | Literal or parameter assignments to non-key columns |
| DELETE | Filtered or all-row deletion |
| Access paths | Full scan, complete primary-key lookup, single-column primary-key range, complete secondary-index equality lookup |
| Diagnostics | EXPLAIN, measured SELECT analysis, tracing, schema/page/index inspection, integrity checking |

Types are signed int64 and arbitrary bytes. Integers parse within int64 bounds.
Byte comparisons use byte order, with no locale collation or implicit numeric
conversion. SQL string literals use single quotes and doubled embedded quotes.
Keywords are case insensitive; stored identifiers are matched by their spelling.
There are no NULL values, floating-point/boolean columns, arithmetic expressions,
joins, aggregates, or explicit ORDER BY. The public API supports positional typed
`?` bindings in literal positions, with exact binding counts and no SQL text
interpolation. CLI parameter input is not implemented.

Columns, predicate types, assignments, projections, and pagination bounds are
checked even for empty tables or no-match access paths. LIMIT 0 is valid. Natural
table access follows primary-key order; secondary access follows index-key order.
An explicit stable SQL ordering contract is not implemented.

The public API caps returned SELECT rows (default 1000), SQL text (65536 bytes),
and binding count (256). Raw internal SQL and the CLI require explicit LIMIT for
bounded output. LIMIT bounds returned rows, not work or memory: SQL collects
candidates before filtering and projection.

Size limits come from the page size (a power of two from 1024 to 65536 bytes,
default 4096):

| Item | Limit |
| --- | --- |
| Stored key (encoded primary key; index entry of index values plus primary key) | `(pageSize − 26) / 4 − 17` bytes: 1000 at 4096-byte pages |
| Stored value (encoded row) | 1 GiB |

Keys carry table or index prefixes and order-preserving encoding, so the column
data that fits is somewhat below the key limit. Values that would take more than
about a quarter of a page are stored in overflow pages. Oversized keys or values
fail the whole statement with the `limit` category; nothing is truncated.

## Persistence, recovery, and compatibility

The file layout is format v2:

- Two checksummed meta pages hold the format version, page size, tree directory
  root, freelist head, page count, and generation (the transaction ID).
- Every other page starts with a CRC32C checksum, its own page ID, its type, and
  the ID of the transaction that wrote it.
- The tree directory maps tree names to root pages. One tree, `data`, holds the
  catalog, rows, and index entries today.
- Branch and leaf pages form a B+tree whose branch keys equal the largest key in
  each child. Large values live in overflow page chains.
- The freelist records each free page with the transaction that freed it.

Committed pages are immutable. A commit copies each page it changes to a new
page ID and rewrites the path to the root, then writes the directory leaf and
freelist, syncs, publishes the next meta page, and syncs again. The meta page is
the visibility point.

Open selects the highest valid meta generation, then verifies the tree directory
and loads the freelist, rejecting entries that are out of bounds, duplicated, or
overlap the freelist's own pages. Open does not walk the data tree. Every page is
verified when it is read from disk: a checksum, page-ID, or type mismatch returns
a corruption error instead of data, and a snapshot also rejects any page written
after it. `sceptre check` (table `Check`) verifies the whole state first: both
trees in full (key order, separators, uniform depth, overflow chains, inline
storage exactly when a value fits), the on-disk freelist against memory, and an
accounting in which every page in the file has exactly one owner.

A page freed by transaction T is reused only after T is at or before both the
previous generation and every open snapshot. So the older meta slot always
describes a complete state: if the newest meta page is lost, even right after an
interrupted commit wrote reused pages, open falls back to the previous complete
state. That fallback can lose the latest acknowledged changes. Checksums detect
corruption; they do not repair it, and no redundant copy of data pages exists.

Format v1 files (the earlier in-memory engine) return `ErrUpgradeRequired`.
`sceptre upgrade <db>` converts one: it copies every entry into a temporary v2
file, verifies it with a full check and a hash comparison of every key and
value, then renames the original to `<db>.v1.bak` and installs the new file. A
failure leaves the original in place. Upgrade moves unsupported v1 page sizes,
such as 512, to 4096. v2 binaries never write v1 files, and there is no
downgrade.

There is no supported live backup/restore or hot file-copy workflow. Backups,
restores, and application deployment are not established operational
guarantees in this version.

## Implementation map

| Path | Implemented responsibility |
| --- | --- |
| `sceptre.go`, `errors.go` | Serialized public execution, transaction lifecycle, result bounds, stable error categories |
| `cmd/sceptre/main.go`, `upgrade.go` | CLI dispatch, shell, rendering, demo, diagnostics, v1 upgrade |
| `cmd/sceptre/serve.go`, `internal/service` | Loopback HTTP service, request security, transaction ownership and expiry, wire encoding, embedded Studio |
| `internal/sql/parser.go`, `lexer.go`, `validate.go` | Syntax and schema/type binding |
| `internal/sql/plan.go`, `executor.go`, `analyze.go` | Access selection, materialized execution, diagnostics |
| `internal/table/table.go`, `scan.go`, `encoding.go` | Catalog, row/index batches, byte/int encoding and scans |
| `internal/tx/tx.go`, `store.go` | Snapshot reads, buffered writes, merged pending scans, conservative conflicts and lifecycle |
| `internal/sql/parameters.go` | Typed parameter token binding |
| `internal/kv/kv.go`, `iter.go` | Atomic batches, snapshots and iterators over the storage engine |
| `internal/storage/` | Copy-on-write B+tree, overflow chains, page checksums, page cache, freelist, readers, commit protocol, full verification and inspection |
| `internal/upgrade/`, `internal/btree/` | Reading format v1 files and converting them to v2 |
| `internal/pager/pager.go`, `meta.go`, `lock_*.go` | File ownership, page reads/writes, sync, dual meta slots and format versions |
| `internal/table/check.go`, `internal/debug/` | Integrity, inspection and deterministic interruption tooling |

A commit's write cost grows with the number of changed keys times the tree
height, plus the freelist, which is rewritten in full on each commit and grows
with the number of free pages. It does not grow with the dataset size. Reads go
through a page cache bounded at 64 MiB by default. A write transaction keeps the
pages it changed in memory until commit, so very large transactions are bounded
by memory. Open loads the freelist into memory.

## Evidence and its limits

Storage engine tests compare randomized workloads with a map model under full
structural verification and page accounting after every commit, including
reopens and aborted transactions. They cover snapshot isolation, readers held
across many commits, fallback after an interrupted commit plus a lost meta page,
checksum detection, corruption that keeps a valid checksum (sealed bad freelist
entries and separators), every commit failure stage, torn meta writes, and
concurrent readers under the race detector. Deliberately planted bugs in page
reuse, freed-page tracking, and node merging are each caught by these tests.

Executable regressions cover row/index statement atomicity, catalog/backfill
atomicity, short writes, sync errors, post-publication outcomes, metadata
fallback after reuse, immutable iterators, live ownership exclusion, abrupt
subprocess exit/reopen, and continued writes after recovery. Seeded indexed
workloads compare scans, point and index reads with a map oracle and repeatedly
reopen the database. Public API regressions verify multi-statement
catalog/row/index commit and rollback, read-your-writes, failed statements,
serialized request ownership, context waiting, close, and physical commit
errors. Seeded transaction workloads compare pending and committed index results
with a map model through rollback and reopen. Upgrade tests convert a database
written by the last v1 release.

Storage and parser fuzz tests are bounded smoke experiments. CI collects coverage
and failure histories; coverage percentages are diagnostic, not correctness
proof. Platform CI configuration does not establish that a remote run passed.
See [VALIDATION.md](VALIDATION.md) for commands and replay, and
[BENCHMARKS.md](BENCHMARKS.md) for measured workload methodology.
