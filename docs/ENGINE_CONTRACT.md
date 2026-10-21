# Supported engine contract

Sceptre is an open-source relational database in Go. The current implementation
provides a single-file engine, public Go API, and CLI with a bounded SQL subset.
This document
describes implemented behavior and its limits; it is not a release-readiness claim.

## Execution and ownership

One process owns a database file. OS locks reject another Sceptre owner and release
automatically when the handle closes or the process exits. Open retries a held
lock for up to 500 ms before rejecting it: on Unix, a subprocess started by any
goroutine inherits open descriptors until it execs, so a just-closed handle can
keep its lock for that moment. Use a local filesystem
with working locks. Unix locks are advisory. Replacing or renaming an open file,
network filesystems, and mixed older/newer binaries sharing a file are unsupported.

Within the owner, the [public Go API](API.md) serializes transactions and coordinates
all its reads, writes, and close operations. Public callers may use it concurrently.
Raw internal KV, table, and SQL operations still require caller serialization;
the internal transaction manager only serializes its commits. There is no server,
connection pool, or browser integration in this version.

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
matching no rows does not publish a KV generation.

| Outcome | Meaning and required action |
| --- | --- |
| `nil` error | Data pages and new metadata have both completed OS file sync; the complete batch is visible. |
| Error before metadata publication | The mutation is rolled back in memory; the previous generation remains authoritative. |
| `kv.ErrCommitCommitted` | The post-publication test hook reported an error after the complete batch committed; do not replay it as an aborted operation. |
| `kv.ErrRecoveryRequired` | A metadata write or sync had an uncertain result; close and reopen before new operations, then reconcile application state before retrying. |

Check wrapped categories with `errors.Is`. Internal SQL mutations returning an
error have zero RowsAffected; public automatic execution can return a statement
Result alongside a commit error. Neither result is evidence of rollback.
Metadata uncertainty blocks new KV reads, writes, and iterators, and internal
transaction operations. Existing immutable iterators still hold the older
committed view. Low-level pager/tree access is an internal inspection/testing
escape hatch, not an application execution boundary.

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

`tx.Begin` reads a copied tree and buffers writes, without managed conflict
detection. `tx.Manager` adds conflict checks for point reads and writes against
later committed write sets. Scans conservatively track the whole keyspace and
conflict with any later writing commit, including phantom inserts. A conflict
closes the transaction. A definite pre-publication storage failure leaves it open
for explicit retry or abort.
Success and a known committed error close it. An uncertain publication closes
the attempted writer and blocks operations on the underlying handle until reopen.

Serialized writes outside a manager conservatively invalidate its older snapshots
when it observes a changed pager generation. Conflict history is retained only
while active snapshots need it. Long-lived snapshots can retain history and full
tree copies. Precise predicate/range conflict tracking and concurrent SQL
transactions are not implemented. The public API uses exclusive execution
ownership rather than this optimistic manager.

KV iterators retain their creation-time committed tree across successful and
failed subsequent writes. Their returned key/value bytes are copied. Table
scanners likewise use that iterator; access still must be serialized.

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
bounded output. LIMIT bounds returned rows, not work or memory: SQL collects candidates before
filtering and projection. Row bodies and index keys must fit their encoded cells
in a page; the effective limit depends on keys, escaping, metadata, and tree
structure. Default pages are 4096 bytes. There are no overflow values or silent
truncation. Unsupported sizes return an error. A universal plaintext value-size
limit independent of schema cannot be promised.

## Persistence, recovery, and compatibility

The file layout is version 1: two checksummed metadata pages, B+tree pages, and
freelist pages. Open selects the highest valid metadata generation, then rejects
invalid reachable trees or unsafe freelists. Routing validation checks cell/slot
bounds, cell overlap, key ranges, child-maximum separators, leaf depth, cycles,
and repeated children. Free IDs must be in bounds, unique, and disjoint from the
active tree and freelist metadata pages.

Before a commit reuses pages, it protects the tree and freelist pages needed by
both valid metadata slots. Allocation can reuse retired pages after that retention
window. A damaged newest metadata slot may fall back to the older complete state;
this can lose the latest acknowledged changes if that metadata was corrupted.
An invalid selected tree is rejected rather than silently switching to another
tree. Data pages lack checksums: structurally valid corruption can escape checks.

Older binaries could have damaged old fallback state through premature reuse.
These checks and retention fixes do not reconstruct already corrupted data.
Unsupported metadata versions are invalid; if no supported valid slot exists,
open fails. No automatic upgrade/downgrade or format migration is provided.
Use the same supported format implementation throughout a deployment.

There is no supported live backup/restore or hot file-copy workflow. Backups,
restores, file-format upgrades, and application deployment are not established
operational guarantees in this version.

## Implementation map

| Path | Implemented responsibility |
| --- | --- |
| `sceptre.go`, `errors.go` | Serialized public execution, transaction lifecycle, result bounds, stable error categories |
| `cmd/sceptre/main.go` | CLI dispatch, shell, rendering, demo and diagnostics |
| `internal/sql/parser.go`, `lexer.go`, `validate.go` | Syntax and schema/type binding |
| `internal/sql/plan.go`, `executor.go`, `analyze.go` | Access selection, materialized execution, diagnostics |
| `internal/table/table.go`, `scan.go`, `encoding.go` | Catalog, row/index batches, byte/int encoding and scans |
| `internal/tx/tx.go`, `store.go` | Copied KV snapshots, atomic statement staging, pending scans, conservative conflicts and lifecycle |
| `internal/sql/parameters.go` | Typed parameter token binding |
| `internal/kv/kv.go`, `iter.go` | Private working tree, allocation/remap, atomic publication, rollback and outcome handling |
| `internal/btree/tree.go`, `snapshot.go`, `validate.go` | Slotted B+tree pages, splits/merges, deep snapshots and graph validation |
| `internal/freelist/freelist.go` | Free-page chains and page allocation |
| `internal/pager/pager.go`, `meta.go`, `lock_*.go` | File ownership, reads/writes, sync, dual metadata selection |
| `internal/table/check.go`, `internal/debug/` | Integrity, inspection and deterministic interruption tooling |

Each KV commit copies the in-memory tree, remaps and writes the full tree, writes
the freelist, syncs data, publishes and syncs metadata, and installs another copied
tree. Memory and write cost therefore grow with dataset size. This is measured
behavior, not selective page copying or a bounded cache.

## Evidence and its limits

Executable regressions cover row/index statement atomicity, catalog/backfill
atomicity, short writes, sync errors, post-publication outcomes, unsafe freelists,
routing corruption, metadata fallback after reuse, immutable iterators, live
ownership exclusion, abrupt subprocess exit/reopen, and continued writes after
recovery. Seeded indexed workloads compare scans, point and index reads with a
map oracle and repeatedly reopen the database. Public API regressions verify
multi-statement catalog/row/index commit and rollback, read-your-writes, failed
statements, serialized request ownership, context waiting, close, and physical
commit errors. Seeded transaction workloads compare pending and committed index
results with a map model through rollback and reopen.

Storage and parser fuzz tests are bounded smoke experiments. CI collects coverage
and failure histories; coverage percentages are diagnostic, not correctness
proof. Platform CI configuration does not establish that a remote run passed.
See [VALIDATION.md](VALIDATION.md) for commands and replay, and
[BENCHMARKS.md](BENCHMARKS.md) for measured workload methodology.
