# Performance

The [Go application API](docs/API.md) adds serialized multi-statement transaction
staging. Its transactions read an O(1) snapshot and merge pending writes into
scans. The published workload baseline measures direct internal table/SQL
execution, not this public API's ownership waits. It establishes no API
throughput or concurrent-request performance claim.

Sceptre's performance is described through reproducible workloads with durability
and hardware stated. See [the durable baseline](docs/BENCHMARKS.md) for current
dataset-growth, batch, mixed-workload, memory, and physical-write measurements.
No comparative performance superiority claim is established.

## Demo Result

Example run:

```bash
go run ./cmd/sceptre demo final-demo.db --rows 100000
```

Historical example from an earlier WSL run, with no recorded source revision or
complete hardware metadata. It illustrates candidate reduction and is not the
current reproducible performance baseline:

```text
without_index  table_scan              100000  248.090315ms
with_index     secondary_index_lookup  1429    14.759564ms
```

Interpretation:

- The table scan checks every row.
- The secondary index narrows candidates to matching `age` entries.
- In this dataset, scanned rows dropped from `100000` to `1429`, roughly a
  70x reduction.
- Query time improved because the executor evaluates far fewer candidate rows.

## Why Writes Are Slower Than Reads

Sceptre uses copy-on-write commits on immutable pages:

1. A write transaction copies each page it changes and rewrites the path to the
   root; committed pages are never modified.
2. The changed data pages, the changed free tree records, and the tree directory
   leaf are written.
3. Data pages are synced.
4. A new meta page is written and synced.

Every durable commit has two sync calls. Its writes grow with the number of
changed keys times the tree height, plus the free tree records it changes: the
pages it frees and the records it reuses pages from. Those depend on the
commit's own page churn rather than on the total number of free pages, and they
do not grow with the dataset. The guided demo uses `InsertMany` and `DeleteMany`
to amortize the two syncs across rows. The v1 engine rewrote the whole tree on
every commit; the v1 and v2 rows in [the benchmarks](docs/BENCHMARKS.md) show
the difference. Format v3 replaced the v2 freelist chain with the free tree; the
free tree section there measures the smaller per-commit writes that followed.

## Benchmarks

Run:

```bash
go test -bench=. ./internal/table
```

Typical benchmark categories:

```text
BenchmarkInsertRows
BenchmarkPointLookup
BenchmarkFullScan
BenchmarkSecondaryIndexLookup
```

How to read them:

- `PointLookup` should be much faster than a full scan because it uses the
  primary-key path.
- `SecondaryIndexLookup` reduces candidates but still performs row lookups and
  decoding. Per-lookup routing cost grows with tree height.
- `FullScan` should scale with total row count.
- Insert benchmarks include durable copy-on-write costs and should not be
  interpreted as append-only throughput.

## Tradeoffs

| Area | Current Choice | Impact |
|:--|:--|:--|
| Commit model | Copy-on-write path copying and meta publish | Writes per commit grow with changed keys and tree height, not dataset size |
| Reads | Snapshots over immutable pages through a bounded cache | Readers never wait for the writer; the database need not fit in memory |
| Page reuse | Transaction-tagged free tree | Pages are reused oldest freeing transaction first, once no open snapshot or fallback generation needs them; each commit edits only the free tree records it changes |
| Large values | Overflow page chains | Values up to 1 GiB; reading one walks its chain |
| Query execution | Candidate materialization before filtering | Easy stage reporting, less streaming efficiency |
| SQL scope | Small supported subset | Easier correctness, less compatibility |
| Ownership | OS lock on database file | Single owner, automatic release after process exit |

## Measurement Boundaries

LIMIT currently bounds returned rows, while candidates are materialized in memory.
The baseline records actual WriteAt bytes and sync calls, sampled Go heap,
allocation deltas, and physical file growth. It does not measure device-level
amplification, cold-cache behavior, sustained concurrent throughput, or every
power-loss condition. Read [the methodology](docs/BENCHMARKS.md) before using
timings for comparisons.
