# Performance

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

Sceptre uses copy-on-write commits:

1. Mutations update a private copy of the in-memory B+ tree.
2. The full durable tree is remapped, withholding pages needed by both valid meta slots.
3. Full tree/freelist pages are written.
4. Data pages are synced.
5. A new meta page is written and synced, then the committed tree is installed.

Every durable commit has two sync calls and rewrites the tree. The guided demo
uses `InsertMany` and `DeleteMany` to amortize publication across rows. In the
recorded 10,000-row, 128-byte-payload workload, an indexed update wrote about
3.28 MiB per row mutation. Costs grow with the tree, not just the changed row.

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
  decoding. Its resident tree and per-lookup routing costs depend on dataset size.
- `FullScan` should scale with total row count.
- Insert benchmarks include durable copy-on-write costs and should not be
  interpreted as append-only throughput.

## Tradeoffs

| Area | Current Choice | Impact |
|:--|:--|:--|
| Commit model | Full-tree copy and root publish | Tree-size-dependent writes and allocations |
| Page reuse | Freelist with dual-generation retention | Retired pages are reused after protected generations no longer need them |
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
