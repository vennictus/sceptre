# Isolation

Sceptre is an embedded, single-process database. The current transaction model
is intentionally small and designed for local use, not multi-process writers.

## Transaction Model

Transactions buffer writes until commit. Reads observe a stable snapshot plus
the transaction's own pending writes where applicable.

Supported behavior:

- commit
- abort
- read-your-own-write
- snapshot-style reads
- serialized commit application
- optimistic conflict detection

## Conflict Detection

The transaction manager tracks snapshot generations and write sets. A commit can
fail if another transaction has changed keys that conflict with the committing
transaction's snapshot.

This is not a full MVCC implementation. It is a compact optimistic model that is
useful for demonstrating transaction visibility and conflict handling in an
embedded engine.

## Scope

V1 does not attempt:

- multi-process writer coordination
- distributed transactions
- long-running MVCC garbage collection
- SQL isolation-level configuration

The intended guarantee is a simple local transaction API with atomic commit,
abort, snapshot reads, and documented conflict behavior.
