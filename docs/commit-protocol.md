# Commit Protocol

Sceptre publishes durable state by writing new pages first, syncing them, and
then publishing a new meta page. The old meta page remains available as the
fallback recovery point.

## Commit Flow

For a KV mutation batch:

1. Apply mutations to the in-memory B+ tree.
2. Build a new durable tree snapshot.
3. Allocate pages, reusing freelist pages when possible.
4. Write new tree pages and freelist pages.
5. Sync page writes to disk.
6. Publish the next meta page with a higher generation.
7. Install the committed in-process tree and freelist state.

The published meta page is the atomic visibility point for recovery.

## Crash Windows

Sceptre has deterministic failpoints at these stages:

- `pages-written`
- `pages-synced`
- `meta-published`

If a crash happens before the new meta page is published, reopen should recover
the previous committed state.

If a crash happens after the new meta page is published, reopen should recover
the new committed state.

## Verification

`sceptre crash-test <path>` exercises those commit boundaries with a table and
secondary index, reopens the database, and then runs the consistency checker.

The checker verifies that:

- the old committed row is still readable
- the new row is visible only when the meta page was published
- table/index consistency still holds after reopen
