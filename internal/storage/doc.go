// Package storage is Sceptre's disk-based copy-on-write B+tree engine
// (file format v2).
//
// Committed pages are immutable. A write transaction copies every page it
// changes to a new page ID and rewrites the path up to the root, so a snapshot
// is only a root page ID plus a transaction ID. Readers walk immutable pages
// through a bounded cache and never block, or are blocked by, the single
// writer. A commit writes only the pages it changed, then publishes a new meta
// page, the visibility point inherited from the v1 engine.
//
// Every page carries a CRC32C checksum, its own page ID, and the ID of the
// transaction that wrote it. Pages are verified when read from disk, and a
// snapshot rejects any page written after it, so reuse bugs or corruption
// surface as errors instead of wrong data.
//
// Pages a commit replaces are recorded in a freelist tagged with that commit's
// transaction ID. They are reused only once neither a live reader nor the
// fallback meta generation can still reach them.
package storage
