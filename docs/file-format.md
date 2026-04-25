# File Format

Sceptre stores a database in one file made of fixed-size pages. The default page
size is chosen by the pager when a file is created, and the configured size is
persisted in the meta pages.

## Page Layout

The first two pages are reserved meta pages:

- page `0`: meta slot 0
- page `1`: meta slot 1
- page `2+`: tree pages and freelist pages

The active meta page points at the current durable root page, freelist head
page, page count, page size, and generation.

## Meta Pages

Sceptre keeps two meta pages so a commit can publish a new root without
overwriting the only known-good root. On open, the pager validates both meta
slots and chooses the valid slot with the highest generation.

Each meta page stores:

- magic/version data
- page size
- root page
- freelist page
- page count
- generation
- checksum

If the newest meta page is corrupt but the older one is valid, recovery falls
back to the older durable state.

## Tree Pages

B+ tree pages store either leaf cells or internal cells.

Leaf cells contain ordered key/value pairs. Internal cells contain child page
references and separator keys derived from each child subtree. The tree uses
ordered iteration for scans and key/value reconstruction during reopen.

## Freelist Pages

The freelist is persisted as pages referenced by the active meta page. It tracks
retired page IDs that can be reused by later commits, preventing the file from
growing forever during repeated updates.

## Higher-Level Keys

The table layer maps relational structures onto ordered KV keys:

- catalog keys store table definitions
- row keys store primary-key ordered rows
- index keys store secondary-index entries followed by primary-key values

Integer values are encoded in order-preserving big-endian form. Byte strings
use escaping so they can participate in ordered composite keys.
