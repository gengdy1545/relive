# Local publication protocol

This is the implemented single-machine prototype. It is independent of the
SIBench gRPC protocol and is not a complete lakehouse transaction coordinator.

## Source authority and requests

The caller supplies opaque commit identifiers and an explicit expected parent.
The empty identifier denotes the initial empty index. No arithmetic, timestamp
ordering, or knowledge of future requests is used. A new commit is accepted only
when its parent equals the current published head. A mismatch returns a conflict
without appending anything; the service does not wait for missing source commits,
reorder requests, or change their parent.

The caller is responsible for authorizing the source relationship and supplying
the complete mutation list or counted operation sequence for one index transaction. A source with branches,
out-of-order delivery, or independently acknowledged data commits needs an
integration protocol before use. The compare-parent check does not establish
source transaction validity or serializability of arbitrary business operations.

All endpoints are HTTP/JSON on loopback. There is no authentication or remote
deployment interface in this prototype.

| Endpoint | Request | Successful response |
| --- | --- | --- |
| `GET /describe` | None | Supported online capabilities and durability |
| `POST /bind` | `binding` | Persisted binding, shared index identity, current head and location |
| `POST /stage` | `transaction`, `operation`, `id`, `changes` | Accepted operation position |
| `POST /publish` | `transaction`, `parent`, `id`, `operations`, `writes` | Accepted commit identity |
| `POST /stream` | Same read request | NDJSON header, entries, and mandatory completion frame |
| `GET /head` | None | `{"id":"source-commit"}`; empty initially |
| `POST /commit` | `parent`, `id`, `changes` | `{"id":"accepted-commit"}` |
| `POST /read` | `at`, `scans` | `at` and one ordered result array per scan |
| `POST /flush` | None | Storage statistics after flushing the Memtable |
| `POST /compact` | None | Storage statistics after merging selected sorted files |
| `GET /stats` | None | Refreshed storage statistics |

A mutation has `index`, `key`, `value`, `delete`, and optional `identity` (defaults to key). Index names and commit
identifiers contain 1..128 printable non-space ASCII characters. Keys and values
are lowercase hexadecimal encodings of bytes; keys are nonempty and at most
64 KiB, identities at most 64 KiB, and values at most 64 KiB. An empty value is valid. Deletion requires
`delete: true` with no value or a null value. Omitting a value on an ordinary
write is invalid, not an implicit deletion. The service does not derive
secondary-index changes; the caller must include them in the same transaction.

`changes` is required, may be empty, and contains at most 10,000 mutations. The
last mutation of the same index/key within that transaction determines its final
value. All members are validated before publication. HTTP bodies and encoded
journal payloads are limited to 8 MiB.

A scan has `index`, nullable `lower`/`upper`, `lowerInclusive`, `upperInclusive`,
`descending`, and nonnegative `limit` (zero means unbounded). Hexadecimal key order is unsigned bytewise
lexicographic order. Equal inclusive bounds perform a point lookup. `at: null`
or an omitted `at` selects the current head; `at: ""` selects the empty state;
another identifier selects that exact retained commit. An unknown identifier
returns a conflict rather than an empty or stale result.

A read contains 1..32 scans. All scans share one captured read point, including
when they address different indexes. Each result contains `key`, `identity`, `value`, and
the commit that last changed that entry at the read point. Tombstones suppress
older values. Results have a conservative 8 MiB aggregate bound; an oversized
result fails instead of being silently truncated. Use `/stream` for larger results: its first line is `{"at":"...","scans":N}`,
followed by `{"scan":i,"entry":{...}}` lines and a final
`{"done":true,"counts":[...]}` line. Missing completion, wrong counts, or
trailing frames are failures. Ordinary scans stream without retaining all rows.
A client disconnect does not poison the storage handle.

`combine: "union"` or `"intersection"` combines at least two scans by
`identity`, returning one result stream. Union preserves first occurrence;
intersection preserves the first scan's order. Groups currently retain results
and identities in memory and have no spill support.

## Staged transactions and binding

`/stage` durably records an operation's mutations under a transaction identity.
Operations start at 1 and must arrive at the next position. A read may carry
`transaction` and `operation`; it advances that transaction's position. With
`own` equal to the transaction's proposed commit identity, it overlays all earlier
staged mutations on the explicit `at` read point. Without `own`, staged writes
remain invisible. Later writes to the same key replace earlier staged values;
a staged tombstone suppresses the committed value.

Any publisher can stage, read or publish the same transaction through shared
checksummed files. `/publish` validates the received operation and mutation
counts, commit identity and explicit parent, then appends the complete commit
and durable receipt before removing staging state. Empty transactions are valid.
Publication receipts and completed identities survive flush, compaction and
reopen. Identical publication requests are idempotent; conflicting reuse fails.
Ordinary `/commit` requests cannot supply internal publication receipts.

Staging is limited to 10,000 mutations and an 8 MiB serialized envelope per
transaction, with at most 1,024 active transaction files. Each accepted operation
rewrites and fsyncs its transaction file. This is a correctness prototype, not an
efficient transaction-log layout. Failed requests may leave active state requiring
diagnosis; there is no abort, expiry or staging-file collector. Stage/read
positions are not retry-idempotent. A failed or disconnected read may have
advanced its position; the driver stops instead of replaying it automatically.

`/bind` creates a durable input binding and random shared-index identity on an
empty index, or verifies an existing binding. The adapter checks all endpoints
refer to the same identity. It is a setup check on trusted loopback endpoints,
not authentication or a per-request access token.

## Publication and visibility

Every server opens the same `publish.lock` and `journal` files. Any server can
publish. An OS file lock serializes journal refresh and append across processes;
a per-directory JVM lock also prevents closing another handle from releasing
the process's POSIX locks during publication. Lock acquisition has a five-second
budget. Store calls within one instance are synchronized.

The journal begins with a file-type marker. Each frame contains a bounded
payload length, the complete transaction JSON, a SHA-256 digest, and a completion
marker. There is no supported legacy format or format revision field. Start new experiments
with fresh state; retained states from earlier prototypes require their matching
frozen binary, rather than an in-place upgrade.

While holding the lock, a publisher refreshes prior complete frames, validates
the parent, appends one complete frame, forces the journal to local storage,
and installs its transaction into the local Memtable. Normal publication requires
no batch timer, object upload, or compaction. When a complete transaction brings
the Memtable charge to its limit, a synchronous flush provides backpressure
before returning. This flush latency is part of the triggering request, not
hidden behind an acknowledgment. Directory creation and initial file entries
are also forced before the store becomes available.

A current-state read acquires the same lock, refreshes complete frames, and
captures a read point and opens the selected immutable files before releasing
the lock. It then merges their ordered records with the Memtable. Therefore,
reads beginning after a successful acknowledgment cannot return
state preceding that commit, although reads may observe later commits. An
already-started query can retain its captured state. Catch-up contributes to
query latency; append, fsync, and publication contribute to commit latency.

The internal history ordinal is private to the index and does not reinterpret
source commit identifiers. All historical versions are retained. No full Trace
or source inventory is accepted by the service.

## Sorted storage and compaction

Data records and commit-identity/digest records share the same ordered storage;
commit identities no longer require an unbounded in-memory map. The reserved
empty index namespace holds identity records and is unavailable to callers.
Records are ordered by index, key, and internal revision. A file contains framed
records, an on-disk offset directory, and a checked footer. Binary search reads
the directory from disk; the full key directory is not loaded into memory.
Each record checksum includes its ordinal, so an offset pointing at a different
valid record is rejected. Versions and tombstones are preserved without GC.

Memtable charge is `256 + 2 * (index + key + identity + value + commit string lengths)` per
stored record; replacement of the same transaction-local key updates the charge.
The threshold is checked only after complete transactions, so the transient
charge can exceed it by one bounded transaction. Request parsing, result buffers,
merge cursors, and the file inventory are additional memory. The limit is
1 KiB..256 MiB, default 4 MiB. Smaller limits are useful for forced-spill tests.

A flush forces a new sorted file and its directory entry, then atomically
replaces and forces a checksummed `layout` containing the exact run inventory,
covered journal byte position, head identity, and counters. Each file descriptor
also carries its exact minimum and maximum key for every index present in that
file, including the reserved metadata index. Bounds are derived while writing
all records, including tombstones and every retained version; they are published
atomically with file membership. Invalid or missing descriptors are rejected.
Other publishers reload this checkpoint and replay only its journal tail. The layout generation
identifies a physical replacement, not a source commit or format revision.

A read opens only files whose per-index bounds may intersect at least one of its
scans, respecting inclusive/exclusive endpoints. Files are selected and pinned
under the publication lock. Grouped scans use the union of candidate files;
all selected versions still go through ordinary merge/visibility checks. Bounds
never represent only live or latest entries: dropping deleted or old keys from
them would incorrectly prune historical reads. Irrelevant files are not audited
by that query; checksums remain checked on the files/records actually read.

After refresh, a read at the current head uses its already established internal
revision directly. Historical identities still require a retained commit record;
unknown identities fail. Metadata lookups may also exclude files by the reserved
index's bounds. No arithmetic is applied to source commit identifiers.

Selection still examines the complete descriptor inventory on each read and
reloads the layout; it is not a separate metadata index or an inventory cache.
Overlapping key ranges and full scans may still need every file. Descriptor
bytes and parsing are additional memory/I/O costs; they do not change the
Memtable charge limit. Use fresh state for this checkpoint layout; previous
prototype states remain paired with their frozen binaries.

Explicit compaction opens and pins its input file descriptors under the
publication lock, merges outside that lock with one cursor per input, and
rechecks input membership before publication. New files published meanwhile
remain in the replacement layout. If another compactor already replaced an
input, the unused output is discarded and its completed I/O is still counted.
After the new layout is durable, obsolete inputs are unlinked. Existing readers
can finish through their open descriptors on the tested Linux filesystem.
This behavior is not certification for EFS or other filesystems.

Compaction currently merges all selected runs and never drops logical history.
It is a simple write-amplification reference, not a range-aware or tiered policy.
No automatic compaction scheduler, orphan-file collector, or log reclamation is
implemented. Stop services before manual diagnosis; never edit live files.

## Measurement boundaries

Statistics are shared through the layout and journal replay, not summed across
processes. Reading statistics refreshes the caller and can trigger a
memory-pressure flush, just like other catch-up operations.

- `activeTransactions` / `stagedBytes`: current transaction-file count / bytes, not cumulative staging writes.
- `logicalMutationBytes`: UTF-8 bytes of each normalized mutation JSON, counting
  every accepted operation including deletes, excluding commit envelope and
  identity metadata. Exact idempotent resubmissions do not add bytes.
- `walBytes`: journal size excluding its file-type marker, including frame and
  commit metadata. The journal is retained after flushing.
- `flushBytes`: generated sorted-file lengths from completed flushes.
- `compactionReadBytes`: actual application file-read bytes in completed merges,
  including offset-directory reads; not physical device I/O or cache misses.
- `compactionWriteBytes`: generated sorted-file lengths from completed merges,
  including outputs discarded after a publication conflict.

Sorted-file write counters include file headers, record checksums, commit
identity records, the offset directory, and footer. They exclude transaction staging, binding writes, the temporary
offset spool, layout replacements, filesystem metadata, and failed incomplete
outputs; they must not be called total filesystem write amplification. Report
WAL and sorted-file amplification separately, with initial loading separate from
workload deltas. Include final flush/compaction and its elapsed time rather than
treating unprocessed maintenance as saved work. Full input-file scans and old
snapshot access can still be expensive, and file count bounds merge memory and
descriptor usage.

## Errors and reopening

HTTP 400 rejects an invalid request, 409 reports a parent/identity/read-point
conflict, 413 rejects an oversized HTTP body, and 503 reports storage failure.
Unknown paths return 404 and unsupported methods return 405.

A repeated commit identifier with the same normalized request is idempotent:
it returns that identifier without another append, even if the head has advanced.
Reusing it with a different parent or mutation sequence returns 409. Clients
do not automatically retry. A timeout, disconnected response, or I/O failure
can leave the outcome unknown. Explicitly resubmitting the identical request
can confirm it after the store is healthy; do not invent a new identifier or
parent as an automatic retry strategy.

Reopening verifies the layout and replays complete journal frames after its
covered position. Sorted record checksums are checked when records are read;
covered journal bytes and every sorted record are not rescanned on each open.
A complete frame left
by a process that exited before acknowledgment may become visible; readers force
new complete frames before serving them. This is an unknown-outcome commit, not
a promise that an unacknowledged write was rolled back.

Invalid lengths, incomplete frames, broken parent chains, duplicate commit IDs,
and digest failures stop replay. The journal is never silently truncated or
repaired. I/O failures on publication and query paths poison the current store
handle; diagnose state before reopening. A failed compaction before publication
leaves the old layout authoritative and may leave unreferenced diagnostic files.
External file replacement, directory relocation, or editing while services are
running is unsupported. Checksums are not continuous auditing of cached bytes.

This protocol has no log reclamation, distributed fencing, automatic crash-tail
repair, or power-loss certification. File locks are a local Linux prototype
choice, not a proof of correctness on EFS. Large-scale capacity, real-time
latency bounds, and a transactional metadata table remain unvalidated.
