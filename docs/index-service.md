# Index service design contract

Status: agreed requirements and open design questions. The implemented subset
is described in the [local publication protocol](local-protocol.md). Requirements
beyond that subset remain unimplemented. SIBench's existing Index Service
protocol remains unchanged; this document does not establish wire compatibility.

## Responsibility and input boundary

Relive serves index operations for a lakehouse through an online interface.
Clients call the service; they do not coordinate other clients or act as servers.
The service must work from requests and authoritative transaction information
provided by the lakehouse integration, not from an offline workload inventory.

A SIBench Trace simulates those calls. Knowing a version identifier does not
authorize reordering operations, inventing missing transaction boundaries, or
assuming all preceding commits have arrived. The service must not read ahead in
the Trace, assume source versions are contiguous integers, or use the largest
observed version as proof of complete input or snapshot coverage.

The lakehouse integration must define the authority for transaction outcomes,
the ordering constraints, and how complete delivery of a transaction's index
changes is established. These interfaces have not been selected.

## Multiple publishers

Multiple independent service participants must be able to publish commits.
Multiple clients submitting to one fixed publisher do not satisfy this
requirement. Participants share a protocol, not an exclusive publishing process.

The local prototype uses an OS file lock shared by independent publishers.
The scalable coordination mechanism has not been selected. Log partitioning,
conditional updates, short critical sections, conflict validation, and publisher
failure handling remain design questions. Multiple publishers alone do not imply
lock-free execution or establish serializable transaction isolation.

## Commit acknowledgment and visibility

After a successful commit acknowledgment, a newly started query requesting the
current committed state through any client must include that commit's effects,
or later committed effects that supersede them. Existing queries and explicit
historical reads retain their defined read points.

The wait required to establish visibility belongs before successful commit
acknowledgment. Receiving a request or appending it to a log is not sufficient if
the query path cannot yet serve the complete committed result. Commit success
also requires the agreed persistence boundary. The local prototype forces its
shared disk journal; the production storage boundary remains undecided.

Transaction effects must become visible together, including changes spanning
primary indexes, secondary indexes, and key ranges. Prepared or aborted changes
must not leak into another transaction's reads. Transaction-local reads require
their own defined visibility rules.

Every reader need not hold an identical copy. Routing, shared reads, or catch-up
may provide a correct query path, but a stale participant must not silently
return an outdated result for a current-state query. Query latency and commit
latency must be measured separately.

These requirements must connect to the lakehouse acknowledgment exposed to its
caller. An asynchronous index update after lakehouse success is insufficient on
its own. Coordinated acknowledgment or a correct query path over committed,
not-yet-indexed changes is required; neither has been implemented or chosen.

## Real-time path and background maintenance

The real-time read path must not require waiting for SST formation, compaction,
or a batch of object-store file publications. A durable, replayable log with
queryable in-memory index changes is the working direction under discussion.
An external message broker has not been selected, and a log does not by itself
establish transaction visibility or source ordering.

The protocol must specify log ordering, transaction completion, replay and
duplicate handling, reader progress, and safe handoff to persisted index files.
Log records must not be reclaimed before the replacement state and recovery
requirements are satisfied. Cross-partition atomic visibility remains open.

## Index organization and metadata

The initial research focus is an ordered, LSM-oriented index supporting point
and range queries. A separate Hash backend is deferred. File layout, range
granularity, version organization, and maintenance policies remain undecided.

A [Hudi-style metadata table](https://hudi.apache.org/docs/metadata/) is being considered for file discovery, manifest
information, and statistics. This is an architectural reference, not a decision
to adopt Hudi or a replacement for the commit protocol.

Authoritative file membership, source-commit coverage, and snapshot mappings
must be complete and consistent. Statistics used to exclude files must never
cause false negatives; missing or insufficient statistics require conservative
reads. Approximate or delayed heat statistics may guide maintenance decisions.

The authority of the metadata table versus the commit log is undecided, as are
metadata partitioning, concurrent updates, compaction, and bootstrap. Physical
file replacement must preserve logical results and protect active readers.
Version retention, garbage collection, and scan fallback for old snapshots
require separate designs; no such fallback is assumed to exist.

## Initial evaluation environment

The proposed environment uses multiple clients and publishers on one machine,
with local disk standing in for shared EFS storage and COS available for object
storage. This does not reproduce EFS consistency, latency, or failure behavior.
No real EFS deployment is part of the initial scope.

SIBench's retained HyBench SF10 and YCSB/sysbench 87,060,000-row inputs are
candidate evaluation inputs. Equal baseline row counts do not imply equal index
sizes or operation mixes. New large runs and paid storage experiments require
separate inputs, budgets, duration limits, and stop conditions.

Before performance claims, validation must distinguish multiple independent
publishers, cross-client visibility after acknowledgment, concurrent conflicting
updates, atomic multi-index changes, in-flight and historical reads, deletion
and reinsertion, and failure around the selected acknowledgment boundary.
Tests must supply requests incrementally rather than rely on complete Trace
knowledge. Local tests cover a subset of these boundaries as described in the
protocol and README. Full source replay, capacity, power-loss recovery, and
performance acceptance have not been performed for Relive.
