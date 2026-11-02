# Relive

Relive is an index service project for lakehouse workloads. Its first runnable
prototype supports multiple independent JVM publishers sharing a local disk
journal, atomic multi-index commits, and point/range reads of current or retained
committed state. Clients use loopback HTTP and never access the shared directory.

The [index service design contract](docs/index-service.md) records the agreed
requirements and separates them from implementation choices still under
discussion. The [local protocol](docs/local-protocol.md) describes the implemented
subset and its limits. It now has a bounded charged Memtable, immutable sorted
files, and explicit full-merge compaction. It is not a production lakehouse
integration or a scale-validated storage engine.

## Build and run

JDK 17, Maven, and a Linux local filesystem supporting file locks and file/directory
fsync are required. From this repository:

```bash
mvn package
mkdir -p build/services
java -jar build/relive.jar build/services/demo 7001
```

In a second terminal, start an independent publisher against the same directory:

```bash
java -jar build/relive.jar build/services/demo 7002
```

The directory's parent must exist. Each server binds to `127.0.0.1`; port `0`
selects an available port. The startup line prints the address. Stop each server
with Ctrl-C. These commands create local working state under `build/`.

Commit through the first server, then immediately read through the second:

```bash
curl --fail-with-body http://127.0.0.1:7001/commit \
  -H 'Content-Type: application/json' \
  -d '{"parent":"","id":"lake-41","changes":[{"index":"customers.pk","key":"00000001","value":"0000000a","delete":false}]}'
curl --fail-with-body http://127.0.0.1:7002/read \
  -H 'Content-Type: application/json' \
  -d '{"at":null,"scans":[{"index":"customers.pk","lower":"00000001","lowerInclusive":true,"upper":"00000001","upperInclusive":true,"descending":false,"limit":1}]}'
```

The read returns `at: "lake-41"` and the value `0000000a`. This example expects a
fresh demo directory. The next new commit must name `lake-41` as its parent.
Do not automatically rebase rejected requests onto a new parent.

## Tests

```bash
mvn test
```

The default suite includes separate JVM services on loopback sockets, concurrent
publishers, immediate cross-client visibility, historical/range results,
delete/reinsert behavior, forced process termination after acknowledgment,
reopening, and deliberately invalid requests and journal frames. It requires
permission to start local sockets. Tests use temporary directories and do not
contact COS, EFS, PostgreSQL, or a lakehouse.

For a storage-only diagnostic in a socket-restricted environment, use
`mvn -Dtest=IndexStoreTest test`; this does not replace the multi-process suite.

## SIBench integration

[SIBench](https://github.com/gengdy1545/SIBench) configures this repository as
the `index/relive` submodule. The initial commit and parent gitlink are pending;
the submodule cannot be reproduced from that configuration alone.
Relive implementation belongs in this repository; SIBench owns workload capture,
Trace interpretation, replay, and result validation. SIBench's `relive` adapter
translates online calls, including transaction staging and streaming results,
without giving Relive access to the complete Trace. Use a fresh index directory
and set `target.service.adapter: relive` with a comma-separated list of HTTP
origins in `target.service.endpoint`. All endpoints must share the same directory.
The adapter uses blocking HTTP; it does not measure saturated multi-client load.

When built from SIBench, generated backend artifacts belong under
`build/index/relive/` in the SIBench checkout:

```bash
scripts/build-relive.sh
mkdir -p build/services
java -jar build/index/relive/relive.jar build/services/relive-demo 7001
```

The script runs Relive's tests and builds its standalone JAR. It does not change
the SIBench Maven project or start a benchmark.

## Current limits

Publication, log catch-up, and memory-pressure flush use a global file lock;
there is no fixed publisher process, but these critical sections are serialized.
Both data versions and commit identities spill to sorted files. The checksummed
file inventory records per-index key bounds, allowing reads to skip disjoint
files while preserving old versions and tombstones. Reads at the refreshed
current head use its known internal revision; historical commit identities
still require lookup. Selection scans the descriptor list, so overlapping files
and large inventories remain capacity concerns. The default
Memtable charge limit is 4 MiB; set `-Drelive.memtable.bytes=262144` before `-jar`
for a 256 KiB diagnostic. This accounting bound is not an RSS limit: complete
request buffers, a transaction that crosses the threshold, merge cursors, and
file inventories require additional memory.

`POST /flush` persists the current Memtable. `POST /compact` merges the currently
selected sorted files, preserving every version and tombstone; it does not flush
the tail implicitly. Call flush followed by compact to drain maintenance at a
measurement boundary. Merging runs outside the publication lock, then checks
that the selected inputs are still valid before publishing their replacement.
Compaction is explicit; there is no automatic scheduling or backlog controller.

`GET /stats` reports logical mutation bytes, journal bytes, sorted-file outputs,
compaction reads, Memtable charge, and file count. See the protocol for the exact
measurement boundaries. The file manifest is a physical checkpoint, not a
transactional Hudi-style metadata table. There is no COS backend, version GC,
exact-version relocation, or lakehouse commit adapter yet. Transaction-local
reads use shared durable staging; groups are materialized in memory. File inventories,
retained history, and merge fan-in still need capacity validation before 87m use.

Incomplete or corrupt journal frames fail closed and are retained; automatic
tail repair and power-loss recovery certification are not implemented. Local
journal fsync is the current acknowledgment persistence boundary, not COS
durability. See the local protocol for unknown commit outcomes and explicit
idempotent resubmission.

## Bounded publication diagnostic

From SIBench after `scripts/build-relive.sh`, run the Java synthetic workload
through the orchestration script, with fresh output and state paths:

```bash
python3 scripts/probe_relive.py build/index/relive/relive.jar \
  build/index/relive/test-classes results/relive-probe build/services/relive-probe \
  --maintenance
```

It uses 512 keys, two indexes, and 512 commits, checks every immediate read and
the complete initial/final states, and records maintenance drain separately.
Servers have 256 MiB heaps and a 256 KiB Memtable charge threshold. Execution is
limited to 120 seconds and 1 GiB of local state, with a 20 GiB free-space preflight.
It samples aggregate server PSS and CPU on Linux. It is a serial closed-loop
protocol diagnostic, not HyBench/YCSB/sysbench replay or a throughput limit.

## Development

Keep maintained documentation and code comments in English. Organize future
source, tests, and configuration by responsibility. Keep generated output,
credentials, machine settings, and experiment records out of source commits.
Keep runnable instructions and correctness tests current with behavior changes.
