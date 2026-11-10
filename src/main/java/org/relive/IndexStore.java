package org.relive;

import org.relive.Protocol.Commit;
import org.relive.Protocol.Entry;
import org.relive.Protocol.Head;
import org.relive.Protocol.Read;
import org.relive.Protocol.Results;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/** Local shared-disk publication prototype; every process can append complete commits. */
public final class IndexStore implements AutoCloseable {
    private static final byte[] MAGIC = "RELIVE\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final int SEAL = 0x434f4d54, LAYOUT_SEAL = 0x4c41594f;
    private static final long LOCK_TIMEOUT_NANOS = 5_000_000_000L;
    private static final Map<Path, ReentrantLock> LOCAL_LOCKS = new ConcurrentHashMap<>();
    private final ReentrantLock localLock;
    private final FileChannel lockFile;
    private final FileChannel journal;
    private final Path directory;
    // Level-1 files; the only files compaction rewrites, so this directory may be mounted on cheaper storage.
    private final Path levelOne;
    private final long memoryLimit;
    // Memtable rows by index, so a scan reads only its own key range. A flush replaces the map
    // instead of clearing it, so a reader keeps the snapshot its view named.
    private ConcurrentSkipListMap<String, ConcurrentSkipListMap<SortedRun.Row, SortedRun.Row>> memory = new ConcurrentSkipListMap<>();
    // Open immutable files with their block directories and Bloom filters, keyed by file name.
    // shortcut: one descriptor per live file, unbounded; add an LRU before inventories near the fd limit.
    private final Map<String, SortedRun> open = new ConcurrentHashMap<>();
    // Guarded by the publication lock; readers use the published view instead.
    private long memoryCharge, logicalBytes;
    // Full Memtables waiting for the background flusher, oldest first; replaced, never mutated.
    private volatile List<Frozen> frozen = List.of();
    private final Object frozenChanged = new Object();
    private volatile long stallNanos;
    private Layout layout = Layout.empty();
    private long position = MAGIC.length;
    private long revision;
    private String head = "";
    // Journal end including this handle's written but not yet applied frames; set after the view.
    private volatile long written = MAGIC.length;
    private volatile View view = new View(0, "", Layout.empty(), memory, List.of());
    private volatile boolean closed, failed;
    private final ConcurrentLinkedQueue<Pending> queue = new ConcurrentLinkedQueue<>();
    private final ExecutorService committer = daemon("relive-committer"), flusher = daemon("relive-flusher");
    /** Commits wait for the flusher once this many full Memtables are queued. */
    private static final int MAX_FROZEN = 2;
    /** Level-0 file counts that allow, and that force, a merge into level 1. */
    private static final int MIN_LEVEL_ZERO = 4, MAX_LEVEL_ZERO = 8;
    private final ExecutorService compactor = daemon("relive-compactor");
    private final java.util.concurrent.atomic.AtomicBoolean compacting = new java.util.concurrent.atomic.AtomicBoolean();
    /** Target length of a level-1 file; package-visible so tests can force splits. */
    long levelOneBytes = 256L << 20;

    private static ExecutorService daemon(String name) {
        return Executors.newSingleThreadExecutor(task -> {
            var thread = new Thread(task, name);
            thread.setDaemon(true);
            return thread;
        });
    }

    /** What a reader may use without the lock: rows above {@code revision} are not acknowledged yet. */
    private record View(long revision, String head, Layout layout,
                        ConcurrentSkipListMap<String, ConcurrentSkipListMap<SortedRun.Row, SortedRun.Row>> memory,
                        List<Frozen> frozen) {}
    /** A full Memtable covering journal bytes [start, end), with the checkpoint fields at its end. */
    private record Frozen(ConcurrentSkipListMap<String, ConcurrentSkipListMap<SortedRun.Row, SortedRun.Row>> rows,
                          long start, long end, long revision, String head, long logicalBytes, long charge) {}
    /** A queued commit, serialized by its submitter so the committer only validates and writes. */
    private record Pending(Commit commit, byte[] payload, long logical, CompletableFuture<Head> result) {
        Pending(Commit commit) {
            this(commit, Protocol.JSON.toJson(commit).getBytes(StandardCharsets.UTF_8), logicalBytes(commit), new CompletableFuture<>());
        }
    }

    /** UTF-8 bytes of each normalized mutation's JSON, as counted by {@code logicalMutationBytes}. */
    private static long logicalBytes(Commit commit) {
        long bytes = 0;
        for (var change : commit.changes()) bytes += Protocol.JSON.toJson(change).getBytes(StandardCharsets.UTF_8).length;
        return bytes;
    }
    /** One group's frames are written with one call; larger queues become several groups. */
    private static final int GROUP_BYTES = 64 << 20;
    /** A lock-free reader saw a file that a newer layout already replaced; it retries under the lock. */
    private static final class Stale extends IOException { Stale() { super("Stale view"); } }


    public static final class Conflict extends IllegalArgumentException {
        public Conflict(String message) { super(message); }
    }

    public IndexStore(Path directory) throws IOException {
        this(directory, Long.getLong("relive.memtable.bytes", 4L * 1024 * 1024));
    }

    public IndexStore(Path directory, long memoryLimit) throws IOException {
        if (memoryLimit < 1024 || memoryLimit > 256L * 1024 * 1024)
            throw new IllegalArgumentException("Memtable charge limit must be 1 KiB..256 MiB");
        this.memoryLimit = memoryLimit;
        Path path = directory.toAbsolutePath();
        this.directory = path;
        levelOne = path.resolve("level1");
        for (Path created : List.of(path, levelOne)) {
            try {
                Files.createDirectory(created);
            } catch (FileAlreadyExistsException exists) {
                if (!Files.isDirectory(created)) throw exists;
            }
            forceDirectory(created.getParent());
        }
        localLock = LOCAL_LOCKS.computeIfAbsent(path.toRealPath(), key -> new ReentrantLock());
        lockFile = FileChannel.open(path.resolve("publish.lock"), CREATE, WRITE);
        FileChannel opened = null;
        try (Guard ignored = lock()) {
            opened = FileChannel.open(path.resolve("journal"), CREATE, READ, WRITE);
            journal = opened;
            if (journal.size() == 0) {
                writeFully(ByteBuffer.wrap(MAGIC), 0);
                journal.force(true);
            }
            forceDirectory(path);
            if (!Arrays.equals(readFully(MAGIC.length, 0).array(), MAGIC))
                throw new IOException("Invalid Relive journal header");
            refresh(true);
        } catch (IOException | RuntimeException error) {
            committer.shutdownNow();
            flusher.shutdownNow();
            compactor.shutdownNow();
            localLock.lock();
            try {
                if (opened != null) opened.close();
                lockFile.close();
            } finally {
                localLock.unlock();
            }
            throw error;
        }
    }

    public Head head() throws IOException {
        try (Guard ignored = lock()) {
            refresh();
            return new Head(head);
        } catch (IOException error) {
            failed = true;
            throw error;
        }
    }

    public Head commit(Commit commit) throws IOException {
        return result(commitAsync(commit));
    }

    /**
     * Queues a commit behind those already queued by this handle. Its parent may be the previous
     * queued commit before that one is durable; if that one is rejected, this one is rejected too.
     * The result completes after the commit is durable and visible to new reads.
     */
    public CompletableFuture<Head> commitAsync(Commit commit) {
        if (commit == null) throw new IllegalArgumentException("Missing commit");
        if (commit.publication() != null)
            throw new IllegalArgumentException("Use staged publication for transaction receipts");
        var pending = new Pending(commit);
        queue.add(pending);
        try {
            committer.execute(this::drain);
        } catch (RejectedExecutionException stopped) {
            pending.result().completeExceptionally(new IOException("Store is closed or failed; reopen after diagnosis"));
        }
        return pending.result();
    }

    private static Head result(CompletableFuture<Head> pending) throws IOException {
        try {
            return pending.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for a commit; its outcome is unknown", interrupted);
        } catch (ExecutionException failure) {
            if (failure.getCause() instanceof IOException io) throw io;
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            throw new IOException(failure.getCause());
        }
    }

    /** Commits everything queued so far as one group: one journal write and one fsync. */
    private void drain() {
        if (queue.isEmpty()) return;
        long started = System.nanoTime();
        synchronized (frozenChanged) {
            try {
                while (frozen.size() >= MAX_FROZEN && !failed && !closed) frozenChanged.wait(100);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        stallNanos += System.nanoTime() - started;
        var batch = new ArrayList<Pending>();
        long bytes = 0;
        // This thread is the only consumer, so the peeked commit is the one polled.
        for (Pending next; (next = queue.peek()) != null && (batch.isEmpty() || bytes + next.payload().length <= GROUP_BYTES); ) {
            batch.add(queue.poll());
            bytes += next.payload().length;
        }
        if (batch.isEmpty()) return;
        try (Guard ignored = lock()) {
            refresh();
            append(batch);
        } catch (Throwable error) {
            if (!(error instanceof RuntimeException)) failed = true;
            for (var pending : batch) pending.result().completeExceptionally(error);
        }
        if (!queue.isEmpty()) committer.execute(this::drain);
    }

    private void append(List<Pending> batch) throws IOException {
        String parent = head;
        var accepted = new ArrayList<Pending>();
        var digests = new ArrayList<byte[]>();
        var repeated = new ArrayList<Pending>();
        var identities = new HashMap<String, byte[]>();
        var frames = new java.io.ByteArrayOutputStream();
        for (var pending : batch) {
            var commit = pending.commit();
            byte[] payload = pending.payload();
            if (payload.length > Protocol.MAX_BYTES) {
                pending.result().completeExceptionally(new IllegalArgumentException("Commit exceeds 8 MiB"));
                continue;
            }
            byte[] digest = digest(payload), earlier = identities.get(commit.id());
            if (earlier == null) {
                var previous = findCommit(view, commit.id());
                if (previous != null) earlier = HexFormat.of().parseHex(previous.value());
            }
            if (earlier != null) {
                if (MessageDigest.isEqual(earlier, digest)) repeated.add(pending);
                else pending.result().completeExceptionally(new Conflict("Commit identifier already names a different request"));
            } else if (!parent.equals(commit.parent())) {
                pending.result().completeExceptionally(new Conflict("Expected parent is not the current head"));
            } else {
                frames.writeBytes(frame(payload, digest, SEAL));
                accepted.add(pending);
                digests.add(digest);
                identities.put(commit.id(), digest);
                parent = commit.id();
            }
        }
        if (!accepted.isEmpty()) {
            byte[] bytes = frames.toByteArray();
            // Set first, so readers do not take this group's partial frames for another publisher's.
            written = position + bytes.length;
            writeFully(ByteBuffer.wrap(bytes), position);
            journal.force(true);
            for (int i = 0; i < accepted.size(); i++) apply(accepted.get(i).commit(), digests.get(i), accepted.get(i).logical());
            position += bytes.length;
            publishView();
        }
        // Repeats complete only now, so one that names a commit of this group is already durable.
        for (var pending : accepted) pending.result().complete(new Head(pending.commit().id()));
        for (var pending : repeated) pending.result().complete(new Head(pending.commit().id()));
        if (memoryCharge >= memoryLimit) {
            freeze();
            publishView();
            flusher.execute(this::flushFrozen);
        }
    }

    public interface ReadSink {
        void begin(String at, int scans);
        void entry(int scan, Entry entry);
    }

    public Results read(Read request) throws IOException {
        var lists = new ArrayList<List<Entry>>();
        String[] at = {null};
        long[] bytes = {4096};
        stream(request, new ReadSink() {
            public void begin(String point, int count) {
                at[0] = point;
                for (int i = 0; i < count; i++) lists.add(new ArrayList<>());
            }
            public void entry(int scan, Entry entry) {
                bytes[0] += entry.key().length() + entry.identity().length() + entry.value().length()
                        + 2L * entry.commit().length() + 128;
                if (bytes[0] > Protocol.MAX_BYTES)
                    throw new IllegalArgumentException("Read result exceeds 8 MiB; use streaming reads");
                lists.get(scan).add(entry);
            }
        });
        return new Results(at[0], lists.stream().map(List::copyOf).toList());
    }

    /** A read point and the pinned files of one read. */
    private record Selection(View view, String at, long visible, List<SortedRun> runs) {}

    public void stream(Read request, ReadSink sink) throws IOException {
        if (request == null) throw new IllegalArgumentException("Missing read");
        if (closed || failed) throw new IOException("Store is closed or failed; reopen after diagnosis");
        Selection selection = null;
        try {
            TransactionState transaction = null;
            // No bytes beyond this handle's own frames means no other publisher has published since the view.
            // Others append only under the file lock, which this handle holds while its frames exceed the view.
            if (request.transaction() == null && journal.size() <= written) {
                try {
                    selection = select(view, request);
                } catch (Stale | NoSuchFileException stale) {
                    selection = null;
                }
            }
            if (selection == null) try (Guard ignored = lock()) {
                refresh();
                if (request.transaction() != null) {
                    transaction = transaction(request.transaction(), request.operation());
                    if (request.own() != null && !request.own().equals(transaction.id()))
                        throw new Conflict("Local read does not name this transaction's writes");
                    new TransactionState(transaction.transaction(), request.operation(), transaction.id(),
                            transaction.changes()).save(transactionPath(request.transaction()));
                }
                selection = select(view, request);
            }
            final String at = selection.at();
            final long visible = selection.visible();
            final List<SortedRun> runs = selection.runs();
            final var memories = new ArrayList<ConcurrentSkipListMap<String, ConcurrentSkipListMap<SortedRun.Row, SortedRun.Row>>>();
            memories.add(selection.view().memory());
            for (var full : selection.view().frozen()) memories.add(full.rows());
            var overlay = new TreeMap<SortedRun.Row, SortedRun.Row>();
            if (request.own() != null) for (var change : transaction.changes()) {
                var row = new SortedRun.Row(change.index(), change.key(), Long.MAX_VALUE,
                        change.value(), request.own(), change.identity());
                overlay.put(row, row);
            }
            boolean combine = !request.combine().equals("none");
            sink.begin(at, combine ? 1 : request.scans().size());
            // shortcut: groups retain identities/results in memory; add spilling before unbounded group workloads.
            var combined = new LinkedHashMap<String, Entry>();
            for (int number = 0; number < request.scans().size(); number++) {
                final int n = number;
                var members = new HashSet<String>();
                scan(request.scans().get(n), visible, memories, runs, overlay, entry -> {
                    if (!combine) sink.entry(n, entry);
                    else if (n == 0 || request.combine().equals("union")) combined.putIfAbsent(entry.identity(), entry);
                    else members.add(entry.identity());
                });
                if (combine && n > 0 && request.combine().equals("intersection"))
                    combined.keySet().retainAll(members);
            }
            if (combine) for (Entry entry : combined.values()) sink.entry(0, entry);
        } catch (IOException error) {
            failed = true;
            throw error;
        } finally {
            if (selection != null) SortedRun.closeAll(selection.runs());
        }
    }

    private Selection select(View view, Read request) throws IOException {
        String at = request.at() == null ? view.head() : request.at();
        long visible;
        if (at.equals(view.head())) visible = view.revision();
        else if (at.isEmpty()) visible = 0;
        else {
            var selected = findCommit(view, at);
            if (selected == null) throw new Conflict("Unknown read commit");
            visible = selected.revision();
        }
        var runs = new ArrayList<SortedRun>();
        try {
            for (var descriptor : view.layout().runs()) {
                boolean needed = false;
                for (var scan : request.scans())
                    if (descriptor.overlaps(scan) && (!point(scan) || open(descriptor).mayContain(scan.index(), scan.lower()))) {
                        needed = true;
                        break;
                    }
                // Pinning only files a scan may read keeps point lookups off the reference counts of the rest.
                if (needed) runs.add(pin(descriptor));
            }
        } catch (IOException | RuntimeException error) {
            try { SortedRun.closeAll(runs); } catch (IOException close) { error.addSuppressed(close); }
            throw error;
        }
        return new Selection(view, at, visible, runs);
    }

    private void scan(Protocol.Scan scan, long visible,
                      List<ConcurrentSkipListMap<String, ConcurrentSkipListMap<SortedRun.Row, SortedRun.Row>>> memories,
                      List<SortedRun> runs,
                      TreeMap<SortedRun.Row, SortedRun.Row> overlay,
                      java.util.function.Consumer<Entry> sink) throws IOException {
        var sources = new ArrayList<SortedRun.Rows>();
        var maps = new ArrayList<NavigableMap<SortedRun.Row, SortedRun.Row>>();
        for (var memory : memories) maps.add(memory.getOrDefault(scan.index(), new ConcurrentSkipListMap<>()));
        maps.add(overlay);
        for (var map : maps) {
            NavigableMap<SortedRun.Row, SortedRun.Row> range = map;
            var from = scan.lower() == null ? null : bound(scan.index(), scan.lower(), !scan.lowerInclusive());
            var to = scan.upper() == null ? null : bound(scan.index(), scan.upper(), scan.upperInclusive());
            if (from != null && to != null && from.compareTo(to) > 0) range = new TreeMap<>();
            else if (from != null && to != null) range = range.subMap(from, scan.lowerInclusive(), to, scan.upperInclusive());
            else if (from != null) range = range.tailMap(from, scan.lowerInclusive());
            else if (to != null) range = range.headMap(to, scan.upperInclusive());
            var values = scan.descending() ? range.descendingMap().values() : range.values();
            var iterator = values.stream().filter(row -> row.index().equals(scan.index())).iterator();
            sources.add(() -> iterator.hasNext() ? iterator.next() : null);
        }
        boolean point = point(scan);
        long hash = point ? SortedRun.hash(scan.index(), scan.lower()) : 0;
        for (var run : runs) if (!point || run.mayContain(hash))
            sources.add(run.rows(scan.index(), scan.lower(), scan.lowerInclusive(),
                    scan.upper(), scan.upperInclusive(), scan.descending()));
        var merged = SortedRun.merge(sources, scan.descending());
        SortedRun.Row pending = merged.next();
        long found = 0;
        while (pending != null && (scan.limit() == 0 || found < scan.limit())) {
            String key = pending.key();
            SortedRun.Row best = null;
            do {
                if ((pending.revision() <= visible || pending.revision() == Long.MAX_VALUE)
                        && (best == null || pending.revision() > best.revision())) best = pending;
                pending = merged.next();
            } while (pending != null && pending.key().equals(key));
            if (best != null && best.value() != null) {
                sink.accept(new Entry(best.key(), best.value(), best.commit(), best.identity()));
                found++;
            }
        }
    }

    /** A probe row ordered before every version of {@code key}, or after every version when {@code after}. */
    private static SortedRun.Row bound(String index, String key, boolean after) {
        long revision = after ? Long.MAX_VALUE : 1;
        return index.isEmpty() ? new SortedRun.Row(index, key, revision, "0".repeat(64), "bound")
                : new SortedRun.Row(index, key, revision, null, "bound");
    }

    private static boolean point(Protocol.Scan scan) {
        return scan.lower() != null && scan.lower().equals(scan.upper()) && scan.lowerInclusive() && scan.upperInclusive();
    }

    /** The shared open file, unpinned: enough for its Bloom filter, which stays valid after it closes. */
    private SortedRun open(Layout.Run descriptor) throws IOException {
        var run = open.get(descriptor.name());
        if (run == null) {
            var opened = new SortedRun(file(descriptor));
            run = open.putIfAbsent(descriptor.name(), opened);
            if (run != null) opened.close();
            else run = opened;
        }
        return run;
    }

    /** The shared open file, retained for one reader; release it with {@code close}. */
    private SortedRun pin(Layout.Run descriptor) throws IOException {
        var run = open(descriptor);
        if (!run.retain()) throw new Stale(); // a newer layout dropped this file after the view was taken
        return run;
    }

    /** Level-0 files stay with the journal; level-1 files are only created, read and deleted, never renamed. */
    private Path file(Layout.Run descriptor) {
        return (descriptor.level() == 0 ? directory : levelOne).resolve(descriptor.name());
    }

    private List<SortedRun> openRuns(List<Layout.Run> descriptors) throws IOException {
        List<SortedRun> result = new ArrayList<>();
        try {
            for (var descriptor : descriptors) result.add(new SortedRun(file(descriptor)));
            return result;
        } catch (IOException error) {
            try { SortedRun.closeAll(result); } catch (IOException close) { error.addSuppressed(close); }
            throw error;
        }
    }

    private SortedRun.Row findCommit(View view, String id) throws IOException {
        var row = metadata(view, "c" + Layout.hash(id));
        if (row != null && !row.commit().equals(id)) throw new IOException("Commit identity mismatch");
        return row;
    }

    private SortedRun.Row metadata(View view, String id) throws IOException {
        var memories = new ArrayList<ConcurrentSkipListMap<String, ConcurrentSkipListMap<SortedRun.Row, SortedRun.Row>>>();
        memories.add(view.memory());
        for (var full : view.frozen()) memories.add(full.rows());
        for (var memory : memories) {
            var identities = memory.get("");
            var found = identities == null ? null : identities.ceilingKey(bound("", id, false));
            if (found != null && found.key().equals(id)) return found;
        }
        long hash = SortedRun.hash("", id);
        for (var descriptor : view.layout().runs()) {
            if (!descriptor.overlaps("", id, true, id, true) || !open(descriptor).mayContain(hash)) continue;
            try (var run = pin(descriptor)) {
                var row = run.rows("", id, true, id, true, false).next();
                if (row != null) return row;
            }
        }
        return null;
    }

    private Path transactionPath(String transaction) {
        return directory.resolve("transactions").resolve("t" + Layout.hash(transaction) + ".json");
    }

    private TransactionState transaction(String transaction, long operation) throws IOException {
        if (metadata(view, "t" + Layout.hash(transaction)) != null) throw new Conflict("Transaction already published");
        Path path = transactionPath(transaction);
        if (!Files.exists(path.getParent())) {
            Files.createDirectory(path.getParent());
            Layout.force(directory);
        }
        if (!Files.exists(path)) try (var paths = Files.list(path.getParent())) {
            if (paths.limit(1024).count() >= 1024) throw new Conflict("Too many active transactions; drain or diagnose");
        }
        var state = TransactionState.load(path, transaction);
        if (operation != state.operation() + 1) throw new Conflict("Transaction operation is not next");
        return state;
    }

    public Protocol.Progress stage(Protocol.Stage request) throws IOException {
        if (request == null) throw new IllegalArgumentException("Missing stage request");
        try (Guard ignored = lock()) {
            refresh();
            var previous = transaction(request.transaction(), request.operation());
            if (previous.id() != null && !previous.id().equals(request.id()))
                throw new Conflict("Transaction changes its commit identity");
            var changes = new ArrayList<>(previous.changes());
            changes.addAll(request.changes());
            new TransactionState(request.transaction(), request.operation(), request.id(), changes)
                    .save(transactionPath(request.transaction()));
            return new Protocol.Progress(request.operation());
        } catch (IOException error) { failed = true; throw error; }
    }

    public Head publish(Protocol.Publication request) throws IOException {
        if (request == null) throw new IllegalArgumentException("Missing publication");
        try (Guard ignored = lock()) {
            refresh();
            var receipt = metadata(view, "t" + Layout.hash(request.transaction()));
            Path path = transactionPath(request.transaction());
            if (receipt != null) {
                if (!receipt.value().equals(Layout.hash(Protocol.JSON.toJson(request))))
                    throw new Conflict("Transaction already has a different publication");
            } else {
                var state = TransactionState.load(path, request.transaction());
                if (state.operation() != request.operations() || state.changes().size() != request.writes()
                        || state.id() != null && !state.id().equals(request.id()))
                    throw new Conflict("Publication does not cover the staged transaction");
                // A direct commit with this id must not be mistaken for this transaction's receipt.
                if (findCommit(view, request.id()) != null) throw new Conflict("Commit identifier already exists");
                var pending = new Pending(new Commit(request.parent(), request.id(), state.changes(), request));
                append(List.of(pending));
                result(pending.result());
            }
            if (Files.deleteIfExists(path)) Layout.force(path.getParent());
            return new Head(request.id());
        } catch (IOException error) { failed = true; throw error; }
    }

    public Protocol.BindingInfo bind(Protocol.Binding request) throws IOException {
        if (request == null) throw new IllegalArgumentException("Missing binding");
        try (Guard ignored = lock()) {
            refresh();
            Path path = directory.resolve("binding");
            Protocol.BindingInfo stored;
            if (Files.exists(path)) {
                if (Files.size(path) > 16384) throw new IOException("Invalid binding size");
                try {
                    stored = Protocol.JSON.fromJson(Files.readString(path), Protocol.BindingInfo.class);
                    if (stored == null || stored.identity() == null || stored.binding() == null)
                        throw new IOException("Invalid stored binding");
                    UUID.fromString(stored.identity());
                } catch (RuntimeException error) { throw new IOException("Invalid stored binding", error); }
                if (!request.binding().equals(stored.binding())) throw new Conflict("Index belongs to another input");
            } else {
                if (!head.isEmpty()) throw new Conflict("Cannot bind an existing unbound index");
                stored = new Protocol.BindingInfo(request.binding(), UUID.randomUUID().toString(), "", directory.toString());
                TransactionState.replace(path, Protocol.JSON.toJson(stored).getBytes(StandardCharsets.UTF_8));
            }
            return new Protocol.BindingInfo(stored.binding(), stored.identity(), head, directory.toString());
        } catch (IOException error) { failed = true; throw error; }
    }

    public record Stats(String head, long logicalMutationBytes, long walBytes, long flushBytes,
                        long compactionReadBytes, long compactionWriteBytes, long memoryCharge,
                        long memoryLimit, int runCount, int activeTransactions, long stagedBytes,
                        int frozenMemtables, long writeStallMillis) {}

    public Stats stats() throws IOException {
        try (Guard ignored = lock()) {
            refresh();
            return statistics();
        } catch (IOException error) { failed = true; throw error; }
    }

    private Stats statistics() throws IOException {
        int active = 0;
        long staged = 0;
        if (Files.isDirectory(directory.resolve("transactions")))
            try (var paths = Files.list(directory.resolve("transactions"))) {
                for (Path path : paths.toList()) {
                    staged += Files.size(path);
                    if (path.getFileName().toString().endsWith(".json")) active++;
                }
            }
        long charge = memoryCharge + frozen.stream().mapToLong(Frozen::charge).sum();
        return new Stats(head, logicalBytes, journal.size() - MAGIC.length, layout.flushBytes(),
                layout.compactionReadBytes(), layout.compactionWriteBytes(), charge, memoryLimit,
                layout.runs().size(), active, staged, frozen.size(), stallNanos / 1_000_000);
    }

    public Stats flush() throws IOException {
        try (Guard ignored = lock()) {
            refresh(true);
            flushMemory();
            publishView();
            return statistics();
        } catch (IOException error) { failed = true; throw error; }
    }

    /** Under the lock: freezes the Memtable and flushes every frozen one in order, without the flusher. */
    private void flushMemory() throws IOException {
        freeze();
        while (!frozen.isEmpty()) {
            var oldest = frozen.get(0);
            String name = UUID.randomUUID() + ".sst";
            install(oldest, name, write(oldest, name));
        }
        synchronized (frozenChanged) { frozenChanged.notifyAll(); }
    }

    /** Under the lock: queues the current Memtable for flushing and starts an empty one. */
    private void freeze() {
        if (memory.isEmpty()) return;
        var queued = frozen;
        long start = queued.isEmpty() ? layout.position() : queued.get(queued.size() - 1).end();
        var all = new ArrayList<>(queued);
        all.add(new Frozen(memory, start, position, revision, head, logicalBytes, memoryCharge));
        frozen = List.copyOf(all);
        memory = new ConcurrentSkipListMap<>();
        memoryCharge = 0;
    }

    private Layout.Run write(Frozen full, String name) throws IOException {
        var rows = full.rows().values().stream().flatMap(map -> map.values().stream()).iterator();
        long count = full.rows().values().stream().mapToLong(Map::size).sum();
        var descriptor = SortedRun.write(directory.resolve(name), () -> rows.hasNext() ? rows.next() : null, count, 0);
        Layout.force(directory);
        return descriptor;
    }

    /** Under the lock: publishes the file if {@code full} is still the next checkpoint, else discards it. */
    private void install(Frozen full, String name, Layout.Run descriptor) throws IOException {
        if (frozen.isEmpty() || frozen.get(0) != full || layout.position() != full.start()) {
            // A flush by this or another handle already covers these journal bytes.
            Files.delete(directory.resolve(name));
            Layout.force(directory);
            return;
        }
        long written = Files.size(directory.resolve(name));
        var runs = new ArrayList<>(layout.runs());
        runs.add(descriptor);
        var replacement = new Layout(layout.generation() + 1, full.end(), full.revision(), full.head(), full.logicalBytes(),
                layout.flushBytes() + written, layout.compactionReadBytes(), layout.compactionWriteBytes(), runs);
        frozen = List.copyOf(frozen.subList(1, frozen.size()));
        publish(replacement);
        maybeCompact();
    }

    /** Background: writes the oldest frozen Memtable outside the lock, then installs it under the lock. */
    private void flushFrozen() {
        try {
            while (!closed && !failed && !frozen.isEmpty()) {
                var oldest = frozen.get(0);
                String name = UUID.randomUUID() + ".sst";
                var descriptor = write(oldest, name);
                try (Guard ignored = lock()) {
                    refresh(true);
                    install(oldest, name, descriptor);
                    publishView();
                }
                synchronized (frozenChanged) { frozenChanged.notifyAll(); }
            }
        } catch (IOException | RuntimeException error) {
            failed = true;
            synchronized (frozenChanged) { frozenChanged.notifyAll(); }
        }
    }

    /** Merges every sorted file into level 1. */
    public Stats compact() throws IOException {
        try {
            return compact(true);
        } catch (IOException error) { failed = true; throw error; }
    }

    /**
     * Picks level-0 files and the level-1 files their range overlaps once level 0 has at least
     * {@link #MIN_LEVEL_ZERO} files holding a tenth of level 1's bytes, or {@link #MAX_LEVEL_ZERO}
     * files; this bounds rewriting to about ten times the flushed bytes while keeping level 0 short.
     */
    private List<Layout.Run> pick() {
        var levelZero = layout.runs().stream().filter(run -> run.level() == 0).toList();
        long zeroBytes = levelZero.stream().mapToLong(Layout.Run::bytes).sum();
        long oneBytes = layout.runs().stream().filter(run -> run.level() == 1).mapToLong(Layout.Run::bytes).sum();
        if (levelZero.size() < MIN_LEVEL_ZERO || zeroBytes * 10 < oneBytes && levelZero.size() < MAX_LEVEL_ZERO) return List.of();
        var selected = new ArrayList<>(levelZero);
        for (var run : layout.runs())
            if (run.level() == 1 && levelZero.stream().anyMatch(zero -> !zero.disjoint(run))) selected.add(run);
        return selected;
    }

    /** Under the lock: starts one background compaction if the policy has work and none is running. */
    private void maybeCompact() {
        if (!pick().isEmpty() && compacting.compareAndSet(false, true)) compactor.execute(() -> {
            try {
                do compact(false); while (!closed && !failed && !pickNow().isEmpty());
            } catch (IOException | RuntimeException error) {
                failed = true;
            } finally {
                compacting.set(false);
            }
        });
    }

    private List<Layout.Run> pickNow() throws IOException {
        try (Guard ignored = lock()) {
            refresh(true);
            return pick();
        }
    }

    /**
     * Merges immutable inputs into level-1 files outside the publication lock, then validates their
     * membership before replacing them. A compaction that lost a race discards its outputs.
     */
    private Stats compact(boolean all) throws IOException {
        final List<Layout.Run> selected;
        final List<SortedRun> inputs;
        try (Guard ignored = lock()) {
            refresh(true);
            selected = all ? layout.runs() : pick();
            if (all ? selected.size() < 2 : selected.isEmpty()) return statistics();
        }
        // Opening reads every input's directory and filter, so it happens outside the lock; an input that
        // is already gone was replaced by another compaction, which this one would lose to anyway.
        try {
            inputs = openRuns(selected);
        } catch (NoSuchFileException replaced) {
            return stats();
        }
        var outputs = new ArrayList<Layout.Run>();
        long output = 0, read;
        try {
            var sources = new ArrayList<SortedRun.Rows>();
            for (var input : inputs) sources.add(input.all());
            long rows = inputs.stream().mapToLong(SortedRun::count).sum();
            long bytes = selected.stream().mapToLong(Layout.Run::bytes).sum();
            // Each output's Bloom filter is sized for the rows that fit in one split file.
            long keys = Math.min(rows, levelOneBytes / Math.max(1, bytes / Math.max(1, rows)) + 1);
            var merged = new SortedRun.Peeking(SortedRun.merge(sources, false));
            while (merged.peek() != null) {
                var descriptor = SortedRun.write(levelOne.resolve(UUID.randomUUID() + ".sst"), merged, keys, 1, levelOneBytes);
                outputs.add(descriptor);
                output += descriptor.bytes();
            }
            read = inputs.stream().mapToLong(input -> input.bytesRead.get()).sum();
        } catch (IOException | RuntimeException error) {
            for (var descriptor : outputs) Files.deleteIfExists(file(descriptor));
            throw error;
        } finally { SortedRun.closeAll(inputs); }
        final List<Layout.Run> obsolete;
        final Stats stats;
        try (Guard ignored = lock()) {
            refresh(true);
            boolean accepted = layout.runs().containsAll(selected);
            var runs = new ArrayList<>(layout.runs());
            if (accepted) {
                runs.removeAll(selected);
                runs.addAll(outputs);
            }
            Layout.force(levelOne);
            var replacement = new Layout(layout.generation() + 1, layout.position(), layout.revision(),
                    layout.head(), layout.logicalBytes(), layout.flushBytes(), layout.compactionReadBytes() + read,
                    layout.compactionWriteBytes() + output, runs);
            publish(replacement);
            publishView();
            obsolete = accepted ? selected : outputs;
            stats = statistics();
        }
        // The published layout no longer names these files, so unlinking them needs no lock; readers
        // that pinned one keep its open descriptor, and a reader that finds one gone retries.
        for (var old : obsolete) Files.delete(file(old));
        Layout.force(directory);
        Layout.force(levelOne);
        return stats;
    }

    private void refresh() throws IOException { refresh(false); }

    /**
     * Every layout replacement appends a journal marker, so an unchanged journal length means
     * nothing to replay or reload. Writers of a new layout pass {@code reload} to start from the
     * saved layout even if its marker was lost in a crash.
     */
    private void refresh(boolean reload) throws IOException {
        if (!reload && journal.size() == position) return;
        Layout current = Layout.load(directory);
        if (current.generation() < layout.generation() || current.position() < layout.position())
            throw new IOException("Layout moved backwards");
        if (current.position() != layout.position()) {
            memory = new ConcurrentSkipListMap<>();
            memoryCharge = 0;
            frozen = List.of(); // another handle's flush covers or will replay these bytes
            position = current.position();
            revision = current.revision();
            head = current.head();
            logicalBytes = current.logicalBytes();
        }
        setLayout(current);
        long end = journal.size();
        if (end < position) throw new IOException("Journal shrank; refusing stale cached state");
        if (end > position) journal.force(true);
        while (position < end) {
            if (end - position < 4) throw new EOFException("Incomplete journal frame; retained for diagnosis");
            int length = readFully(4, position).getInt();
            if (length < 1 || length > Protocol.MAX_BYTES) throw new IOException("Invalid journal frame length");
            long frameSize = 4L + length + 32 + 4;
            if (end - position < frameSize)
                throw new EOFException("Incomplete journal frame; retained for diagnosis");
            ByteBuffer body = readFully(length + 32 + 4, position + 4);
            byte[] payload = new byte[length];
            byte[] checksum = new byte[32];
            body.get(payload).get(checksum);
            int seal = body.getInt();
            if (seal != SEAL && seal != LAYOUT_SEAL || !MessageDigest.isEqual(checksum, digest(payload)))
                throw new IOException("Journal integrity check failed");
            if (seal == LAYOUT_SEAL) {
                long generation;
                try { generation = Long.parseLong(new String(payload, StandardCharsets.US_ASCII)); }
                catch (NumberFormatException invalid) { throw new IOException("Invalid layout marker", invalid); }
                if (generation < 1 || generation > layout.generation()) throw new IOException("Layout marker is ahead of the layout");
                position += frameSize;
                continue;
            }
            Commit commit;
            try {
                commit = Protocol.JSON.fromJson(new String(payload, StandardCharsets.UTF_8), Commit.class);
                // The replay state stays private until it is complete, so readers never see it regress.
                if (commit == null || !head.equals(commit.parent())
                        || findCommit(new View(revision, head, layout, memory, frozen), commit.id()) != null)
                    throw new IllegalArgumentException("Invalid commit chain");
            } catch (RuntimeException invalid) {
                throw new IOException("Invalid journal commit", invalid);
            }
            apply(commit, checksum, logicalBytes(commit));
            position += frameSize;
            if (memoryCharge >= memoryLimit) flushMemory();
        }
        publishView();
    }

    /** Publishes the view before {@code written}, which lock-free readers check first. */
    private void publishView() {
        view = new View(revision, head, layout, memory, frozen);
        written = position;
    }

    /** Drops this handle's reference to files the new layout no longer names. */
    private void setLayout(Layout replacement) throws IOException {
        layout = replacement;
        var live = new HashSet<String>();
        for (var run : layout.runs()) live.add(run.name());
        for (var iterator = open.entrySet().iterator(); iterator.hasNext(); ) {
            var entry = iterator.next();
            if (!live.contains(entry.getKey())) { iterator.remove(); entry.getValue().close(); }
        }
    }

    private void apply(Commit commit, byte[] digest, long logical) {
        long next = ++revision;
        for (var change : commit.changes())
            putMemory(new SortedRun.Row(change.index(), change.key(), next, change.value(), commit.id(), change.identity()));
        logicalBytes += logical;
        head = commit.id();
        putMemory(new SortedRun.Row("", "c" + Layout.hash(head), next, HexFormat.of().formatHex(digest), head));
        if (commit.publication() != null)
            putMemory(new SortedRun.Row("", "t" + Layout.hash(commit.publication().transaction()), next,
                    Layout.hash(Protocol.JSON.toJson(commit.publication())), head));
    }

    private void putMemory(SortedRun.Row row) {
        SortedRun.Row previous = memory.computeIfAbsent(row.index(), index -> new ConcurrentSkipListMap<>()).put(row, row);
        if (previous != null) memoryCharge -= previous.charge();
        memoryCharge += row.charge();
    }

    private Guard lock() throws IOException {
        if (closed || failed) throw new IOException("Store is closed or failed; reopen after diagnosis");
        long deadline = System.nanoTime() + LOCK_TIMEOUT_NANOS;
        try {
            if (!localLock.tryLock(LOCK_TIMEOUT_NANOS, TimeUnit.NANOSECONDS))
                throw new IOException("Publication lock timed out after 5 seconds");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted waiting for local publication lock", interrupted);
        }
        try {
            // A holder of the lock may have failed or closed this handle while we waited.
            if (closed || failed) throw new IOException("Store is closed or failed; reopen after diagnosis");
            return acquireFileLock(deadline);
        } catch (IOException | RuntimeException error) {
            localLock.unlock();
            throw error;
        }
    }

    private Guard acquireFileLock(long deadline) throws IOException {
        while (true) {
            try {
                FileLock acquired = lockFile.tryLock();
                if (acquired != null) return new Guard(acquired);
            } catch (OverlappingFileLockException busyInThisJvm) {
                // Another store handle in this JVM holds the same OS lock.
            }
            if (System.nanoTime() >= deadline) throw new IOException("Publication lock timed out after 5 seconds");
            try {
                Thread.sleep(2);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted waiting for publication lock", interrupted);
            }
        }
    }

    private final class Guard implements AutoCloseable {
        private final FileLock fileLock;
        Guard(FileLock fileLock) { this.fileLock = fileLock; }
        @Override public void close() throws IOException {
            try {
                fileLock.release();
            } finally {
                localLock.unlock();
            }
        }
    }

    private ByteBuffer readFully(int size, long offset) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(size);
        while (bytes.hasRemaining()) {
            int read = journal.read(bytes, offset + bytes.position());
            if (read < 0) throw new EOFException("Incomplete journal");
        }
        return bytes.flip();
    }

    private static byte[] frame(byte[] payload, byte[] digest, int seal) {
        return ByteBuffer.allocate(4 + payload.length + 32 + 4).putInt(payload.length).put(payload).put(digest).putInt(seal).array();
    }

    /** Saves a replacement layout, then appends a marker so other handles notice it from the journal length alone. */
    private void publish(Layout replacement) throws IOException {
        replacement.save(directory);
        byte[] payload = Long.toString(replacement.generation()).getBytes(StandardCharsets.US_ASCII);
        byte[] marker = frame(payload, digest(payload), LAYOUT_SEAL);
        long at = journal.size();
        writeFully(ByteBuffer.wrap(marker), at);
        journal.force(true);
        // This handle wrote the marker itself; stepping over it keeps lock-free readers on their fast path.
        if (position == at) position = at + marker.length;
        setLayout(replacement);
    }

    private void writeFully(ByteBuffer bytes, long offset) throws IOException {
        while (bytes.hasRemaining()) journal.write(bytes, offset + bytes.position());
    }

    private static byte[] digest(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, READ)) {
            channel.force(true);
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
        synchronized (frozenChanged) { frozenChanged.notifyAll(); }
        for (var executor : List.of(committer, flusher, compactor)) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(LOCK_TIMEOUT_NANOS, TimeUnit.NANOSECONDS)) executor.shutdownNow();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        for (Pending pending; (pending = queue.poll()) != null; )
            pending.result().completeExceptionally(new IOException("Store is closed; commit outcome is unknown"));
        // Closing another descriptor can release this JVM's POSIX locks on the same file.
        localLock.lock();
        try {
            try {
                journal.close();
            } finally {
                try { SortedRun.closeAll(List.copyOf(open.values())); } finally { lockFile.close(); }
            }
        } finally {
            localLock.unlock();
        }
    }
}
