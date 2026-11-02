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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/** Local shared-disk publication prototype; every process can append complete commits. */
public final class IndexStore implements AutoCloseable {
    private static final byte[] MAGIC = "RELIVE\r\n".getBytes(StandardCharsets.US_ASCII);
    private static final int SEAL = 0x434f4d54;
    private static final long LOCK_TIMEOUT_NANOS = 5_000_000_000L;
    private static final Map<Path, ReentrantLock> LOCAL_LOCKS = new ConcurrentHashMap<>();
    private final ReentrantLock localLock;
    private final FileChannel lockFile;
    private final FileChannel journal;
    private final Path directory;
    private final long memoryLimit;
    private final TreeMap<SortedRun.Row, SortedRun.Row> memory = new TreeMap<>();
    private long memoryCharge, logicalBytes;
    private Layout layout = Layout.empty();
    private long position = MAGIC.length;
    private long revision;
    private String head = "";
    private boolean closed;
    private boolean failed;


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
        try {
            Files.createDirectory(path);
        } catch (FileAlreadyExistsException exists) {
            if (!Files.isDirectory(path)) throw exists;
        }
        forceDirectory(path.getParent());
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
            refresh();
        } catch (IOException | RuntimeException error) {
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

    public synchronized Head head() throws IOException {
        try (Guard ignored = lock()) {
            refresh();
            return new Head(head);
        } catch (IOException error) {
            failed = true;
            throw error;
        }
    }

    public synchronized Head commit(Commit commit) throws IOException {
        if (commit != null && commit.publication() != null)
            throw new IllegalArgumentException("Use staged publication for transaction receipts");
        try (Guard ignored = lock()) {
            refresh();
            return append(commit);
        } catch (IOException error) { failed = true; throw error; }
    }

    private Head append(Commit commit) throws IOException {
        if (commit == null) throw new IllegalArgumentException("Missing commit");
        byte[] payload = Protocol.JSON.toJson(commit).getBytes(StandardCharsets.UTF_8);
        if (payload.length > Protocol.MAX_BYTES) throw new IllegalArgumentException("Commit exceeds 8 MiB");
        byte[] digest = digest(payload);
        {
            SortedRun.Row previous = findCommit(commit.id());
            if (previous != null) {
                if (!MessageDigest.isEqual(HexFormat.of().parseHex(previous.value()), digest))
                    throw new Conflict("Commit identifier already names a different request");
                return new Head(commit.id());
            }
            if (!head.equals(commit.parent())) throw new Conflict("Expected parent is not the current head");
            ByteBuffer frame = ByteBuffer.allocate(4 + payload.length + 32 + 4);
            frame.putInt(payload.length).put(payload).put(digest).putInt(SEAL).flip();
            int length = frame.remaining();
            writeFully(frame, position);
            journal.force(true);
            apply(commit, digest);
            position += length;
            if (memoryCharge >= memoryLimit) flushMemory();
            return new Head(commit.id());
        }
    }

    public interface ReadSink {
        void begin(String at, int scans);
        void entry(int scan, Entry entry);
    }

    public synchronized Results read(Read request) throws IOException {
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

    public synchronized void stream(Read request, ReadSink sink) throws IOException {
        if (request == null) throw new IllegalArgumentException("Missing read");
        List<SortedRun> runs = new ArrayList<>();
        try {
            final String at;
            final long visible;
            TransactionState transaction = null;
            try (Guard ignored = lock()) {
                refresh();
                at = request.at() == null ? head : request.at();
                if (at.equals(head)) visible = revision;
                else if (at.isEmpty()) visible = 0;
                else {
                    var selected = findCommit(at);
                    if (selected == null) throw new Conflict("Unknown read commit");
                    visible = selected.revision();
                }
                if (request.transaction() != null) {
                    transaction = transaction(request.transaction(), request.operation());
                    if (request.own() != null && !request.own().equals(transaction.id()))
                        throw new Conflict("Local read does not name this transaction's writes");
                    new TransactionState(transaction.transaction(), request.operation(), transaction.id(),
                            transaction.changes()).save(transactionPath(request.transaction()));
                }
                runs = openRuns(layout.runs().stream()
                        .filter(run -> request.scans().stream().anyMatch(run::overlaps)).toList());
            }
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
                scan(request.scans().get(n), visible, runs, overlay, entry -> {
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
        } finally { SortedRun.closeAll(runs); }
    }

    private void scan(Protocol.Scan scan, long visible, List<SortedRun> runs,
                      TreeMap<SortedRun.Row, SortedRun.Row> overlay,
                      java.util.function.Consumer<Entry> sink) throws IOException {
        var sources = new ArrayList<SortedRun.Rows>();
        for (var map : List.of(memory, overlay)) {
            var values = scan.descending() ? map.descendingMap().values() : map.values();
            var iterator = values.stream().filter(row -> row.index().equals(scan.index()))
                    .filter(row -> within(row.key(), scan)).iterator();
            sources.add(() -> iterator.hasNext() ? iterator.next() : null);
        }
        for (var run : runs) sources.add(run.rows(scan.index(), scan.lower(), scan.lowerInclusive(),
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

    private static boolean within(String key, Protocol.Scan scan) {
        int lower = scan.lower() == null ? 1 : key.compareTo(scan.lower());
        int upper = scan.upper() == null ? -1 : key.compareTo(scan.upper());
        return (lower > 0 || lower == 0 && scan.lowerInclusive())
                && (upper < 0 || upper == 0 && scan.upperInclusive());
    }

    private List<SortedRun> openRuns(List<Layout.Run> descriptors) throws IOException {
        List<SortedRun> result = new ArrayList<>();
        try {
            for (var descriptor : descriptors) result.add(new SortedRun(directory.resolve(descriptor.name())));
            return result;
        } catch (IOException error) {
            try { SortedRun.closeAll(result); } catch (IOException close) { error.addSuppressed(close); }
            throw error;
        }
    }

    private SortedRun.Row findCommit(String id) throws IOException {
        var row = metadata("c" + Layout.hash(id));
        if (row != null && !row.commit().equals(id)) throw new IOException("Commit identity mismatch");
        return row;
    }

    private SortedRun.Row metadata(String id) throws IOException {
        for (var row : memory.values()) {
            if (!row.index().isEmpty()) break;
            if (row.key().equals(id)) return row;
        }
        for (var descriptor : layout.runs()) {
            if (!descriptor.overlaps("", id, true, id, true)) continue;
            try (var run = new SortedRun(directory.resolve(descriptor.name()))) {
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
        if (metadata("t" + Layout.hash(transaction)) != null) throw new Conflict("Transaction already published");
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

    public synchronized Protocol.Progress stage(Protocol.Stage request) throws IOException {
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

    public synchronized Head publish(Protocol.Publication request) throws IOException {
        if (request == null) throw new IllegalArgumentException("Missing publication");
        try (Guard ignored = lock()) {
            refresh();
            var receipt = metadata("t" + Layout.hash(request.transaction()));
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
                if (findCommit(request.id()) != null) throw new Conflict("Commit identifier already exists");
                append(new Commit(request.parent(), request.id(), state.changes(), request));
            }
            if (Files.deleteIfExists(path)) Layout.force(path.getParent());
            return new Head(request.id());
        } catch (IOException error) { failed = true; throw error; }
    }

    public synchronized Protocol.BindingInfo bind(Protocol.Binding request) throws IOException {
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
                        long memoryLimit, int runCount, int activeTransactions, long stagedBytes) {}

    public synchronized Stats stats() throws IOException {
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
        return new Stats(head, logicalBytes, journal.size() - MAGIC.length, layout.flushBytes(),
                layout.compactionReadBytes(), layout.compactionWriteBytes(), memoryCharge, memoryLimit,
                layout.runs().size(), active, staged);
    }

    public synchronized Stats flush() throws IOException {
        try (Guard ignored = lock()) {
            refresh();
            flushMemory();
            return statistics();
        } catch (IOException error) { failed = true; throw error; }
    }

    private void flushMemory() throws IOException {
        if (memory.isEmpty()) return;
        String name = UUID.randomUUID() + ".sst";
        var rows = memory.values().iterator();
        var descriptor = SortedRun.write(directory.resolve(name), () -> rows.hasNext() ? rows.next() : null);
        long written = Files.size(directory.resolve(name));
        Layout.force(directory);
        var runs = new ArrayList<>(layout.runs());
        runs.add(descriptor);
        var replacement = new Layout(layout.generation() + 1, position, revision, head, logicalBytes,
                layout.flushBytes() + written, layout.compactionReadBytes(), layout.compactionWriteBytes(), runs);
        replacement.save(directory);
        layout = replacement;
        memory.clear();
        memoryCharge = 0;
    }

    /** Merge immutable inputs outside the publication lock; validate their membership before replacement. */
    public Stats compact() throws IOException {
        final List<Layout.Run> selected;
        final List<SortedRun> inputs;
        synchronized (this) {
            try (Guard ignored = lock()) {
                refresh();
                if (layout.runs().size() < 2) return statistics();
                selected = layout.runs();
                inputs = openRuns(selected);
            }
        }
        String name = UUID.randomUUID() + ".sst";
        long written, read;
        final Layout.Run descriptor;
        try {
            var sources = new ArrayList<SortedRun.Rows>();
            for (var input : inputs) sources.add(input.all());
            descriptor = SortedRun.write(directory.resolve(name), SortedRun.merge(sources, false));
            written = Files.size(directory.resolve(name));
            read = inputs.stream().mapToLong(input -> input.bytesRead).sum();
        } finally { SortedRun.closeAll(inputs); }
        synchronized (this) {
            try (Guard ignored = lock()) {
                refresh();
                boolean accepted = layout.runs().containsAll(selected);
                var runs = new ArrayList<>(layout.runs());
                if (accepted) {
                    runs.removeAll(selected);
                    runs.add(descriptor);
                }
                Layout.force(directory);
                var replacement = new Layout(layout.generation() + 1, layout.position(), layout.revision(),
                        layout.head(), layout.logicalBytes(), layout.flushBytes(), layout.compactionReadBytes() + read,
                        layout.compactionWriteBytes() + written, runs);
                replacement.save(directory);
                layout = replacement;
                if (accepted) for (var old : selected) Files.delete(directory.resolve(old.name()));
                else Files.delete(directory.resolve(name));
                Layout.force(directory);
                return statistics();
            } catch (IOException error) { failed = true; throw error; }
        }
    }

    private void refresh() throws IOException {
        Layout current = Layout.load(directory);
        if (current.generation() < layout.generation() || current.position() < layout.position())
            throw new IOException("Layout moved backwards");
        if (current.position() != layout.position()) {
            memory.clear();
            memoryCharge = 0;
            position = current.position();
            revision = current.revision();
            head = current.head();
            logicalBytes = current.logicalBytes();
        }
        layout = current;
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
            if (body.getInt() != SEAL || !MessageDigest.isEqual(checksum, digest(payload)))
                throw new IOException("Journal integrity check failed");
            Commit commit;
            try {
                commit = Protocol.JSON.fromJson(new String(payload, StandardCharsets.UTF_8), Commit.class);
                if (commit == null || !head.equals(commit.parent()) || findCommit(commit.id()) != null)
                    throw new IllegalArgumentException("Invalid commit chain");
            } catch (RuntimeException invalid) {
                throw new IOException("Invalid journal commit", invalid);
            }
            apply(commit, checksum);
            position += frameSize;
            if (memoryCharge >= memoryLimit) flushMemory();
        }
    }

    private void apply(Commit commit, byte[] digest) {
        long next = ++revision;
        for (var change : commit.changes()) {
            putMemory(new SortedRun.Row(change.index(), change.key(), next, change.value(), commit.id(), change.identity()));
            logicalBytes += Protocol.JSON.toJson(change).getBytes(StandardCharsets.UTF_8).length;
        }
        head = commit.id();
        putMemory(new SortedRun.Row("", "c" + Layout.hash(head), next, HexFormat.of().formatHex(digest), head));
        if (commit.publication() != null)
            putMemory(new SortedRun.Row("", "t" + Layout.hash(commit.publication().transaction()), next,
                    Layout.hash(Protocol.JSON.toJson(commit.publication())), head));
    }

    private void putMemory(SortedRun.Row row) {
        SortedRun.Row previous = memory.put(row, row);
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
    public synchronized void close() throws IOException {
        // Closing another descriptor can release this JVM's POSIX locks on the same file.
        localLock.lock();
        try {
            closed = true;
            try {
                journal.close();
            } finally {
                lockFile.close();
            }
        } finally {
            localLock.unlock();
        }
    }
}
