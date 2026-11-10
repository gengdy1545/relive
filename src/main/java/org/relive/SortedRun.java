package org.relive;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.zip.CRC32C;

import static java.nio.file.StandardOpenOption.*;

/** Immutable ordered records in checksummed blocks, with a sparse block directory and a Bloom filter. */
final class SortedRun implements AutoCloseable {
    private static final byte[] MAGIC = "RELVSST\n".getBytes(StandardCharsets.US_ASCII);
    record Row(String index, String key, long revision, String value, String commit, String identity) implements Comparable<Row> {
        Row(String index, String key, long revision, String value, String commit) {
            this(index, key, revision, value, commit, key);
        }
        Row {
            if (identity == null) throw new IllegalArgumentException("Missing row identity");
            if (revision < 1) throw new IllegalArgumentException("Invalid row revision");
            Protocol.name(commit, false);
            if (index != null && index.isEmpty()) {
                Protocol.name(key, false);
                if (value == null || !value.matches("[0-9a-f]{64}"))
                    throw new IllegalArgumentException("Invalid commit digest");
            } else new Protocol.Mutation(index, key, value, value == null, identity);
        }
        @Override public int compareTo(Row other) {
            int c = index.compareTo(other.index);
            if (c == 0) c = key.compareTo(other.key);
            return c == 0 ? Long.compare(revision, other.revision) : c;
        }
        long charge() { return 256L + 2L * (index.length() + key.length() + identity.length() + commit.length() + (value == null ? 0 : value.length())); }
    }

    @FunctionalInterface interface Rows { Row next() throws IOException; }
    private static final int BLOCK_BYTES = 16 * 1024, BLOOM_BITS_PER_KEY = 16, BLOOM_PROBES = 11;
    private final FileChannel channel;
    // Block directory and Bloom filter, loaded once per open file; the rows stay on disk.
    private final long[] blockOffsets, firstOrdinals, bloom;
    private final int[] blockLengths;
    private final String[] firstIndexes, firstKeys;
    private final long count;
    // Owners and active readers; the channel closes when the last one releases.
    private final java.util.concurrent.atomic.AtomicInteger references = new java.util.concurrent.atomic.AtomicInteger(1);
    final java.util.concurrent.atomic.AtomicLong bytesRead = new java.util.concurrent.atomic.AtomicLong();

    /** The decoded block of one lookup or cursor, so concurrent readers share only immutable state. */
    private static final class Block {
        int number = -1;
        ByteBuffer rows;
        int[] slots;
    }

    SortedRun(Path path) throws IOException {
        channel = FileChannel.open(path, READ);
        try {
            long size = channel.size();
            if (size < 32 || !Arrays.equals(read(8, 0).array(), MAGIC)) throw new IOException("Invalid sorted file header");
            ByteBuffer footer = read(24, size - 24);
            long directory = footer.getLong();
            int length = footer.getInt(), checksum = footer.getInt();
            byte[] marker = new byte[8];
            footer.get(marker);
            if (!Arrays.equals(marker, MAGIC) || directory < 8 || length < 16 || directory + length != size - 24)
                throw new IOException("Invalid sorted file footer");
            ByteBuffer bytes = read(length, directory);
            if (crc(bytes.array(), 0, length) != checksum) throw new IOException("Sorted file directory checksum failed");
            int blocks = bytes.getInt();
            if (blocks < 0 || blocks > length / 20) throw new IOException("Invalid sorted block count");
            blockOffsets = new long[blocks];
            blockLengths = new int[blocks];
            firstOrdinals = new long[blocks + 1];
            firstIndexes = new String[blocks];
            firstKeys = new String[blocks];
            long end = 8;
            for (int b = 0; b < blocks; b++) {
                blockOffsets[b] = bytes.getLong();
                blockLengths[b] = bytes.getInt();
                int rows = bytes.getInt();
                if (blockOffsets[b] != end || blockLengths[b] < 12 || blockLengths[b] > Protocol.MAX_BYTES + 2 * BLOCK_BYTES || rows < 1)
                    throw new IOException("Invalid sorted block directory");
                end += blockLengths[b] + 4L;
                firstOrdinals[b + 1] = firstOrdinals[b] + rows;
                String index = text(bytes);
                firstIndexes[b] = b > 0 && index.equals(firstIndexes[b - 1]) ? firstIndexes[b - 1] : index;
                firstKeys[b] = text(bytes);
            }
            if (end != directory) throw new IOException("Sorted blocks do not reach the directory");
            count = firstOrdinals[blocks];
            int words = bytes.getInt();
            if (words < 8 || words % 8 != 0 || bytes.remaining() != 8L * words) throw new IOException("Invalid sorted Bloom filter");
            bloom = new long[words];
            bytes.asLongBuffer().get(bloom);
        } catch (IOException | RuntimeException error) {
            channel.close();
            throw error instanceof IOException io ? io : new IOException("Invalid sorted file directory", error);
        }
    }

    long count() { return count; }

    /** False only when no version of this key is in the file. */
    boolean mayContain(String index, String key) { return mayContain(hash(index, key)); }

    /** As {@link #mayContain(String, String)} with {@link #hash}, so one lookup hashes its key once. */
    boolean mayContain(long hash) {
        int base = block(bloom, hash);
        long bit = hash, step = Long.rotateLeft(hash, 21) | 1;
        for (int i = 0; i < BLOOM_PROBES; i++, bit += step)
            if ((bloom[base + (int) (bit >>> 6 & 7)] & 1L << bit) == 0) return false;
        return true;
    }

    /** First word of the key's 512-bit block: all of a key's probes share one cache line. */
    private static int block(long[] bloom, long hash) {
        return 8 * (int) Long.remainderUnsigned(Long.rotateLeft(hash, 43), bloom.length / 8);
    }

    private Row row(Block cache, long ordinal) throws IOException {
        int found = Arrays.binarySearch(firstOrdinals, ordinal), block = found >= 0 ? found : -found - 2;
        return row(cache, block, (int) (ordinal - firstOrdinals[block]));
    }

    private Row row(Block cache, int block, int slot) throws IOException {
        if (block != cache.number) {
            ByteBuffer bytes = read(blockLengths[block] + 4, blockOffsets[block]);
            int length = blockLengths[block];
            if (crc(bytes.array(), 0, length) != bytes.getInt(length)) throw new IOException("Sorted block checksum failed");
            int rows = bytes.getInt(length - 4), area = length - 4 - 4 * rows;
            if (rows != firstOrdinals[block + 1] - firstOrdinals[block] || area < 4L * rows) throw new IOException("Invalid sorted block");
            int[] slots = new int[rows + 1];
            for (int i = 0; i < rows; i++) slots[i] = bytes.getInt(area + 4 * i);
            slots[rows] = area;
            if (slots[0] != 0) throw new IOException("Invalid sorted row offset");
            for (int i = 0; i < rows; i++)
                if (slots[i + 1] - slots[i] < 5 || bytes.getInt(slots[i]) != slots[i + 1] - slots[i] - 4)
                    throw new IOException("Invalid sorted row offset");
            Row first = decode(bytes, slots, 0);
            if (!first.index().equals(firstIndexes[block]) || !first.key().equals(firstKeys[block]))
                throw new IOException("Sorted block does not match its directory");
            cache.number = block;
            cache.rows = bytes;
            cache.slots = slots;
        }
        return decode(cache.rows, cache.slots, slot);
    }

    private static Row decode(ByteBuffer rows, int[] slots, int slot) throws IOException {
        int start = slots[slot] + 4, length = slots[slot + 1] - start;
        try {
            Row result = Protocol.JSON.fromJson(new String(rows.array(), start, length, StandardCharsets.UTF_8), Row.class);
            if (result == null) throw new IllegalArgumentException("Missing sorted row");
            return result;
        } catch (RuntimeException error) {
            throw new IOException("Invalid sorted row", error);
        }
    }

    private static boolean before(String rowIndex, String rowKey, String index, String key, boolean after) {
        int c = rowIndex.compareTo(index);
        if (c == 0) c = key == null ? (after ? -1 : 1) : rowKey.compareTo(key);
        return c < 0 || c == 0 && after;
    }

    /** The first ordinal not before the target; the block directory narrows it to one block. */
    private long seek(Block cache, String index, String key, boolean after) throws IOException {
        int low = 0, high = firstKeys.length;
        while (low < high) {
            int mid = (low + high) >>> 1;
            if (before(firstIndexes[mid], firstKeys[mid], index, key, after)) low = mid + 1;
            else high = mid;
        }
        if (low == 0) return 0;
        int block = low - 1, first = 1, last = (int) (firstOrdinals[low] - firstOrdinals[block]);
        while (first < last) {
            int mid = (first + last) >>> 1;
            Row row = row(cache, block, mid);
            if (before(row.index(), row.key(), index, key, after)) first = mid + 1;
            else last = mid;
        }
        return firstOrdinals[block] + first;
    }

    Rows rows(String index, String lower, boolean lowerInclusive, String upper, boolean upperInclusive,
              boolean descending) throws IOException {
        var cache = new Block();
        long begin = seek(cache, index, lower, lower != null && !lowerInclusive);
        long end = seek(cache, index, upper, upper == null || upperInclusive);
        return cursor(cache, begin, end, descending);
    }

    Rows all() { return cursor(new Block(), 0, count, false); }

    private Rows cursor(Block cache, long begin, long end, boolean descending) {
        return new Rows() {
            long current = descending ? end - 1 : begin;
            public Row next() throws IOException {
                if (current < begin || current >= end) return null;
                Row result = row(cache, current);
                current += descending ? -1 : 1;
                return result;
            }
        };
    }

    static Rows merge(List<Rows> inputs, boolean descending) throws IOException {
        record Item(Row row, Rows source) {}
        Comparator<Item> order = Comparator.comparing(Item::row);
        var queue = new PriorityQueue<>(descending ? order.reversed() : order);
        for (Rows source : inputs) {
            Row row = source.next();
            if (row != null) queue.add(new Item(row, source));
        }
        return () -> {
            if (queue.isEmpty()) return null;
            Item result = queue.remove();
            Row next = result.source().next();
            if (next != null) queue.add(new Item(next, result.source()));
            return result.row();
        };
    }

    /** Rows that can hand one row back, so a split file ends before it. */
    static final class Peeking implements Rows {
        private final Rows source;
        private Row held;
        Peeking(Rows source) { this.source = source; }
        Row peek() throws IOException { if (held == null) held = source.next(); return held; }
        public Row next() throws IOException { Row row = peek(); held = null; return row; }
        void push(Row row) { held = row; }
    }

    static Layout.Run write(Path path, Rows rows, long keys, int level) throws IOException {
        return write(path, new Peeking(rows), keys, level, 0);
    }

    /**
     * Writes rows in blocks; {@code keys} bounds the distinct keys and sizes the Bloom filter. With a
     * positive {@code splitBytes}, the file ends at the first new key after that size; the rest stays in {@code rows}.
     */
    static Layout.Run write(Path path, Peeking rows, long keys, int level, long splitBytes) throws IOException {
        var bounds = new java.util.TreeMap<String, Layout.Bounds>();
        long[] bloom = new long[8 * (int) Math.min(1L << 25, Math.max(1, (Math.max(1, keys) * BLOOM_BITS_PER_KEY + 511) / 512))];
        // shortcut: the block directory is held in memory while writing; spool it before multi-GiB files.
        var directory = new java.io.ByteArrayOutputStream();
        var entries = new DataOutputStream(directory);
        entries.writeInt(0); // block count, patched below
        int blocks = 0;
        try (var channel = FileChannel.open(path, CREATE_NEW, WRITE);
             var output = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(channel)))) {
            output.write(MAGIC);
            long offset = 8;
            var block = new java.io.ByteArrayOutputStream();
            var blockOutput = new DataOutputStream(block);
            var slots = new ArrayList<Integer>();
            Row previous = null, first = null;
            for (Row row = rows.next(); ; row = rows.next()) {
                if (row != null && splitBytes > 0 && previous != null && offset + block.size() >= splitBytes
                        && !(previous.index().equals(row.index()) && previous.key().equals(row.key()))) {
                    rows.push(row);
                    row = null;
                }
                if (first != null && (row == null || block.size() >= BLOCK_BYTES)) {
                    for (int slot : slots) blockOutput.writeInt(slot);
                    blockOutput.writeInt(slots.size());
                    byte[] payload = block.toByteArray();
                    output.write(payload);
                    output.writeInt(crc(payload, 0, payload.length));
                    entries.writeLong(offset);
                    entries.writeInt(payload.length);
                    entries.writeInt(slots.size());
                    text(entries, first.index());
                    text(entries, first.key());
                    offset += payload.length + 4L;
                    blocks++;
                    block.reset();
                    slots.clear();
                    first = null;
                }
                if (row == null) break;
                if (previous != null && previous.compareTo(row) >= 0) throw new IOException("Unordered or duplicate sorted row");
                var lower = bounds.get(row.index());
                bounds.put(row.index(), new Layout.Bounds(lower == null ? row.key() : lower.lower(), row.key()));
                byte[] payload = Protocol.JSON.toJson(row).getBytes(StandardCharsets.UTF_8);
                if (payload.length > Protocol.MAX_BYTES) throw new IOException("Sorted row too large");
                if (previous == null || !previous.index().equals(row.index()) || !previous.key().equals(row.key()))
                    add(bloom, hash(row.index(), row.key()));
                if (first == null) first = row;
                slots.add(block.size());
                blockOutput.writeInt(payload.length);
                blockOutput.write(payload);
                previous = row;
            }
            entries.writeInt(bloom.length);
            for (long word : bloom) entries.writeLong(word);
            byte[] all = directory.toByteArray();
            ByteBuffer.wrap(all).putInt(blocks);
            output.write(all);
            output.writeLong(offset);
            output.writeInt(all.length);
            output.writeInt(crc(all, 0, all.length));
            output.write(MAGIC);
            output.flush();
            channel.force(true);
        }
        return new Layout.Run(path.getFileName().toString(), level, java.nio.file.Files.size(path), bounds);
    }

    private static void add(long[] bloom, long hash) {
        int base = block(bloom, hash);
        long bit = hash, step = Long.rotateLeft(hash, 21) | 1;
        for (int i = 0; i < BLOOM_PROBES; i++, bit += step) bloom[base + (int) (bit >>> 6 & 7)] |= 1L << bit;
    }

    /** FNV-1a with a murmur3 finalizer; persisted in files, so it must not depend on String.hashCode. */
    static long hash(String index, String key) {
        long h = 0xcbf29ce484222325L;
        for (int i = 0; i < index.length(); i++) h = (h ^ index.charAt(i)) * 0x100000001b3L;
        h = (h ^ ' ') * 0x100000001b3L;
        for (int i = 0; i < key.length(); i++) h = (h ^ key.charAt(i)) * 0x100000001b3L;
        h ^= h >>> 33; h *= 0xff51afd7ed558ccdL; h ^= h >>> 33; h *= 0xc4ceb9fe1a85ec53L;
        return h ^ h >>> 33;
    }

    private static void text(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String text(ByteBuffer input) {
        int length = input.getInt();
        if (length < 0 || length > input.remaining()) throw new IllegalArgumentException("Invalid sorted directory text");
        String value = new String(input.array(), input.position(), length, StandardCharsets.UTF_8);
        input.position(input.position() + length);
        return value;
    }

    private ByteBuffer read(int length, long offset) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(length);
        while (bytes.hasRemaining()) {
            int n = channel.read(bytes, offset + bytes.position());
            if (n < 0) throw new EOFException("Incomplete sorted file");
            bytesRead.addAndGet(n);
        }
        return bytes.flip();
    }

    private static int crc(byte[] bytes, int offset, int length) {
        var crc = new CRC32C();
        crc.update(bytes, offset, length);
        return (int) crc.getValue();
    }

    static void closeAll(List<SortedRun> runs) throws IOException {
        IOException first = null;
        for (var run : runs) try { run.close(); } catch (IOException e) {
            if (first == null) first = e; else first.addSuppressed(e);
        }
        if (first != null) throw first;
    }

    /** Pins the open file for one reader; false once it has closed. */
    boolean retain() {
        for (int current = references.get(); current > 0; current = references.get())
            if (references.compareAndSet(current, current + 1)) return true;
        return false;
    }

    /** Releases one owner or reader reference. */
    @Override public void close() throws IOException {
        if (references.decrementAndGet() == 0) channel.close();
    }
}
