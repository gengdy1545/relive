package org.relive;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import static java.nio.file.StandardOpenOption.*;

/** Immutable ordered records with an on-disk offset directory, not an in-memory key index. */
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
    private final FileChannel channel;
    private final long count, offsets;
    long bytesRead;

    SortedRun(Path path) throws IOException {
        channel = FileChannel.open(path, READ);
        try {
            long size = channel.size();
            if (size < 32 || !Arrays.equals(read(8, 0).array(), MAGIC)) throw new IOException("Invalid sorted file header");
            ByteBuffer footer = read(24, size - 24);
            count = footer.getLong();
            offsets = footer.getLong();
            byte[] marker = new byte[8];
            footer.get(marker);
            if (!Arrays.equals(marker, MAGIC) || count < 0 || count > (size - 32) / 8
                    || offsets < 8 || offsets + count * 8 != size - 24)
                throw new IOException("Invalid sorted file footer");
        } catch (IOException | RuntimeException error) {
            channel.close();
            throw error;
        }
    }

    private Row row(long ordinal) throws IOException {
        long offset = read(8, offsets + ordinal * 8).getLong();
        long end = ordinal + 1 == count ? offsets : read(8, offsets + (ordinal + 1) * 8).getLong();
        if (offset < 8 || end > offsets || end - offset < 36) throw new IOException("Invalid sorted row offset");
        int length = read(4, offset).getInt();
        if (length < 1 || length > Protocol.MAX_BYTES || end - offset != length + 36L)
            throw new IOException("Invalid sorted row length");
        ByteBuffer frame = read(length + 32, offset + 4);
        byte[] payload = new byte[length], checksum = new byte[32];
        frame.get(payload).get(checksum);
        if (!MessageDigest.isEqual(checksum, checksum(ordinal, payload))) throw new IOException("Sorted row checksum failed");
        try {
            Row result = Protocol.JSON.fromJson(new String(payload, StandardCharsets.UTF_8), Row.class);
            if (result == null) throw new IllegalArgumentException("Missing sorted row");
            return result;
        } catch (RuntimeException error) {
            throw new IOException("Invalid sorted row", error);
        }
    }

    private long seek(String index, String key, boolean after) throws IOException {
        long low = 0, high = count;
        while (low < high) {
            long mid = low + (high - low) / 2;
            Row row = row(mid);
            int c = row.index().compareTo(index);
            if (c == 0) c = key == null ? (after ? -1 : 1) : row.key().compareTo(key);
            if (c < 0 || c == 0 && after) low = mid + 1;
            else high = mid;
        }
        return low;
    }

    Rows rows(String index, String lower, boolean lowerInclusive, String upper, boolean upperInclusive,
              boolean descending) throws IOException {
        long begin = seek(index, lower, lower != null && !lowerInclusive);
        long end = seek(index, upper, upper == null || upperInclusive);
        return cursor(begin, end, descending);
    }

    Rows all() { return cursor(0, count, false); }

    private Rows cursor(long begin, long end, boolean descending) {
        return new Rows() {
            long current = descending ? end - 1 : begin;
            public Row next() throws IOException {
                if (current < begin || current >= end) return null;
                Row result = row(current);
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

    static Layout.Run write(Path path, Rows rows) throws IOException {
        Path offsetsPath = path.resolveSibling(path.getFileName() + ".offsets");
        var bounds = new java.util.TreeMap<String, Layout.Bounds>();
        try (var channel = FileChannel.open(path, CREATE_NEW, WRITE);
             var output = new DataOutputStream(new BufferedOutputStream(Channels.newOutputStream(channel)))) {
            output.write(MAGIC);
            long offset = 8, count = 0;
            Row previous = null;
            try (var offsets = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(offsetsPath, CREATE_NEW)))) {
                for (Row row; (row = rows.next()) != null;) {
                    if (previous != null && previous.compareTo(row) >= 0) throw new IOException("Unordered or duplicate sorted row");
                    var first = bounds.get(row.index());
                    bounds.put(row.index(), new Layout.Bounds(first == null ? row.key() : first.lower(), row.key()));
                    byte[] payload = Protocol.JSON.toJson(row).getBytes(StandardCharsets.UTF_8);
                    if (payload.length > Protocol.MAX_BYTES) throw new IOException("Sorted row too large");
                    offsets.writeLong(offset);
                    output.writeInt(payload.length);
                    output.write(payload);
                    output.write(checksum(count, payload));
                    offset += 36L + payload.length;
                    count++;
                    previous = row;
                }
            }
            Files.copy(offsetsPath, output);
            output.writeLong(count);
            output.writeLong(offset);
            output.write(MAGIC);
            output.flush();
            channel.force(true);
        }
        Files.delete(offsetsPath);
        return new Layout.Run(path.getFileName().toString(), bounds);
    }

    private ByteBuffer read(int length, long offset) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(length);
        while (bytes.hasRemaining()) {
            int n = channel.read(bytes, offset + bytes.position());
            if (n < 0) throw new EOFException("Incomplete sorted file");
            bytesRead += n;
        }
        return bytes.flip();
    }

    private static byte[] checksum(long ordinal, byte[] payload) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            digest.update(ByteBuffer.allocate(8).putLong(ordinal).array());
            return digest.digest(payload);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    static void closeAll(List<SortedRun> runs) throws IOException {
        IOException first = null;
        for (var run : runs) try { run.close(); } catch (IOException e) {
            if (first == null) first = e; else first.addSuppressed(e);
        }
        if (first != null) throw first;
    }

    @Override public void close() throws IOException { channel.close(); }
}
