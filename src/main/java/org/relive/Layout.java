package org.relive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collections;
import java.util.UUID;

import static java.nio.file.StandardCopyOption.*;
import static java.nio.file.StandardOpenOption.*;

/** A durable physical checkpoint; logical commit identity remains supplied by the caller. */
record Layout(long generation, long position, long revision, String head, long logicalBytes,
              long flushBytes, long compactionReadBytes, long compactionWriteBytes, List<Run> runs) {
    Layout {
        if (generation < 0 || position < 8 || revision < 0 || logicalBytes < 0 || flushBytes < 0
                || compactionReadBytes < 0 || compactionWriteBytes < 0 || runs == null)
            throw new IllegalArgumentException("Invalid layout fields");
        Protocol.name(head, revision == 0);
        if ((revision == 0) != head.isEmpty() || (revision == 0) != (position == 8))
            throw new IllegalArgumentException("Invalid checkpoint identity");
        runs = List.copyOf(runs);
        if (runs.stream().map(Run::name).distinct().count() != runs.size())
            throw new IllegalArgumentException("Invalid sorted file inventory");
    }
    record Bounds(String lower, String upper) {
        Bounds {
            if (lower == null || upper == null || lower.compareTo(upper) > 0)
                throw new IllegalArgumentException("Invalid file key bounds");
        }
        boolean overlaps(String from, boolean fromInclusive, String to, boolean toInclusive) {
            int above = from == null ? 1 : upper.compareTo(from);
            int below = to == null ? -1 : lower.compareTo(to);
            return (above > 0 || above == 0 && fromInclusive) && (below < 0 || below == 0 && toInclusive);
        }
    }
    record Run(String name, Map<String, Bounds> indexes) {
        Run {
            if (name == null || !name.matches("[0-9a-f-]{36}\\.sst") || indexes == null || indexes.isEmpty())
                throw new IllegalArgumentException("Invalid file descriptor");
            for (var entry : indexes.entrySet()) {
                var bounds = entry.getValue();
                if (entry.getKey() == null || bounds == null) throw new IllegalArgumentException("Missing file bounds");
                if (entry.getKey().isEmpty()) {
                    Protocol.name(bounds.lower(), false); Protocol.name(bounds.upper(), false);
                } else {
                    new Protocol.Mutation(entry.getKey(), bounds.lower(), "", false);
                    new Protocol.Mutation(entry.getKey(), bounds.upper(), "", false);
                }
            }
            indexes = Collections.unmodifiableMap(new TreeMap<>(indexes));
        }
        boolean overlaps(String index, String lower, boolean lowerInclusive, String upper, boolean upperInclusive) {
            var bounds = indexes.get(index);
            return bounds != null && bounds.overlaps(lower, lowerInclusive, upper, upperInclusive);
        }
        boolean overlaps(Protocol.Scan scan) {
            return overlaps(scan.index(), scan.lower(), scan.lowerInclusive(), scan.upper(), scan.upperInclusive());
        }
    }
    record Envelope(String body, String sha256) {}
    static Layout empty() { return new Layout(0, 8, 0, "", 0, 0, 0, 0, List.of()); }

    static Layout load(Path directory) throws IOException {
        Path path = directory.resolve("layout");
        if (!Files.exists(path)) return empty();
        if (Files.size(path) > Protocol.MAX_BYTES) throw new IOException("Layout exceeds 8 MiB; compact runs");
        try {
            var envelope = Protocol.JSON.fromJson(Files.readString(path), Envelope.class);
            if (envelope == null || envelope.body() == null || !hash(envelope.body()).equals(envelope.sha256()))
                throw new IOException("Layout checksum failed");
            Layout result = Protocol.JSON.fromJson(envelope.body(), Layout.class);
            if (result == null) throw new IOException("Missing layout");
            return result;
        } catch (RuntimeException invalid) {
            throw new IOException("Invalid layout", invalid);
        }
    }

    void save(Path directory) throws IOException {
        String body = Protocol.JSON.toJson(this);
        byte[] bytes = Protocol.JSON.toJson(new Envelope(body, hash(body))).getBytes(StandardCharsets.UTF_8);
        if (bytes.length > Protocol.MAX_BYTES) throw new IOException("Layout exceeds 8 MiB; compact runs");
        Path temporary = directory.resolve(UUID.randomUUID() + ".layout");
        try (var channel = FileChannel.open(temporary, CREATE_NEW, WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) channel.write(buffer);
            channel.force(true);
        }
        Files.move(temporary, directory.resolve("layout"), ATOMIC_MOVE, REPLACE_EXISTING);
        force(directory);
    }

    static void force(Path path) throws IOException {
        try (var channel = FileChannel.open(path, READ)) { channel.force(true); }
    }

    static String hash(String text) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
