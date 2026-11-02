package org.relive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static java.nio.file.StandardOpenOption.*;
import static java.nio.file.StandardCopyOption.*;

/** Active transaction state is shared by publishers; completed identities live in sorted storage. */
record TransactionState(String transaction, long operation, String id, List<Protocol.Mutation> changes) {
    TransactionState {
        Protocol.name(transaction, false);
        if (id != null) Protocol.name(id, false);
        if (operation < 0 || changes == null || changes.size() > 10000 || id == null && !changes.isEmpty())
            throw new IllegalArgumentException("Invalid staged transaction");
        changes = List.copyOf(changes);
    }

    static TransactionState load(Path path, String transaction) throws IOException {
        if (!Files.exists(path)) return new TransactionState(transaction, 0, null, List.of());
        try {
            if (Files.size(path) > Protocol.MAX_BYTES) throw new IOException("Staged transaction exceeds 8 MiB");
            var envelope = Protocol.JSON.fromJson(Files.readString(path), Layout.Envelope.class);
            if (envelope == null || envelope.body() == null || !Layout.hash(envelope.body()).equals(envelope.sha256()))
                throw new IOException("Staged transaction checksum failed");
            var state = Protocol.JSON.fromJson(envelope.body(), TransactionState.class);
            if (state == null || !state.transaction().equals(transaction)) throw new IOException("Wrong transaction identity");
            return state;
        } catch (RuntimeException invalid) { throw new IOException("Invalid staged transaction", invalid); }
    }

    void save(Path path) throws IOException {
        String body = Protocol.JSON.toJson(this);
        byte[] bytes = Protocol.JSON.toJson(new Layout.Envelope(body, Layout.hash(body))).getBytes(StandardCharsets.UTF_8);
        if (bytes.length > Protocol.MAX_BYTES) throw new IllegalArgumentException("Staged transaction exceeds 8 MiB");
        replace(path, bytes);
    }

    static void replace(Path path, byte[] bytes) throws IOException {
        Path temporary = path.resolveSibling(UUID.randomUUID() + ".pending");
        try (var channel = FileChannel.open(temporary, CREATE_NEW, WRITE)) {
            ByteBuffer data = ByteBuffer.wrap(bytes);
            while (data.hasRemaining()) channel.write(data);
            channel.force(true);
        }
        Files.move(temporary, path, ATOMIC_MOVE, REPLACE_EXISTING);
        Layout.force(path.getParent());
    }
}
