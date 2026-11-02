package org.relive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.relive.Protocol.Commit;
import org.relive.Protocol.Entry;
import org.relive.Protocol.Scan;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.relive.IndexStoreTest.*;

public class SortedStorageTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void spillAndCompactionPreserveAllVersionsAndIdempotentCommits() throws Exception {
        var path = temporary.newFolder().toPath();
        var first = new Commit("", "a", List.of(put("pk", "01", "aa"), put("secondary", "1001", "01")));
        try (var writer = new IndexStore(path, 1024); var reader = new IndexStore(path, 1024)) {
            writer.commit(first);
            writer.flush();
            writer.commit(new Commit("a", "b", List.of(delete("pk", "01"), delete("secondary", "1001"))));
            writer.flush();
            writer.commit(new Commit("b", "c", List.of(put("pk", "01", "cc"), put("secondary", "2001", "01"))));
            writer.flush();
            assertEquals(3, writer.stats().runCount());
            long fileBytes = 0;
            for (var descriptor : Layout.load(path).runs()) fileBytes += Files.size(path.resolve(descriptor.name()));
            assertEquals(fileBytes, writer.stats().flushBytes());
            assertEquals(Files.size(path.resolve("journal")) - 8, writer.stats().walBytes());
            assertTrue(writer.stats().memoryCharge() < 1024);
            var before = reader.read(read(null, all("pk"), all("secondary")));
            var stats = writer.compact();
            assertEquals(1, stats.runCount());
            assertTrue(stats.compactionReadBytes() > 0);
            assertTrue(stats.compactionWriteBytes() > 0);
            assertEquals(Files.size(path.resolve(Layout.load(path).runs().get(0).name())), stats.compactionWriteBytes());
            assertEquals(before, reader.read(read(null, all("pk"), all("secondary"))));
            assertEquals(List.of(new Entry("01", "aa", "a")), reader.read(read("a", all("pk"))).scans().get(0));
            assertTrue(reader.read(read("b", all("pk"))).scans().get(0).isEmpty());
            assertEquals(List.of(new Entry("2001", "01", "c")), reader.read(read("c", all("secondary"))).scans().get(0));
            long wal = stats.walBytes();
            reader.commit(first);
            assertEquals(wal, reader.stats().walBytes());
            try (var files = Files.list(path)) {
                assertEquals(1, files.filter(p -> p.toString().endsWith(".sst")).count());
            }
        }
        try (var reopened = new IndexStore(path, 1024)) {
            assertEquals("c", reopened.head().id());
            assertEquals(0, reopened.stats().memoryCharge());
            assertEquals(List.of(new Entry("01", "cc", "c")), reopened.read(read(null, all("pk"))).scans().get(0));
            assertEquals(List.of(new Entry("01", "aa", "a")), reopened.read(read("a", all("pk"))).scans().get(0));
        }
    }

    @Test public void oversizedTransactionSpillsDataAndCommitMetadataTogether() throws Exception {
        var path = temporary.newFolder().toPath();
        var changes = new ArrayList<Protocol.Mutation>();
        for (int i = 0; i < 8; i++) changes.add(put("pk", String.format("%02x", i), "aa"));
        var commit = new Commit("", "one-whole-transaction", changes);
        try (var store = new IndexStore(path, 1024)) {
            store.commit(commit);
            assertEquals(0, store.stats().memoryCharge());
            assertEquals(1, store.stats().runCount());
        }
        try (var store = new IndexStore(path, 1024)) {
            long wal = store.stats().walBytes();
            store.commit(commit);
            assertEquals(wal, store.stats().walBytes());
            assertEquals(8, store.read(read(null, all("pk"))).scans().get(0).size());
        }
    }

    @Test public void onDiskRangesRespectBoundsDirectionAndVersionSelection() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path, 1024)) {
            store.commit(new Commit("", "a", List.of(put("pk", "00", "00"), put("pk", "01", "01"), put("pk", "ff", "ff"))));
            store.commit(new Commit("a", "b", List.of(put("pk", "01", "bb"), put("other", "01", "cc"))));
            store.flush();
            store.compact();
            assertEquals(List.of(new Entry("ff", "ff", "a"), new Entry("01", "bb", "b")),
                    store.read(read(null, new Scan("pk", "00", false, null, false, true, 2))).scans().get(0));
            assertEquals(List.of(new Entry("01", "01", "a")),
                    store.read(read("a", new Scan("pk", "01", true, "01", true, false, 1))).scans().get(0));
            assertTrue(store.read(read(null, new Scan("pk", "01", false, "01", true, false, 10))).scans().get(0).isEmpty());
            assertTrue(store.read(read("", all("pk"))).scans().get(0).isEmpty());
        }
    }

    @Test public void readerCanFinishFromUnlinkedImmutableInputs() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path, 1024)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            store.flush();
            store.commit(new Commit("a", "b", List.of(put("pk", "02", "bb"))));
            store.flush();
            var layout = Layout.load(path);
            try (var pinned = new SortedRun(path.resolve(layout.runs().get(0).name()))) {
                store.compact();
                assertFalse(Files.exists(path.resolve(layout.runs().get(0).name())));
                var rows = pinned.rows("pk", "01", true, "01", true, false);
                assertEquals("aa", rows.next().value());
                assertNull(rows.next());
            }
        }
    }

    @Test(timeout = 20000) public void concurrentCompactionAndFlushDoNotLoseNewRuns() throws Exception {
        var path = temporary.newFolder().toPath();
        var executor = Executors.newFixedThreadPool(2);
        try (var writer = new IndexStore(path, 4096); var compactor = new IndexStore(path, 4096)) {
            String parent = "";
            for (int i = 0; i < 12; i++) {
                String id = "base-" + i;
                var changes = new ArrayList<Protocol.Mutation>();
                for (int k = 0; k < 32; k++) changes.add(put("pk", String.format("%04x", i * 32 + k), "aa".repeat(128)));
                writer.commit(new Commit(parent, id, changes));
                parent = id;
            }
            var gate = new CountDownLatch(1);
            var merging = executor.submit(() -> {
                gate.countDown();
                for (int i = 0; i < 3; i++) compactor.compact();
                return null;
            });
            assertTrue(gate.await(1, TimeUnit.SECONDS));
            for (int i = 0; i < 20; i++) {
                String id = "tail-" + i;
                writer.commit(new Commit(parent, id, List.of(put("pk", "ffff", String.format("%02x", i)))));
                writer.flush();
                parent = id;
            }
            merging.get(10, TimeUnit.SECONDS);
            writer.compact();
            assertEquals(List.of(new Entry("ffff", "13", "tail-19")),
                    compactor.read(read(null, new Scan("pk", "ffff", true, "ffff", true, false, 1))).scans().get(0));
            assertEquals(385, writer.read(read(null, new Scan("pk", null, false, null, false, false, 1000))).scans().get(0).size());
            assertEquals("aa".repeat(128), writer.read(read("base-0", all("pk"))).scans().get(0).get(0).value());
        } finally { executor.shutdownNow(); }
    }

    @Test public void corruptedManifestAndSortedRowsAreRejected() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path, 1024)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            store.flush();
        }
        var layout = Layout.load(path);
        var file = path.resolve(layout.runs().get(0).name());
        byte[] bytes = Files.readAllBytes(file);
        bytes[20] ^= 1;
        Files.write(file, bytes);
        try (var store = new IndexStore(path, 1024)) {
            assertThrows(IOException.class, () -> store.read(read(null, all("pk"))));
        }
        Files.writeString(path.resolve("layout"), "{\"body\":\"{}\",\"sha256\":\"wrong\"}");
        assertThrows(IOException.class, () -> new IndexStore(path, 1024));
    }
}
