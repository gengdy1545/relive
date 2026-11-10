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
            assertEquals(Files.size(path.resolve("level1").resolve(Layout.load(path).runs().get(0).name())), stats.compactionWriteBytes());
            assertEquals(before, reader.read(read(null, all("pk"), all("secondary"))));
            assertEquals(List.of(new Entry("01", "aa", "a")), reader.read(read("a", all("pk"))).scans().get(0));
            assertTrue(reader.read(read("b", all("pk"))).scans().get(0).isEmpty());
            assertEquals(List.of(new Entry("2001", "01", "c")), reader.read(read("c", all("secondary"))).scans().get(0));
            long wal = stats.walBytes();
            reader.commit(first);
            assertEquals(wal, reader.stats().walBytes());
            // Compaction moves every row out of the journal's directory into level 1's.
            try (var files = Files.list(path)) {
                assertEquals(0, files.filter(p -> p.toString().endsWith(".sst")).count());
            }
            try (var files = Files.list(path.resolve("level1"))) {
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
            awaitFlushed(store);
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

    static void awaitFlushed(IndexStore store) throws Exception {
        for (int i = 0; i < 500 && store.stats().frozenMemtables() > 0; i++) Thread.sleep(10);
        assertEquals(0, store.stats().frozenMemtables());
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

    @Test public void blockedFilesMatchAnIndependentModelForRandomRanges() throws Exception {
        var path = temporary.newFolder().toPath();
        // key -> version (1-based commit) -> value, null for a delete; written as three commits.
        var model = new java.util.TreeMap<String, java.util.TreeMap<Integer, String>>();
        var commits = List.of("a", "b", "c");
        var changes = new ArrayList<List<Protocol.Mutation>>();
        for (int version = 1; version <= 3; version++) {
            var list = new ArrayList<Protocol.Mutation>();
            for (int i = 0; i < 2000; i++) {
                String key = String.format("%04x", i * 3), value = String.format("%02x", version).repeat(200);
                boolean write = version == 1 || version == 2 && i % 5 == 0 || version == 3 && i % 7 == 0;
                if (!write) continue;
                boolean delete = version == 3;
                list.add(delete ? delete("pk", key) : put("pk", key, value));
                model.computeIfAbsent(key, k -> new java.util.TreeMap<>()).put(version, delete ? null : value);
            }
            changes.add(list);
        }
        var random = new java.util.Random(11);
        try (var store = new IndexStore(path, 64L * 1024 * 1024)) {
            for (int version = 1; version <= 3; version++) {
                store.commit(new Commit(version == 1 ? "" : commits.get(version - 2), commits.get(version - 1), changes.get(version - 1)));
                if (version < 3) store.flush(); // two blocked files, the deletes stay in the Memtable
            }
            checkRandomRanges(store, model, commits, random);
            store.flush();
            checkRandomRanges(store, model, commits, random);
            store.compact();
            assertEquals(1, store.stats().runCount());
            checkRandomRanges(store, model, commits, random);
        }
        try (var reopened = new IndexStore(path, 64L * 1024 * 1024)) {
            checkRandomRanges(reopened, model, commits, random);
        }
    }

    private static void checkRandomRanges(IndexStore store, java.util.TreeMap<String, java.util.TreeMap<Integer, String>> model,
                                          List<String> commits, java.util.Random random) throws Exception {
        for (int n = 0; n < 300; n++) {
            int version = 1 + random.nextInt(3);
            String lower = random.nextInt(8) == 0 ? null : String.format("%04x", random.nextInt(6100));
            String upper = random.nextInt(8) == 0 ? null : n % 3 == 0 ? lower : String.format("%04x", random.nextInt(6100));
            if (lower != null && upper != null && lower.compareTo(upper) > 0) { String t = lower; lower = upper; upper = t; }
            boolean lowerInclusive = n % 3 == 0 || random.nextBoolean(), upperInclusive = n % 3 == 0 || random.nextBoolean();
            boolean descending = random.nextBoolean();
            int limit = random.nextInt(4) == 0 ? 0 : 1 + random.nextInt(40);
            var expected = new ArrayList<Entry>();
            var keys = new ArrayList<>(model.keySet());
            if (descending) java.util.Collections.reverse(keys);
            for (String key : keys) {
                if (lower != null && (key.compareTo(lower) < 0 || !lowerInclusive && key.equals(lower))) continue;
                if (upper != null && (key.compareTo(upper) > 0 || !upperInclusive && key.equals(upper))) continue;
                var visible = model.get(key).floorEntry(version);
                if (visible == null || visible.getValue() == null) continue;
                if (limit > 0 && expected.size() == limit) break;
                expected.add(new Entry(key, visible.getValue(), commits.get(visible.getKey() - 1)));
            }
            var scan = new Scan("pk", lower, lowerInclusive, upper, upperInclusive, descending, limit);
            assertEquals(scan.toString(), expected, store.read(read(commits.get(version - 1), scan)).scans().get(0));
        }
    }

    @Test public void bloomFilterSkipsFilesWithoutThePointKeyAndOtherwiseFailsClosed() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path, 1024 * 1024)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"), put("pk", "03", "cc"))));
            store.flush();
            store.commit(new Commit("a", "b", List.of(put("pk", "00", "00"), put("pk", "ff", "ff"))));
            store.flush();
        }
        var spanning = path.resolve(Layout.load(path).runs().get(1).name());
        byte[] bytes = Files.readAllBytes(spanning);
        bytes[20] ^= 1; // inside the first data block; the block directory and filter stay intact
        Files.write(spanning, bytes);
        try (var store = new IndexStore(path, 1024 * 1024)) {
            // The spanning file's bounds include 01, so only its filter keeps this read away from the damaged block.
            assertEquals(List.of(new Entry("01", "aa", "a")),
                    store.read(read(null, new Scan("pk", "01", true, "01", true, false, 0))).scans().get(0));
            assertThrows(IOException.class, () -> store.read(read(null, new Scan("pk", "00", true, "00", true, false, 0))));
        }
        try (var store = new IndexStore(path, 1024 * 1024)) {
            assertThrows(IOException.class, () -> store.read(read(null, new Scan("pk", "01", true, "03", true, false, 0))));
        }
        bytes = Files.readAllBytes(spanning);
        bytes[bytes.length - 30] ^= 1; // the filter at the end of the directory
        Files.write(spanning, bytes);
        try (var store = new IndexStore(path, 1024 * 1024)) {
            assertThrows(IOException.class, () -> store.read(read(null, new Scan("pk", "01", true, "01", true, false, 0))));
        }
    }

    @Test public void unchangedJournalSkipsLayoutReloadAndAnyAppendReloadsIt() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var writer = new IndexStore(path, 1024 * 1024); var reader = new IndexStore(path, 1024 * 1024)) {
            writer.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            writer.flush();
            assertEquals(List.of(new Entry("01", "aa", "a")), reader.read(read(null, all("pk"))).scans().get(0));
            var layout = path.resolve("layout");
            byte[] saved = Files.readAllBytes(layout);
            Files.writeString(layout, "{\"body\":\"{}\",\"sha256\":\"wrong\"}");
            // The journal did not move, so the reader must not touch the damaged layout.
            assertEquals(List.of(new Entry("01", "aa", "a")), reader.read(read(null, all("pk"))).scans().get(0));
            Files.write(layout, saved);
            writer.commit(new Commit("a", "b", List.of(put("pk", "02", "bb"))));
            writer.compact();
            assertEquals(1, reader.stats().runCount()); // the compaction marker makes the reader reload
            Files.writeString(layout, "{\"body\":\"{}\",\"sha256\":\"wrong\"}");
            writer.commit(new Commit("b", "c", List.of(put("pk", "03", "cc"))));
            assertThrows(IOException.class, () -> reader.read(read(null, all("pk"))));
        }
    }

    @Test public void pressureFlushBuildsOnALayoutSavedWithoutItsMarker() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path, 1024)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            var current = Layout.load(path);
            // A publisher that saved this layout and stopped before appending its marker.
            var unmarked = new Layout(current.generation() + 1, current.position(), current.revision(), current.head(),
                    current.logicalBytes(), current.flushBytes(), current.compactionReadBytes(), current.compactionWriteBytes(),
                    current.runs());
            unmarked.save(path);
            var large = new ArrayList<Protocol.Mutation>();
            for (int i = 0; i < 8; i++) large.add(put("pk", String.format("%02x", 16 + i), "bb"));
            // The background flush reloads the saved layout and builds on it instead of on this handle's copy.
            store.commit(new Commit("a", "b", large));
            awaitFlushed(store);
            var replaced = Layout.load(path);
            assertEquals(unmarked.generation() + 1, replaced.generation());
            assertTrue(replaced.runs().containsAll(unmarked.runs()));
            assertEquals(1, replaced.runs().size());
        }
        try (var reopened = new IndexStore(path, 1024)) {
            assertEquals(List.of(new Entry("01", "aa", "a")), reopened.read(read("a", all("pk"))).scans().get(0));
            assertEquals(9, reopened.read(read("b", all("pk"))).scans().get(0).size());
        }
    }

    /** Both handles flush the same journal bytes; only one file may cover them. */
    @Test(timeout = 60000) public void concurrentFlushesOfTwoHandlesNeverDuplicateRows() throws Exception {
        var path = temporary.newFolder().toPath();
        int commits = 400;
        try (var writer = new IndexStore(path, 8 * 1024); var reader = new IndexStore(path, 8 * 1024)) {
            var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
            var stop = new java.util.concurrent.atomic.AtomicBoolean();
            var reading = executor.submit(() -> {
                while (!stop.get()) reader.read(read(null, all("pk")));
                return null;
            });
            String parent = "";
            for (int i = 0; i < commits; i++) {
                String id = "c" + i;
                writer.commit(new Commit(parent, id, List.of(put("pk", String.format("%04x", i % 100), String.format("%04x", i)))));
                parent = id;
            }
            stop.set(true);
            reading.get(30, java.util.concurrent.TimeUnit.SECONDS);
            executor.shutdown();
            writer.flush();
            reader.flush();
            writer.compact(); // a duplicated row in two files makes the merge reject its input
            for (var store : List.of(writer, reader)) {
                var rows = store.read(read(null, all("pk"))).scans().get(0);
                assertEquals(100, store.read(read(null, new Scan("pk", null, false, null, false, false, 0))).scans().get(0).size());
                assertEquals(new Entry("0001", String.format("%04x", 301), "c301"), rows.get(1));
            }
        }
    }

    @Test(timeout = 60000) public void backgroundCompactionBoundsLevelZeroAndSplitsDisjointLevelOne() throws Exception {
        var path = temporary.newFolder().toPath();
        var model = new java.util.TreeMap<String, java.util.TreeMap<Integer, String>>();
        var random = new java.util.Random(5);
        try (var store = new IndexStore(path, 8 * 1024)) {
            store.levelOneBytes = 16 * 1024;
            String parent = "";
            for (int version = 1; version <= 600; version++) {
                var changes = new ArrayList<Protocol.Mutation>();
                for (int k = 0; k < 4; k++) {
                    String key = String.format("%04x", random.nextInt(300) * 7), value = String.format("%04x", version);
                    if (model.containsKey(key) && model.get(key).containsKey(version)) continue;
                    boolean delete = random.nextInt(10) == 0;
                    changes.add(delete ? delete("pk", key) : put("pk", key, value));
                    model.computeIfAbsent(key, x -> new java.util.TreeMap<>()).put(version, delete ? null : value);
                }
                String id = "v" + version;
                store.commit(new Commit(parent, id, changes));
                parent = id;
            }
            awaitFlushed(store);
            for (int i = 0; i < 500 && store.stats().compactionWriteBytes() == 0; i++) Thread.sleep(10);
            var commits = new ArrayList<String>();
            for (int version = 1; version <= 600; version++) commits.add("v" + version);
            checkRandomRanges(store, model, commits, random);
            var layout = Layout.load(path); // rejects overlapping level-1 files
            assertTrue(layout.runs().stream().filter(run -> run.level() == 0).count() <= 8);
            assertTrue(layout.runs().stream().filter(run -> run.level() == 1).count() > 1);
            assertTrue(store.stats().compactionWriteBytes() > 0);
        }
    }

    @Test public void ownLayoutMarkerKeepsLaterReadsOffTheLayoutFile() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path, 1024 * 1024); var other = new IndexStore(path, 1024 * 1024)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            store.flush();
            store.commit(new Commit("a", "b", List.of(put("pk", "02", "bb"))));
            store.flush();
            assertEquals(1, store.compact().runCount()); // publishes a layout and its marker
            var layout = path.resolve("layout");
            byte[] saved = Files.readAllBytes(layout);
            Files.writeString(layout, "{\"body\":\"{}\",\"sha256\":\"wrong\"}");
            // This handle appended both layout markers itself, so it has nothing to reload.
            assertEquals(2, store.read(read(null, all("pk"))).scans().get(0).size());
            Files.write(layout, saved);
            other.commit(new Commit("b", "c", List.of(put("pk", "03", "cc"))));
            Files.writeString(layout, "{\"body\":\"{}\",\"sha256\":\"wrong\"}");
            assertThrows(IOException.class, () -> store.read(read(null, all("pk"))));
        }
    }

    @Test public void bloomFilterFalsePositivesStayRare() throws Exception {
        var path = temporary.newFolder().toPath().resolve(java.util.UUID.randomUUID() + ".sst");
        var rows = new ArrayList<SortedRun.Row>();
        for (int i = 0; i < 20000; i++) rows.add(new SortedRun.Row("pk", String.format("%08x", 2 * i), 1, "aa", "a"));
        var iterator = rows.iterator();
        SortedRun.write(path, () -> iterator.hasNext() ? iterator.next() : null, rows.size(), 0);
        try (var run = new SortedRun(path)) {
            int falsePositives = 0;
            for (int i = 0; i < 20000; i++) {
                assertTrue(run.mayContain("pk", String.format("%08x", 2 * i)));
                if (run.mayContain("pk", String.format("%08x", 2 * i + 1))) falsePositives++;
            }
            // 16 bits and 11 probes per key in 512-bit blocks give about 0.25% (47 here); 10 unblocked bits gave 0.8%.
            assertTrue("false positives: " + falsePositives, falsePositives < 60);
        }
    }

    @Test public void logicalMutationBytesCountEachNormalizedMutation() throws Exception {
        var path = temporary.newFolder().toPath();
        long expected = "{\"index\":\"pk\",\"key\":\"01\",\"value\":\"aa\",\"delete\":false,\"identity\":\"01\"}".length()
                + "{\"index\":\"pk\",\"key\":\"02\",\"value\":null,\"delete\":true,\"identity\":\"02\"}".length();
        try (var store = new IndexStore(path)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"), delete("pk", "02"))));
            assertEquals(expected, store.stats().logicalMutationBytes());
        }
        try (var reopened = new IndexStore(path)) {
            assertEquals(expected, reopened.stats().logicalMutationBytes());
        }
    }
}
