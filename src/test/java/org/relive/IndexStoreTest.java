package org.relive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.relive.Protocol.Commit;
import org.relive.Protocol.Entry;
import org.relive.Protocol.Mutation;
import org.relive.Protocol.Read;
import org.relive.Protocol.Scan;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class IndexStoreTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    static Mutation put(String index, String key, String value) { return new Mutation(index, key, value, false); }
    static Mutation delete(String index, String key) { return new Mutation(index, key, null, true); }
    static Scan all(String index) { return new Scan(index, null, false, null, false, false, 100); }
    static Read read(String at, Scan... scans) { return new Read(at, List.of(scans)); }

    @Test public void atomicMultiIndexChangesAndHistoricalReads() throws Exception {
        var directory = temporary.newFolder().toPath();
        try (var writer = new IndexStore(directory); var reader = new IndexStore(directory)) {
            writer.commit(new Commit("", "source/900", List.of(
                    put("pk", "01", "aa"), put("pk", "02", "bb"), put("secondary", "1001", "01"))));
            var first = reader.read(read(null, all("pk"), all("secondary")));
            assertEquals("source/900", first.at());
            assertEquals(List.of(new Entry("01", "aa", "source/900"), new Entry("02", "bb", "source/900")), first.scans().get(0));
            writer.commit(new Commit("source/900", "source/17", List.of(
                    put("pk", "01", "cc"), delete("secondary", "1001"), put("secondary", "2001", "01"))));
            var second = reader.read(read(null, all("pk"), all("secondary")));
            assertEquals("source/17", second.at());
            assertEquals(List.of(new Entry("01", "cc", "source/17"), new Entry("02", "bb", "source/900")), second.scans().get(0));
            assertEquals(List.of(new Entry("2001", "01", "source/17")), second.scans().get(1));
            assertEquals(first, reader.read(read("source/900", all("pk"), all("secondary"))));
            assertTrue(reader.read(read("", all("pk"))).scans().get(0).isEmpty());
            assertThrows(IndexStore.Conflict.class, () -> reader.read(read("not-received", all("pk"))));
        }
        try (var reopened = new IndexStore(directory)) {
            assertEquals("source/17", reopened.head().id());
            assertEquals("aa", reopened.read(read("source/900", all("pk"))).scans().get(0).get(0).value());
        }
    }

    @Test public void deleteDoesNotResurrectAndReinsertionPreservesHistory() throws Exception {
        try (var store = new IndexStore(temporary.newFolder().toPath())) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            store.commit(new Commit("a", "b", List.of(delete("pk", "01"))));
            assertTrue(store.read(read(null, all("pk"))).scans().get(0).isEmpty());
            store.commit(new Commit("b", "c", List.of(put("pk", "01", "bb"), put("pk", "01", "cc"))));
            assertEquals(List.of(new Entry("01", "cc", "c")), store.read(read(null, all("pk"))).scans().get(0));
            assertTrue(store.read(read("b", all("pk"))).scans().get(0).isEmpty());
            assertEquals(List.of(new Entry("01", "aa", "a")), store.read(read("a", all("pk"))).scans().get(0));
        }
    }

    @Test public void rangeBoundsDirectionAndLimitAreByteOrdered() throws Exception {
        try (var store = new IndexStore(temporary.newFolder().toPath())) {
            store.commit(new Commit("", "a", List.of(put("pk", "00", ""), put("pk", "01", "01"),
                    put("pk", "0100", "02"), put("pk", "ff", "03"))));
            assertEquals(List.of(new Entry("0100", "02", "a"), new Entry("01", "01", "a")),
                    store.read(read(null, new Scan("pk", "00", false, "ff", false, true, 2))).scans().get(0));
            for (boolean inclusive : List.of(false, true)) {
                assertTrue(store.read(read(null, new Scan("pk", "01", false, "01", inclusive, false, 10))).scans().get(0).isEmpty());
            }
            assertEquals(List.of(new Entry("01", "01", "a")),
                    store.read(read(null, new Scan("pk", "01", true, "01", true, false, 10))).scans().get(0));
            assertThrows(IllegalArgumentException.class, () -> new Scan("pk", "ff", true, "00", true, false, 10));
        }
    }

    @Test public void staleParentAndChangedRetryNeverMutateState() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path)) {
            var original = new Commit("", "a", List.of(put("pk", "01", "aa")));
            store.commit(original);
            long size = Files.size(path.resolve("journal"));
            assertThrows(IndexStore.Conflict.class, () -> store.commit(new Commit("", "b", List.of(put("pk", "02", "bb")))));
            assertThrows(IndexStore.Conflict.class, () -> store.commit(new Commit("", "a", List.of(put("pk", "01", "cc")))));
            assertEquals(size, Files.size(path.resolve("journal")));
            store.commit(new Commit("a", "b", List.of()));
            size = Files.size(path.resolve("journal"));
            assertEquals("a", store.commit(original).id());
            assertEquals("b", store.head().id());
            assertEquals(size, Files.size(path.resolve("journal")));
            assertEquals(List.of(new Entry("01", "aa", "a")), store.read(read(null, all("pk"))).scans().get(0));
        }
    }

    @Test public void independentPublishersCompeteWithoutOverwritingEachOther() throws Exception {
        var path = temporary.newFolder().toPath();
        var pool = Executors.newFixedThreadPool(2);
        try (var first = new IndexStore(path); var second = new IndexStore(path)) {
            var gate = new CountDownLatch(1);
            var a = pool.submit(() -> claim(first, gate, "a", "01"));
            var b = pool.submit(() -> claim(second, gate, "b", "02"));
            gate.countDown();
            int accepted = a.get(10, TimeUnit.SECONDS) + b.get(10, TimeUnit.SECONDS);
            assertEquals(1, accepted);
            assertEquals(first.read(read(null, all("pk"))), second.read(read(null, all("pk"))));
            assertEquals(1, second.read(read(null, all("pk"))).scans().get(0).size());
        } finally {
            pool.shutdownNow();
        }
    }

    private static int claim(IndexStore store, CountDownLatch gate, String id, String key) throws Exception {
        gate.await();
        try {
            store.commit(new Commit("", id, List.of(put("pk", key, "aa"))));
            return 1;
        } catch (IndexStore.Conflict expected) {
            return 0;
        }
    }

    @Test public void incompleteAndCorruptFramesFailClosedWithoutRepair() throws Exception {
        for (boolean corrupt : List.of(false, true)) {
            var path = temporary.newFolder().toPath();
            try (var store = new IndexStore(path)) {
                store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            }
            var journal = path.resolve("journal");
            byte[] bytes = Files.readAllBytes(journal);
            if (corrupt) bytes[bytes.length - 10] ^= 1;
            else bytes = Arrays.copyOf(bytes, bytes.length - 1);
            Files.write(journal, bytes);
            assertThrows(IOException.class, () -> new IndexStore(path));
            assertArrayEquals(bytes, Files.readAllBytes(journal));
        }
    }

    @Test public void incompleteTailStopsAlreadyOpenReader() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            Files.write(path.resolve("journal"), new byte[]{0, 0}, java.nio.file.StandardOpenOption.APPEND);
            assertThrows(IOException.class, store::head);
            assertThrows(IOException.class, () -> store.read(read("a", all("pk"))));
        }
    }

    @Test public void readerWaitsForWholeTransactionWhilePublisherHoldsLock() throws Exception {
        var donor = temporary.newFolder().toPath();
        try (var store = new IndexStore(donor)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"), put("secondary", "1001", "01"))));
        }
        byte[] complete = Files.readAllBytes(donor.resolve("journal"));
        var path = temporary.newFolder().toPath();
        var pool = Executors.newSingleThreadExecutor();
        try (var reader = new IndexStore(path);
             var lockFile = FileChannel.open(path.resolve("publish.lock"), java.nio.file.StandardOpenOption.WRITE);
             var journal = FileChannel.open(path.resolve("journal"), java.nio.file.StandardOpenOption.WRITE)) {
            java.util.concurrent.Future<Protocol.Results> result;
            try (var lock = lockFile.lock()) {
                int split = complete.length / 2;
                journal.position(8);
                ByteBuffer first = ByteBuffer.wrap(complete, 8, split - 8);
                while (first.hasRemaining()) journal.write(first);
                var started = new CountDownLatch(1);
                result = pool.submit(() -> {
                    started.countDown();
                    return reader.read(read(null, all("pk"), all("secondary")));
                });
                assertTrue(started.await(1, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> result.get(100, TimeUnit.MILLISECONDS));
                ByteBuffer rest = ByteBuffer.wrap(complete, split, complete.length - split);
                while (rest.hasRemaining()) journal.write(rest);
                journal.force(true);
            }
            var visible = result.get(5, TimeUnit.SECONDS);
            assertEquals("a", visible.at());
            assertEquals(List.of(new Entry("01", "aa", "a")), visible.scans().get(0));
            assertEquals(List.of(new Entry("1001", "01", "a")), visible.scans().get(1));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test public void requestValidationPrecedesAnyAppend() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> put("pk", "0", "aa"));
        assertThrows(IllegalArgumentException.class, () -> put("pk", "FF", "aa"));
        assertThrows(IllegalArgumentException.class, () -> put("pk", "01", null));
        assertThrows(IllegalArgumentException.class, () -> new Mutation("pk", "01", "aa", true));
        assertThrows(IllegalArgumentException.class, () -> new Commit("a", "a", List.of()));
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path)) {
            long before = Files.size(path.resolve("journal"));
            var mutations = new ArrayList<Mutation>();
            String value = "aa".repeat(65536);
            for (int i = 0; i < 65; i++) mutations.add(put("pk", "01", value));
            assertThrows(IllegalArgumentException.class, () -> store.commit(new Commit("", "a", mutations)));
            assertEquals(before, Files.size(path.resolve("journal")));
            assertEquals("", store.head().id());
        }
    }
}
