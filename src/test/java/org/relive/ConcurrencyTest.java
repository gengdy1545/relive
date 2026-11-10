package org.relive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.relive.Protocol.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.relive.IndexStoreTest.*;

public class ConcurrencyTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    private static Throwable failure(CompletableFuture<Head> result) throws InterruptedException {
        try {
            result.get();
            return null;
        } catch (ExecutionException failed) {
            return failed.getCause();
        }
    }

    @Test public void pipelinedChainFailsFromItsFirstRejectedCommit() throws Exception {
        try (var store = new IndexStore(temporary.newFolder().toPath())) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            var wrongParent = store.commitAsync(new Commit("missing", "b", List.of(put("pk", "02", "bb"))));
            var dependent = store.commitAsync(new Commit("b", "c", List.of(put("pk", "03", "cc"))));
            var further = store.commitAsync(new Commit("c", "d", List.of(put("pk", "04", "dd"))));
            for (var result : List.of(wrongParent, dependent, further))
                assertTrue(failure(result) instanceof IndexStore.Conflict);
            assertEquals("a", store.head().id());

            var first = new Commit("a", "b", List.of(put("pk", "02", "bb")));
            var results = List.of(store.commitAsync(first), store.commitAsync(first),
                    store.commitAsync(new Commit("a", "b", List.of(put("pk", "02", "ee")))),
                    store.commitAsync(new Commit("b", "c", List.of(put("pk", "03", "cc")))));
            assertNull(failure(results.get(0)));
            assertNull(failure(results.get(1)));
            assertTrue(failure(results.get(2)) instanceof IndexStore.Conflict);
            assertNull(failure(results.get(3)));
            assertEquals(List.of(new Entry("01", "aa", "a"), new Entry("02", "bb", "b"), new Entry("03", "cc", "c")),
                    store.read(read(null, all("pk"))).scans().get(0));
        }
    }

    /**
     * Each commit i writes keys i and 0x8000+i; acknowledged commits are visible, also through a second
     * handle that must notice the other handle's frames, and commits are never split.
     */
    @Test(timeout = 60000) public void readsSeeAcknowledgedCommitsWholeDuringFlushAndCompaction() throws Exception {
        var path = temporary.newFolder().toPath();
        int commits = 1500;
        var acknowledged = new AtomicInteger();
        var stop = new AtomicBoolean();
        var executor = Executors.newFixedThreadPool(6);
        try (var store = new IndexStore(path, 16 * 1024); var other = new IndexStore(path, 16 * 1024)) {
            var readers = new ArrayList<java.util.concurrent.Future<Integer>>();
            for (int r = 0; r < 4; r++) {
                var handle = r % 2 == 0 ? store : other;
                readers.add(executor.submit(() -> {
                int reads = 0;
                while (!stop.get()) {
                    int floor = acknowledged.get();
                    var rows = handle.read(read(null, new Scan("pk", null, false, null, false, false, 0))).scans().get(0);
                    int low = 0, high = 0;
                    for (var entry : rows) {
                        int key = Integer.parseInt(entry.key(), 16);
                        if (key < 0x8000) { assertEquals(++low, key); assertEquals(entry.key(), entry.value()); }
                        else assertEquals(0x8000 + ++high, key);
                    }
                    assertEquals("a commit was split", low, high);
                    assertTrue("acknowledged commit " + floor + " missing, saw " + low, low >= floor);
                    reads++;
                }
                return reads;
                }));
            }
            var compactor = executor.submit(() -> {
                while (!stop.get()) { store.compact(); Thread.sleep(5); }
                return null;
            });
            var submitted = new ArrayList<CompletableFuture<Head>>();
            String parent = "";
            for (int i = 1; i <= commits; i++) {
                String id = "c" + i, low = String.format("%04x", i);
                int number = i;
                var result = store.commitAsync(new Commit(parent, id,
                        List.of(put("pk", low, low), put("pk", String.format("%04x", 0x8000 + i), low))));
                result.thenRun(() -> acknowledged.accumulateAndGet(number, Math::max));
                submitted.add(result);
                parent = id;
            }
            for (var result : submitted) result.get(30, TimeUnit.SECONDS);
            Thread.sleep(50);
            stop.set(true);
            int reads = 0;
            for (var reader : readers) reads += reader.get(30, TimeUnit.SECONDS);
            compactor.get(30, TimeUnit.SECONDS);
            assertTrue(reads > 0);
            assertEquals(2 * commits, store.read(read(null, new Scan("pk", null, false, null, false, false, 0))).scans().get(0).size());
            assertTrue(store.stats().flushBytes() > 0);
            assertTrue(store.stats().compactionWriteBytes() > 0);
        } finally {
            stop.set(true);
            executor.shutdownNow();
        }
        try (var reopened = new IndexStore(path, 16 * 1024)) {
            assertEquals("c" + commits, reopened.head().id());
        }
    }
}
