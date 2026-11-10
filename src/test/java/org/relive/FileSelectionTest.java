package org.relive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.relive.Protocol.*;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.*;
import static org.relive.IndexStoreTest.*;

public class FileSelectionTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void irrelevantFilesAreNotOpenedButAffectedQueriesFailClosed() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa")))); store.flush();
            store.commit(new Commit("a", "b", List.of(put("pk", "ff", "bb"), put("other", "01", "cc")))); store.flush();
            var descriptor = Layout.load(path).runs().get(1);
            var file = path.resolve(descriptor.name()); var held = path.resolve("held");
            Files.move(file, held);
            try {
                // The old all-file path fails here; neither this range nor the head ordinal needs that file.
                assertEquals(List.of(new Entry("01", "aa", "a")),
                        store.read(read("b", new Scan("pk", "01", true, "01", true, false, 0))).scans().get(0));
                assertTrue(store.read(read(null, all("absent"))).scans().get(0).isEmpty());
                assertThrows(IOException.class, () -> store.read(read(null, all("pk"))));
            } finally { Files.move(held, file); }
        }
    }

    @Test public void boundsIncludeTombstonesOldVersionsAndAllIndexes() throws Exception {
        var path = temporary.newFolder().toPath();
        try (var store = new IndexStore(path)) {
            store.commit(new Commit("", "a", List.of(put("pk", "01", "aa"), put("pk", "10", "bb"), put("other", "ff", "cc"))));
            store.flush();
            store.commit(new Commit("a", "b", List.of(delete("pk", "01"), put("pk", "80", "dd")))); store.flush();
            var runs = Layout.load(path).runs();
            assertEquals(new Layout.Bounds("01", "10"), runs.get(0).indexes().get("pk"));
            assertEquals(new Layout.Bounds("ff", "ff"), runs.get(0).indexes().get("other"));
            assertEquals(new Layout.Bounds("01", "80"), runs.get(1).indexes().get("pk"));
            for (boolean compact : List.of(false, true)) {
                if (compact) store.compact();
                assertEquals(List.of(new Entry("01", "aa", "a"), new Entry("10", "bb", "a")),
                        store.read(read("a", all("pk"))).scans().get(0));
                var deletedKey = new Scan("pk", "01", true, "01", true, false, 1);
                assertEquals(List.of(new Entry("01", "aa", "a")), store.read(read("a", deletedKey)).scans().get(0));
                assertTrue(store.read(read("b", deletedKey)).scans().get(0).isEmpty());
                assertEquals(List.of(new Entry("10", "bb", "a"), new Entry("80", "dd", "b")),
                        store.read(read("b", all("pk"))).scans().get(0));
                for (boolean reverse : List.of(false, true)) for (boolean inclusive : List.of(false, true)) {
                    var scan = new Scan("pk", "10", inclusive, "80", inclusive, reverse, 0);
                    var expected = !inclusive ? List.of() : reverse
                            ? List.of(new Entry("80", "dd", "b"), new Entry("10", "bb", "a"))
                            : List.of(new Entry("10", "bb", "a"), new Entry("80", "dd", "b"));
                    assertEquals(expected, store.read(read(null, scan)).scans().get(0));
                }
                assertEquals(2, store.read(read(null, all("pk"), all("other"))).scans().get(0).size());
                assertEquals(List.of(new Entry("ff", "cc", "a")), store.read(read(null, all("other"))).scans().get(0));
            }
        }
        try (var reopened = new IndexStore(path)) {
            assertEquals(List.of(new Entry("01", "aa", "a"), new Entry("10", "bb", "a")),
                    reopened.read(read("a", all("pk"))).scans().get(0));
            assertThrows(IndexStore.Conflict.class, () -> reopened.read(read("unknown", all("pk"))));
        }
    }

    @Test public void intervalPruningIsConservativeAtEveryBoundary() {
        var bounds = new Layout.Bounds("10", "80");
        for (String from : List.of("01", "10", "40", "80", "ff"))
            for (String to : List.of("01", "10", "40", "80", "ff"))
                for (boolean fromInclusive : List.of(false, true)) for (boolean toInclusive : List.of(false, true)) {
                    if (from.compareTo(to) > 0) continue;
                    for (int value = 0x10; value <= 0x80; value++) {
                        String key = String.format("%02x", value);
                        boolean inside = (key.compareTo(from) > 0 || key.equals(from) && fromInclusive)
                                && (key.compareTo(to) < 0 || key.equals(to) && toInclusive);
                        if (inside) assertTrue(bounds.overlaps(from, fromInclusive, to, toInclusive));
                    }
                }
        assertFalse(bounds.overlaps("80", false, null, false));
        assertFalse(bounds.overlaps(null, false, "10", false));
        assertTrue(bounds.overlaps(null, false, null, false));
        assertThrows(IllegalArgumentException.class, () -> new Layout.Bounds("80", "10"));
        assertThrows(IllegalArgumentException.class, () -> new Layout.Run("00000000-0000-0000-0000-000000000000.sst", 0, 1, Map.of()));
    }

    @Test public void levelOneFilesMustNotOverlapEvenAtOneKey() {
        var a = new Layout.Run("00000000-0000-0000-0000-000000000001.sst", 1, 1, Map.of("pk", new Layout.Bounds("01", "10")));
        var b = new Layout.Run("00000000-0000-0000-0000-000000000002.sst", 1, 1, Map.of("pk", new Layout.Bounds("10", "20")));
        var c = new Layout.Run("00000000-0000-0000-0000-000000000003.sst", 1, 1, Map.of("pk", new Layout.Bounds("11", "20")));
        var metadata = new Layout.Run("00000000-0000-0000-0000-000000000004.sst", 1, 1, Map.of("", new Layout.Bounds("c1", "c2")));
        var empty = Layout.empty();
        assertThrows(IllegalArgumentException.class, () -> new Layout(1, 8, 0, "", 0, 0, 0, 0, List.of(a, b)));
        var disjoint = new Layout(1, 8, 0, "", 0, 0, 0, 0, List.of(c, metadata, a));
        assertEquals(3, disjoint.runs().size());
        // Level-0 files may overlap anything.
        new Layout(1, 8, 0, "", 0, 0, 0, 0, List.of(a, new Layout.Run(b.name(), 0, 1, b.indexes())));
        assertEquals(0, empty.runs().size());
    }
}
