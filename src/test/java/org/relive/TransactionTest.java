package org.relive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import static org.junit.Assert.*;
import static org.relive.IndexStoreTest.*;
import org.relive.Protocol.*;

public class TransactionTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test public void stagedWritesArePrivateAndPublicationIsCompleteAndIdempotentAfterCompaction() throws Exception {
        var directory = temporary.newFolder().toPath();
        var publication = new Publication("tx", "base", "next", 4, 3);
        try (var a = new IndexStore(directory, 1024); var b = new IndexStore(directory, 1024)) {
            a.commit(new Commit("", "base", List.of(put("pk", "01", "aa"))));
            a.stage(new Stage("tx", 1, "next", List.of(put("pk", "01", "bb"))));
            assertEquals("aa", b.read(read(null, all("pk"))).scans().get(0).get(0).value());
            b.stage(new Stage("tx", 2, "next", List.of(delete("pk", "01"))));
            assertTrue(a.read(new Read("base", List.of(all("pk")), "tx", 3, "next", "none")).scans().get(0).isEmpty());
            a.stage(new Stage("tx", 4, "next", List.of(put("pk", "01", "cc"))));
            assertThrows(IndexStore.Conflict.class, () -> b.stage(new Stage("tx", 4, "next", List.of(put("pk", "02", "dd")))));
            assertThrows(IndexStore.Conflict.class, () -> b.publish(new Publication("tx", "base", "next", 4, 2)));
            assertEquals("base", a.head().id());
            assertEquals("next", b.publish(publication).id());
            assertEquals("cc", a.read(read(null, all("pk"))).scans().get(0).get(0).value());
            assertEquals("aa", a.read(read("base", all("pk"))).scans().get(0).get(0).value());
            assertEquals(0, b.stats().activeTransactions());
            a.flush(); b.compact();
        }
        try (var reopened = new IndexStore(directory)) {
            long bytes = reopened.stats().walBytes();
            assertEquals("next", reopened.publish(publication).id());
            assertEquals(bytes, reopened.stats().walBytes());
            assertThrows(IndexStore.Conflict.class, () -> reopened.publish(new Publication("tx", "base", "different", 4, 3)));
            assertThrows(IndexStore.Conflict.class, () -> reopened.stage(new Stage("tx", 1, "different", List.of(put("pk", "02", "dd")))));
        }
    }

    @Test public void groupsUseRowIdentityAndLocalOverlayAtOneReadPoint() throws Exception {
        try (var store = new IndexStore(temporary.newFolder().toPath(), 1024)) {
            store.commit(new Commit("", "base", List.of(
                    new Mutation("a", "1001", "aa", false, "01"), new Mutation("a", "1002", "bb", false, "02"),
                    new Mutation("b", "2002", "bb", false, "02"), new Mutation("b", "2003", "cc", false, "03"))));
            var scans = List.of(all("a"), all("b"));
            assertEquals(List.of("01", "02", "03"), store.read(new Read("base", scans, null, 0, null, "union"))
                    .scans().get(0).stream().map(Entry::identity).toList());
            assertEquals(List.of("02"), store.read(new Read("base", scans, null, 0, null, "intersection"))
                    .scans().get(0).stream().map(Entry::identity).toList());
            store.stage(new Stage("tx", 1, "next", List.of(new Mutation("b", "2002", null, true, "02"))));
            assertTrue(store.read(new Read("base", scans, "tx", 2, "next", "intersection")).scans().get(0).isEmpty());
        }
    }

    @Test public void bindingAndCorruptStagingFailClosed() throws Exception {
        var directory = temporary.newFolder().toPath();
        try (var a = new IndexStore(directory); var b = new IndexStore(directory)) {
            assertEquals(a.bind(new Binding("input-a")).identity(), b.bind(new Binding("input-a")).identity());
            assertThrows(IndexStore.Conflict.class, () -> b.bind(new Binding("input-b")));
            a.stage(new Stage("tx", 1, "next", List.of(put("pk", "01", "aa"))));
            var path = directory.resolve("transactions/t" + Layout.hash("tx") + ".json");
            Files.writeString(path, Files.readString(path).replace("aa", "bb"));
            assertThrows(IOException.class, () -> b.publish(new Publication("tx", "", "next", 1, 1)));
            assertEquals("", a.head().id());
        }
    }
}
