package org.relive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.relive.Protocol.Commit;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.relive.IndexStoreTest.*;

public class IndexServerTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();

    @Test(timeout = 45000) public void twoProcessesPublishAndImmediatelyObserveEachOtherThenRestart() throws Exception {
        var path = temporary.newFolder("shared").toPath();
        try (var first = start(path); var second = start(path)) {
            String parent = "";
            for (int i = 1; i <= 12; i++) {
                var writer = (i & 1) == 0 ? first.client : second.client;
                var reader = (i & 1) == 0 ? second.client : first.client;
                String id = "source-" + (1000 - i * 7);
                String value = String.format("%02x", i);
                writer.commit(new Commit(parent, id, List.of(put("pk", "01", value), put("secondary", "a001", value))));
                var result = reader.read(read(null, all("pk"), all("secondary")));
                assertEquals(id, result.at());
                assertEquals(value, result.scans().get(0).get(0).value());
                assertEquals(value, result.scans().get(1).get(0).value());
                parent = id;
            }
            first.process.destroyForcibly();
            assertTrue(first.process.waitFor(5, TimeUnit.SECONDS));
            assertEquals(parent, second.client.head().id());
            try (var reopened = start(path)) {
                assertEquals(parent, reopened.client.head().id());
                assertEquals("01", reopened.client.read(read("source-993", all("pk"))).scans().get(0).get(0).value());
                reopened.client.commit(new Commit(parent, "after-restart", List.of(delete("pk", "01"), delete("secondary", "a001"))));
                var result = second.client.read(read(null, all("pk"), all("secondary")));
                assertTrue(result.scans().get(0).isEmpty());
                assertTrue(result.scans().get(1).isEmpty());
            }
        }
    }

    @Test(timeout = 30000) public void conflictingProcessesReturnOneSuccessAndOneConflict() throws Exception {
        var path = temporary.newFolder("shared").toPath();
        var pool = Executors.newFixedThreadPool(2);
        try (var first = start(path); var second = start(path)) {
            var gate = new CountDownLatch(1);
            var a = pool.submit(() -> claim(first.client, gate, "a", "01"));
            var b = pool.submit(() -> claim(second.client, gate, "b", "02"));
            gate.countDown();
            var statuses = List.of(a.get(10, TimeUnit.SECONDS), b.get(10, TimeUnit.SECONDS));
            assertTrue(statuses.contains(200));
            assertTrue(statuses.contains(409));
            var before = first.client.read(read(null, all("pk")));
            assertEquals(1, before.scans().get(0).size());
            assertEquals(before, second.client.read(read(null, all("pk"))));
            String winner = before.at();
            String key = winner.equals("a") ? "01" : "02";
            second.client.commit(new Commit("", winner, List.of(put("pk", key, "aa"))));
            assertEquals(before, first.client.read(read(null, all("pk"))));
            var wrong = assertThrows(IndexClient.Rejected.class,
                    () -> first.client.commit(new Commit("", winner, List.of(put("pk", key, "bb")))));
            assertEquals(409, wrong.status());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test(timeout = 30000) public void invalidRequestCannotPartiallyPublish() throws Exception {
        try (var service = start(temporary.newFolder("shared").toPath())) {
            var http = HttpClient.newHttpClient();
            for (String body : List.of(
                    "{\"parent\":\"\",\"id\":\"bad\",\"changes\":[{\"index\":\"pk\",\"key\":\"01\",\"value\":\"aa\"},{\"index\":\"pk\",\"key\":\"02\"}]}",
                    "null", "{}", "{broken", "{\"parent\":\"\",\"id\":\"bad\",\"changes\":[null]}")) {
                var request = HttpRequest.newBuilder(URI.create(service.address + "/commit"))
                        .timeout(Duration.ofSeconds(5)).POST(HttpRequest.BodyPublishers.ofString(body)).build();
                var response = http.send(request, HttpResponse.BodyHandlers.ofString());
                assertEquals(response.body(), 400, response.statusCode());
                assertEquals("", service.client.head().id());
            }
            service.client.commit(new Commit("", "good", List.of(put("pk", "01", "aa"))));
            assertEquals("aa", service.client.read(read(null, all("pk"))).scans().get(0).get(0).value());
        }
    }

    @Test(timeout = 30000) public void independentServersRefreshCompactedLayouts() throws Exception {
        var path = temporary.newFolder("shared").toPath();
        try (var first = start(path); var second = start(path)) {
            first.client.commit(new Commit("", "a", List.of(put("pk", "01", "aa"))));
            maintenance(first.address, "/flush");
            second.client.commit(new Commit("a", "b", List.of(put("pk", "02", "bb"))));
            maintenance(second.address, "/flush");
            maintenance(first.address, "/compact");
            assertEquals(List.of(new Protocol.Entry("01", "aa", "a"), new Protocol.Entry("02", "bb", "b")),
                    second.client.read(read(null, all("pk"))).scans().get(0));
            first.process.destroyForcibly();
            assertTrue(first.process.waitFor(5, TimeUnit.SECONDS));
            try (var reopened = start(path)) {
                assertEquals("a", reopened.client.read(read("a", all("pk"))).at());
                reopened.client.commit(new Commit("b", "c", List.of(delete("pk", "01"))));
                assertEquals(List.of(new Protocol.Entry("02", "bb", "b")), second.client.read(read(null, all("pk"))).scans().get(0));
            }
        }
    }

    private static void maintenance(String address, String path) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(address + path)).timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody()).build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(response.body(), 200, response.statusCode());
    }

    @Test(timeout = 45000) public void stagedTransactionSurvivesPublisherExitAndPublishesThroughAnotherProcess() throws Exception {
        var directory = temporary.newFolder().toPath();
        try (var a = start(directory); var b = start(directory)) {
            a.client.commit(new Commit("", "base", List.of(put("pk", "01", "aa"))));
            post(a.address, "/stage", new Protocol.Stage("tx", 1, "next", List.of(put("pk", "01", "bb"))));
            a.process.destroyForcibly();
            assertTrue(a.process.waitFor(5, TimeUnit.SECONDS));
            assertEquals("aa", b.client.read(read(null, all("pk"))).scans().get(0).get(0).value());
            assertEquals("bb", b.client.read(new Protocol.Read("base", List.of(all("pk")), "tx", 2, "next", "none"))
                    .scans().get(0).get(0).value());
            post(b.address, "/publish", new Protocol.Publication("tx", "base", "next", 2, 1));
            try (var c = start(directory)) {
                assertEquals("bb", c.client.read(read(null, all("pk"))).scans().get(0).get(0).value());
                post(c.address, "/stage", new Protocol.Stage("other", 1, "last", List.of(delete("pk", "01"))));
                post(c.address, "/publish", new Protocol.Publication("other", "next", "last", 1, 1));
                assertTrue(b.client.read(read(null, all("pk"))).scans().get(0).isEmpty());
            }
        }
    }

    @Test(timeout = 60000) public void streamingReturnsMoreThanTenThousandRowsAndEightMiB() throws Exception {
        try (var server = new IndexServer(temporary.newFolder().toPath(), 0)) {
            var client = new IndexClient(URI.create(server.address()));
            for (int batch = 0; batch < 2; batch++) {
                var changes = new java.util.ArrayList<Protocol.Mutation>();
                for (int i = batch * 6000; i < (batch + 1) * 6000; i++)
                    changes.add(put("pk", String.format("%08x", i), "aa".repeat(400)));
                client.commit(new Commit(batch == 0 ? "" : "first", batch == 0 ? "first" : "last", changes));
            }
            var scan = new Protocol.Scan("pk", null, false, null, false, false, 0);
            assertEquals(400, assertThrows(IndexClient.Rejected.class, () -> client.read(read(null, scan))).status());
            var body = post(server.address(), "/stream", read(null, scan));
            assertTrue(body.length() > Protocol.MAX_BYTES);
            var lines = body.lines().toList();
            assertEquals(12002, lines.size());
            for (int i = 0; i < 12000; i++) {
                var frame = com.google.gson.JsonParser.parseString(lines.get(i + 1)).getAsJsonObject();
                assertEquals(String.format("%08x", i), frame.getAsJsonObject("entry").get("key").getAsString());
            }
            var done = com.google.gson.JsonParser.parseString(lines.get(12001)).getAsJsonObject();
            assertTrue(done.get("done").getAsBoolean());
            assertEquals(12000, done.getAsJsonArray("counts").get(0).getAsLong());
            assertEquals("last", client.head().id());
        }
    }

    private static String post(String address, String path, Object body) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(address + path)).timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(Protocol.JSON.toJson(body))).build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(response.body(), 200, response.statusCode());
        return response.body();
    }

    private static int claim(IndexClient client, CountDownLatch gate, String id, String key) throws Exception {
        gate.await();
        try {
            client.commit(new Commit("", id, List.of(put("pk", key, "aa"))));
            return 200;
        } catch (IndexClient.Rejected rejected) {
            return rejected.status();
        }
    }

    private Running start(Path directory) throws Exception {
        Path output = temporary.newFile().toPath();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, IndexServer.class.getName(), directory.toString(), "0")
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (process.isAlive() && System.nanoTime() < deadline) {
            for (String line : Files.readAllLines(output)) {
                if (line.startsWith("LISTENING ")) return new Running(process, line.substring(10));
            }
            Thread.sleep(10);
        }
        process.destroyForcibly();
        process.waitFor(5, TimeUnit.SECONDS);
        throw new IOException("Service failed to start: " + Files.readString(output));
    }

    private static final class Running implements AutoCloseable {
        final Process process;
        final String address;
        final IndexClient client;
        Running(Process process, String address) {
            this.process = process;
            this.address = address;
            client = new IndexClient(URI.create(address));
        }
        @Override public void close() throws Exception {
            process.destroy();
            if (!process.waitFor(3, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertTrue("Service did not terminate", process.waitFor(3, TimeUnit.SECONDS));
            }
        }
    }
}
