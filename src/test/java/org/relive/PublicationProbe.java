package org.relive;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Finite synthetic protocol diagnostic, not a Trace workload or saturation benchmark. */
public final class PublicationProbe {
    private final IndexClient[] clients;
    private final MessageDigest identity = MessageDigest.getInstance("SHA-256");
    private final TreeMap<String, Protocol.Entry> expected = new TreeMap<>();
    private String parent = "";
    private long logicalBytes;

    private PublicationProbe(String a, String b) throws Exception {
        clients = new IndexClient[]{new IndexClient(URI.create(a)), new IndexClient(URI.create(b))};
    }

    private static String key(int i) { return String.format("%08x", i); }
    private static String value(int i) { return key(i).repeat(16); }

    private void commit(int writer, String id, List<Protocol.Mutation> changes) throws Exception {
        var commit = new Protocol.Commit(parent, id, changes);
        identity.update(Protocol.JSON.toJson(commit).getBytes(StandardCharsets.UTF_8));
        identity.update((byte)'\n');
        for (var change : changes) logicalBytes += Protocol.JSON.toJson(change).getBytes(StandardCharsets.UTF_8).length;
        var ack = clients[writer].commit(commit);
        if (!id.equals(ack.id())) throw new AssertionError("Wrong commit acknowledgment");
        for (var change : changes) if (change.index().equals("pk")) {
            if (change.delete()) expected.remove(change.key());
            else expected.put(change.key(), new Protocol.Entry(change.key(), change.value(), id));
        }
        parent = id;
    }

    private Protocol.Read query(int key, String at) {
        String lower = key(key), upper = key(Math.min(511, key + 3));
        return new Protocol.Read(at, List.of(
                new Protocol.Scan("pk", lower, true, upper, true, false, 4),
                new Protocol.Scan("secondary", lower, true, upper, true, false, 4)));
    }

    static void check(Protocol.Results actual, String at, List<Protocol.Entry> rows) {
        if (!actual.at().equals(at) || !actual.scans().equals(List.of(rows, rows)))
            throw new AssertionError("Incorrect query result at " + at);
    }

    private static Object control(String url, String path, boolean post) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(url + path)).timeout(Duration.ofSeconds(30));
        if (post) request.POST(HttpRequest.BodyPublishers.noBody());
        var response = HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) throw new AssertionError(response.body());
        return Protocol.JSON.fromJson(response.body(), Object.class);
    }

    private static Map<String, Double> latency(long[] values) {
        Arrays.sort(values);
        var result = new LinkedHashMap<String, Double>();
        result.put("p50_ms", values[(values.length - 1) / 2] / 1e6);
        result.put("p95_ms", values[(int)Math.ceil(values.length * .95) - 1] / 1e6);
        result.put("p99_ms", values[(int)Math.ceil(values.length * .99) - 1] / 1e6);
        return result;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) throw new IllegalArgumentException("URL_A URL_B OUTPUT MAINTENANCE_BOOLEAN");
        var probe = new PublicationProbe(args[0], args[1]);
        Path output = Path.of(args[2]);
        if (Files.exists(output)) throw new IllegalArgumentException("Output already exists");
        boolean maintenance = Boolean.parseBoolean(args[3]);
        var report = new LinkedHashMap<String, Object>();
        long started = System.nanoTime();
        for (int batch = 0; batch < 16; batch++) {
            var changes = new ArrayList<Protocol.Mutation>();
            for (int i = batch * 32; i < batch * 32 + 32; i++)
                for (String index : List.of("pk", "secondary"))
                    changes.add(new Protocol.Mutation(index, key(i), value(i), false));
            probe.commit(batch & 1, "seed-" + batch, changes);
        }
        report.put("load_seconds", (System.nanoTime() - started) / 1e9);
        report.put("load_logical_bytes", probe.logicalBytes);
        var initial = new TreeMap<>(probe.expected);
        if (maintenance) {
            control(args[0], "/flush", true);
            control(args[0], "/compact", true);
            report.put("loaded_stats", control(args[0], "/stats", false));
        }
        long beforeBytes = probe.logicalBytes;
        long[] commits = new long[512], reads = new long[512];
        started = System.nanoTime();
        for (int i = 0; i < 512; i++) {
            int selected = (i * 37) % 512;
            boolean delete = i % 37 == 0;
            var changes = new ArrayList<Protocol.Mutation>();
            for (String index : List.of("pk", "secondary"))
                changes.add(new Protocol.Mutation(index, key(selected), delete ? null : value(i + 512), delete));
            long begin = System.nanoTime();
            probe.commit(i & 1, "change-" + i, changes);
            commits[i] = System.nanoTime() - begin;
            var query = probe.query(selected, null);
            begin = System.nanoTime();
            var result = probe.clients[1 - (i & 1)].read(query);
            reads[i] = System.nanoTime() - begin;
            check(result, probe.parent, new ArrayList<>(probe.expected.subMap(key(selected), true, key(Math.min(511, selected + 3)), true).values()));
        }
        double seconds = (System.nanoTime() - started) / 1e9;
        report.put("work_seconds", seconds);
        report.put("commits_per_second", 512 / seconds);
        report.put("commit", latency(commits));
        report.put("read", latency(reads));
        report.put("work_logical_bytes", probe.logicalBytes - beforeBytes);
        if (maintenance) {
            report.put("before_drain_stats", control(args[0], "/stats", false));
            long drain = System.nanoTime();
            control(args[0], "/flush", true);
            control(args[0], "/compact", true);
            report.put("drain_seconds", (System.nanoTime() - drain) / 1e9);
            report.put("drained_stats", control(args[0], "/stats", false));
        }
        for (int i = 0; i < 512; i += 4) {
            var query = probe.query(i, "seed-15");
            check(probe.clients[i % 2].read(query), "seed-15", new ArrayList<>(initial.subMap(key(i), true, key(i + 3), true).values()));
            check(probe.clients[1].read(probe.query(i, null)), probe.parent,
                    new ArrayList<>(probe.expected.subMap(key(i), true, key(i + 3), true).values()));
        }
        report.put("input_sha256", HexFormat.of().formatHex(probe.identity.digest()));
        report.put("correctness", "all immediate reads and complete initial/final state passed");
        report.put("workload", "512 keys, two indexes, 512 commits; serial closed-loop clients alternating publishers");
        Files.writeString(output, Protocol.JSON.toJson(report) + "\n");
    }
}
