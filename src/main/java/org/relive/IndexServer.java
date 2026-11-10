package org.relive;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.BufferedOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Loopback-only HTTP service for the single-machine multi-publisher prototype. */
public final class IndexServer implements AutoCloseable {
    private final IndexStore store;
    private final HttpServer server;
    private static final int WORKERS = Integer.getInteger("relive.http.workers", 4);
    private final ThreadPoolExecutor workers = new ThreadPoolExecutor(WORKERS, WORKERS, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(64));

    public IndexServer(Path directory, int port) throws IOException {
        store = new IndexStore(directory);
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 64);
            server.setExecutor(workers);
            server.createContext("/", this::handle);
            server.start();
        } catch (IOException | RuntimeException error) {
            store.close();
            workers.shutdownNow();
            throw error;
        }
    }

    public String address() { return "http://127.0.0.1:" + server.getAddress().getPort(); }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            Object result;
            int status = 200;
            try {
                String path = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();
                if (path.equals("/head") || path.equals("/stats") || path.equals("/describe")) {
                    if (!method.equals("GET")) {
                        respond(exchange, 405, new Protocol.Error("Use GET"));
                        return;
                    }
                    result = switch (path) {
                        case "/head" -> store.head();
                        case "/stats" -> store.stats();
                        default -> java.util.Map.of("name", "relive-local", "transactionLocalReads", true,
                                "streamingReads", true, "searchGroups", true, "completeCommitPublication", true,
                                "durability", "local_fsync", "maxCommitChanges", 10000);
                    };
                } else if (path.equals("/flush") || path.equals("/compact")) {
                    if (!method.equals("POST")) {
                        respond(exchange, 405, new Protocol.Error("Use POST"));
                        return;
                    }
                    result = path.equals("/flush") ? store.flush() : store.compact();
                } else if (java.util.List.of("/commit", "/read", "/stream", "/stage", "/publish", "/bind").contains(path)) {
                    if (!method.equals("POST")) {
                        respond(exchange, 405, new Protocol.Error("Use POST"));
                        return;
                    }
                    byte[] bytes = exchange.getRequestBody().readNBytes(Protocol.MAX_BYTES + 1);
                    if (bytes.length > Protocol.MAX_BYTES) {
                        respond(exchange, 413, new Protocol.Error("Request exceeds 8 MiB"));
                        return;
                    }
                    String json = StandardCharsets.UTF_8.newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
                    if (path.equals("/stream")) {
                        stream(exchange, decode(json, Protocol.Read.class));
                        return;
                    }
                    result = switch (path) {
                        case "/commit" -> store.commit(decode(json, Protocol.Commit.class));
                        case "/stage" -> store.stage(decode(json, Protocol.Stage.class));
                        case "/publish" -> store.publish(decode(json, Protocol.Publication.class));
                        case "/bind" -> store.bind(decode(json, Protocol.Binding.class));
                        default -> store.read(decode(json, Protocol.Read.class));
                    };
                } else {
                    respond(exchange, 404, new Protocol.Error("Unknown endpoint"));
                    return;
                }
            } catch (IndexStore.Conflict conflict) {
                status = 409;
                result = new Protocol.Error(conflict.getMessage());
            } catch (IllegalArgumentException
                     | java.nio.charset.CharacterCodingException invalid) {
                status = 400;
                result = new Protocol.Error("Invalid request: " + invalid.getMessage());
            } catch (IOException unavailable) {
                status = 503;
                result = new Protocol.Error("Storage unavailable; commit outcome may be unknown: " + unavailable.getMessage());
            }
            respond(exchange, status, result);
        }
    }

    private void stream(HttpExchange exchange, Protocol.Read request) throws IOException {
        boolean[] started = {false};
        long[][] counts = {null};
        var output = new BufferedOutputStream(exchange.getResponseBody(), 65536);
        java.util.function.Consumer<Object> line = value -> {
            try {
                output.write(Protocol.JSON.toJson(value).getBytes(StandardCharsets.UTF_8));
                output.write('\n');
            } catch (IOException error) { throw new UncheckedIOException(error); }
        };
        try {
            store.stream(request, new IndexStore.ReadSink() {
                public void begin(String at, int scans) {
                    try {
                        exchange.getResponseHeaders().set("Content-Type", "application/x-ndjson; charset=utf-8");
                        exchange.sendResponseHeaders(200, 0);
                        started[0] = true;
                        counts[0] = new long[scans];
                        line.accept(java.util.Map.of("at", at, "scans", scans));
                    } catch (IOException error) { throw new UncheckedIOException(error); }
                }
                public void entry(int scan, Protocol.Entry entry) {
                    line.accept(java.util.Map.of("scan", scan, "entry", entry));
                    counts[0][scan]++;
                }
            });
            line.accept(java.util.Map.of("done", true, "counts", counts[0]));
            output.flush();
        } catch (UncheckedIOException disconnected) {
            // The missing terminal frame makes a disconnected query fail at the client.
        } catch (IOException | RuntimeException error) {
            if (!started[0]) throw error;
            // Do not turn partial output into a successful, truncated result.
            System.err.println("Relive stream failed before completion: " + error);
        }
    }

    private static <T> T decode(String json, Class<T> type) {
        try {
            return Protocol.JSON.fromJson(json, type);
        } catch (RuntimeException invalid) {
            // Gson wraps record constructor validation failures in RuntimeException.
            throw new IllegalArgumentException("Malformed or invalid " + type.getSimpleName(), invalid);
        }
    }

    private static void respond(HttpExchange exchange, int status, Object result) throws IOException {
        byte[] bytes = Protocol.JSON.toJson(result).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
    }

    @Override
    public void close() throws IOException {
        server.stop(0);
        workers.shutdownNow();
        store.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("Usage: java -jar relive.jar DIRECTORY PORT");
        var service = new IndexServer(Path.of(args[0]), Integer.parseInt(args[1]));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                service.close();
            } catch (IOException error) {
                System.err.println("Relive shutdown failed: " + error.getMessage());
            }
        }));
        System.out.println("LISTENING " + service.address());
    }
}
