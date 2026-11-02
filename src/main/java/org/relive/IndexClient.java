package org.relive;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/** Clients issue requests only; no shared directory or publisher role is required. */
public final class IndexClient {
    private final URI address;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    public IndexClient(URI address) { this.address = address; }

    public Protocol.Head head() throws IOException, InterruptedException {
        return call("/head", null, Protocol.Head.class);
    }

    public Protocol.Head commit(Protocol.Commit commit) throws IOException, InterruptedException {
        return call("/commit", commit, Protocol.Head.class);
    }

    public Protocol.Results read(Protocol.Read read) throws IOException, InterruptedException {
        return call("/read", read, Protocol.Results.class);
    }

    public static final class Rejected extends IOException {
        private final int status;
        Rejected(int status, String body) {
            super("HTTP " + status + ": " + body);
            this.status = status;
        }
        public int status() { return status; }
    }

    private <T> T call(String path, Object request, Class<T> type) throws IOException, InterruptedException {
        var builder = HttpRequest.newBuilder(address.resolve(path)).timeout(Duration.ofSeconds(15));
        if (request == null) builder.GET();
        else builder.header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(Protocol.JSON.toJson(request)));
        var response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) throw new Rejected(response.statusCode(), response.body());
        return Protocol.JSON.fromJson(response.body(), type);
    }
}
