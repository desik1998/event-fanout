package com.eventfanout.worker;

import com.eventfanout.store.SubscriptionStore;
import com.eventfanout.support.TestDb;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class BatchWorkerTest {

    @TempDir Path temp;
    JdbcTemplate jdbc;
    SubscriptionStore subs;
    MockWebServer server;
    BatchWorker worker;
    ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        jdbc = TestDb.create(temp.resolve("w.db"));
        subs = new SubscriptionStore(jdbc, mapper);
        server = new MockWebServer();
        server.start();
        worker = new BatchWorker(jdbc, mapper, subs, RestClient.create(), "test-worker");
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    void claimsReadyBatchFiltersAndDelivers() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        String hook = server.url("/hook").toString();
        var sub = subs.create("acme", hook, Map.of("types", List.of("order.*"), "sources", List.of("billing")));

        String eventId = "evt-1";
        String batchId = "batch-1";
        Path file = temp.resolve(batchId + ".jsonl");
        Files.writeString(file, eventJson(eventId, "acme", "order.created", "billing", Map.of("status", "paid")));

        insertReadyBatch(batchId, file);
        worker.poll();

        Integer delivered = jdbc.queryForObject(
                "SELECT COUNT(*) FROM deliveries WHERE event_id = ? AND status = 'DELIVERED'",
                Integer.class, eventId
        );
        assertThat(delivered).isEqualTo(1);
        assertThat(server.takeRequest(1, TimeUnit.SECONDS)).isNotNull();
        assertThat(jdbc.queryForObject("SELECT status FROM batches WHERE id = ?", String.class, batchId))
                .isEqualTo("DONE");
        assertThat(sub.get("id")).isNotNull();
    }

    @Test
    void doesNotDeliverAcrossCustomers() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        subs.create("other-cust", server.url("/hook").toString(), Map.of());

        String batchId = "batch-x";
        Path file = temp.resolve(batchId + ".jsonl");
        Files.writeString(file, eventJson("e-x", "acme", "t", "s", Map.of()));
        insertReadyBatch(batchId, file);

        worker.poll();

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM deliveries", Integer.class)).isZero();
        assertThat(server.getRequestCount()).isZero();
    }

    @Test
    void skipsNonMatchingSubscription() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        subs.create("acme", server.url("/hook").toString(), Map.of("sources", List.of("crm")));

        String batchId = "batch-2";
        Path file = temp.resolve(batchId + ".jsonl");
        Files.writeString(file, eventJson("e2", "acme", "order.created", "billing", Map.of()));
        insertReadyBatch(batchId, file);

        worker.poll();

        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM deliveries", Integer.class)).isZero();
        assertThat(server.getRequestCount()).isZero();
    }

    @Test
    void reclaimsStaleProcessingBatch() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        subs.create("acme", server.url("/hook").toString(), Map.of());

        String batchId = "batch-stale";
        Path file = temp.resolve(batchId + ".jsonl");
        Files.writeString(file, eventJson("e3", "acme", "t", "s", Map.of()));

        String staleStart = Instant.now().minus(10, ChronoUnit.SECONDS).toString();
        jdbc.update(
                "INSERT INTO batches(id, path, status, owner_host, started_at, created_at, event_count, size_bytes) VALUES (?,?,?,?,?,?,?,?)",
                batchId, file.toString(), "PROCESSING", "dead-host", staleStart, staleStart, 1, 1
        );

        worker.poll();

        assertThat(jdbc.queryForObject("SELECT owner_host FROM batches WHERE id = ?", String.class, batchId))
                .isEqualTo("test-worker");
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM deliveries WHERE status = 'DELIVERED'", Integer.class
        )).isEqualTo(1);
    }

    @Test
    void marksFailedAfterMaxAttempts() throws Exception {
        for (int i = 0; i < 5; i++) {
            server.enqueue(new MockResponse().setResponseCode(500));
        }
        subs.create("acme", server.url("/hook").toString(), Map.of());

        String batchId = "batch-fail";
        Path file = temp.resolve(batchId + ".jsonl");
        Files.writeString(file, eventJson("e4", "acme", "t", "s", Map.of()));
        insertReadyBatch(batchId, file);

        worker.poll();

        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE event_id = 'e4'", String.class))
                .isEqualTo("FAILED");
        assertThat(jdbc.queryForObject(
                "SELECT COUNT(*) FROM delivery_attempts WHERE delivery_id LIKE 'e4%'", Integer.class
        )).isEqualTo(5);
    }

    private void insertReadyBatch(String batchId, Path file) throws Exception {
        jdbc.update(
                "INSERT INTO batches(id, path, status, created_at, event_count, size_bytes) VALUES (?,?,?,?,?,?)",
                batchId, file.toString(), "READY", Instant.now().toString(), 1, Files.size(file)
        );
    }

    private String eventJson(String id, String customerId, String type, String source, Map<String, Object> payload)
            throws Exception {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("id", id);
        event.put("customerId", customerId);
        event.put("type", type);
        event.put("source", source);
        event.put("payload", payload);
        event.put("createdAt", Instant.now().toString());
        return mapper.writeValueAsString(event) + "\n";
    }
}
