package com.eventfanout.replay;

import com.eventfanout.store.SubscriptionStore;
import com.eventfanout.support.TestDb;
import com.eventfanout.worker.BatchWorker;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestClient;
import org.springframework.web.server.ResponseStatusException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReplayServiceTest {

    @TempDir Path temp;
    JdbcTemplate jdbc;
    SubscriptionStore subs;
    ReplayService replay;
    BatchWorker worker;
    MockWebServer server;
    ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws Exception {
        jdbc = TestDb.create(temp.resolve("r.db"));
        subs = new SubscriptionStore(jdbc, mapper);
        replay = new ReplayService(jdbc, mapper, subs);
        server = new MockWebServer();
        server.start();
        worker = new BatchWorker(jdbc, mapper, subs, RestClient.create(), "test-worker");
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    void replayResetsDeliveryAndWorkerDeliversAgain() throws Exception {
        server.enqueue(new MockResponse().setResponseCode(200));
        server.enqueue(new MockResponse().setResponseCode(200));
        var sub = subs.create("acme", server.url("/hook").toString(),
                Map.of("types", List.of("order.*"), "sources", List.of("billing")));

        String eventId = "evt-replay-1";
        String batchId = "batch-r1";
        Path file = temp.resolve(batchId + ".jsonl");
        Files.writeString(file, eventJson(eventId, "acme", "order.created", "billing", Map.of("n", 1)));
        insertBatch(batchId, file, "DONE");

        // seed a prior DELIVERED row
        String deliveryId = eventId + "__" + sub.get("id");
        String now = Instant.now().toString();
        jdbc.update(
                """
                INSERT INTO deliveries
                (id, batch_id, event_id, subscription_id, status, attempt_count, webhook_url, created_at, updated_at)
                VALUES (?,?,?,?, 'DELIVERED', 1, ?, ?, ?)
                """,
                deliveryId, batchId, eventId, sub.get("id"), server.url("/hook").toString(), now, now
        );

        Map<String, Object> result = replay.replay("acme", eventId, null);
        assertThat(result.get("batchId")).isEqualTo(batchId);
        assertThat(jdbc.queryForObject("SELECT status FROM batches WHERE id = ?", String.class, batchId))
                .isEqualTo("READY");
        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?", String.class, deliveryId))
                .isEqualTo("PENDING");

        worker.poll();

        assertThat(jdbc.queryForObject("SELECT status FROM deliveries WHERE id = ?", String.class, deliveryId))
                .isEqualTo("DELIVERED");
        assertThat(server.getRequestCount()).isEqualTo(1);
        assertThat(server.takeRequest(1, TimeUnit.SECONDS)).isNotNull();
    }

    @Test
    void replayRejectsOtherTenant() throws Exception {
        String eventId = "evt-x";
        String batchId = "batch-x";
        Path file = temp.resolve(batchId + ".jsonl");
        Files.writeString(file, eventJson(eventId, "acme", "order.created", "billing", Map.of()));
        insertBatch(batchId, file, "DONE");

        assertThatThrownBy(() -> replay.replay("globex", eventId, null))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("404");
    }

    @Test
    void replayUnknownEventIsNotFound() {
        assertThatThrownBy(() -> replay.replay("acme", "missing", null))
                .isInstanceOf(ResponseStatusException.class)
                .hasMessageContaining("404");
    }

    private void insertBatch(String batchId, Path file, String status) {
        jdbc.update(
                "INSERT INTO batches(id, path, status, created_at, event_count, size_bytes) VALUES (?,?,?,?,?,?)",
                batchId, file.toString(), status, Instant.now().toString(), 1, 1
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
        return mapper.writeValueAsString(event);
    }
}
