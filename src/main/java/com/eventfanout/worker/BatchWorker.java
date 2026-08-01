package com.eventfanout.worker;

import com.eventfanout.match.FilterMatcher;
import com.eventfanout.store.SubscriptionStore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pulls READY (or stale PROCESSING) batches from SQLite, filters events,
 * delivers to webhooks, tracks per-subscriber progress.
 */
@Component
public class BatchWorker {

    private static final int MAX_FILES = 10;
    private static final long STALE_SECONDS = 5;
    private static final int MAX_ATTEMPTS = 5;

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final SubscriptionStore subscriptions;
    private final RestClient http;
    private final String workerId;

    public BatchWorker(
            JdbcTemplate jdbc,
            ObjectMapper mapper,
            SubscriptionStore subscriptions,
            RestClient restClient,
            @Value("${WORKER_ID:worker-1}") String workerId
    ) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.subscriptions = subscriptions;
        this.http = restClient;
        this.workerId = workerId;
    }

    @Scheduled(fixedDelay = 500)
    public void poll() {
        String staleBefore = Instant.now().minus(STALE_SECONDS, ChronoUnit.SECONDS).toString();
        List<Map<String, Object>> candidates = jdbc.query(
                """
                SELECT id, path, status FROM batches
                WHERE status = 'READY'
                   OR (status = 'PROCESSING' AND started_at < ?)
                ORDER BY created_at
                LIMIT ?
                """,
                (rs, i) -> {
                    Map<String, Object> m = new HashMap<>();
                    m.put("id", rs.getString("id"));
                    m.put("path", rs.getString("path"));
                    m.put("status", rs.getString("status"));
                    return m;
                },
                staleBefore, MAX_FILES
        );

        for (Map<String, Object> batch : candidates) {
            String batchId = (String) batch.get("id");
            if (!claim(batchId, staleBefore)) {
                continue;
            }
            try {
                processBatch(batchId, (String) batch.get("path"));
                if (pendingCount(batchId) == 0) {
                    markDone(batchId);
                }
                // else leave PROCESSING — reclaim after 5s if this host dies
            } catch (Exception ex) {
                System.err.println("[BatchWorker] batch " + batchId + " failed: " + ex.getMessage());
            }
        }
    }

    private boolean claim(String batchId, String staleBefore) {
        String now = Instant.now().toString();
        int updated = jdbc.update(
                """
                UPDATE batches
                SET status = 'PROCESSING', owner_host = ?, started_at = ?
                WHERE id = ?
                  AND (status = 'READY'
                       OR (status = 'PROCESSING' AND started_at < ?))
                """,
                workerId, now, batchId, staleBefore
        );
        return updated == 1;
    }

    private void markDone(String batchId) {
        jdbc.update(
                "UPDATE batches SET status = 'DONE', completed_at = ? WHERE id = ?",
                Instant.now().toString(), batchId
        );
    }

    private void processBatch(String batchId, String path) throws Exception {
        Path file = Path.of(path);
        if (!Files.exists(file)) {
            throw new IllegalStateException("batch file missing: " + path);
        }

        List<Map<String, Object>> subs = subscriptions.listAll();
        List<String> lines = Files.readAllLines(file);

        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            Map<String, Object> event = mapper.readValue(line, new TypeReference<>() {
            });
            String eventId = String.valueOf(event.get("id"));
            String eventCustomer = event.get("customerId") == null ? "" : String.valueOf(event.get("customerId"));
            String type = String.valueOf(event.get("type"));
            String source = String.valueOf(event.get("source"));
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = event.get("payload") instanceof Map<?, ?> p
                    ? (Map<String, Object>) p
                    : Map.of();

            for (Map<String, Object> sub : subs) {
                String subCustomer = sub.get("customerId") == null ? "" : String.valueOf(sub.get("customerId"));
                // Multi-tenant isolation: only deliver within the same customer.
                if (!eventCustomer.equals(subCustomer)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> filter = (Map<String, Object>) sub.get("filter");
                if (!FilterMatcher.matches(filter, type, source, payload)) {
                    continue;
                }
                String subId = (String) sub.get("id");
                String url = (String) sub.get("url");
                String deliveryId = eventId + "__" + subId;
                ensureDelivery(deliveryId, batchId, eventId, subId, url);
            }
        }

        // Retry until terminal or attempts exhausted (up to MAX_ATTEMPTS rounds).
        for (int round = 0; round < MAX_ATTEMPTS; round++) {
            List<Map<String, Object>> pending = loadPending(batchId);
            if (pending.isEmpty()) {
                return;
            }
            for (Map<String, Object> d : pending) {
                deliverOne(d, file);
            }
        }
    }

    private int pendingCount(String batchId) {
        Integer n = jdbc.queryForObject(
                "SELECT COUNT(*) FROM deliveries WHERE batch_id = ? AND status NOT IN ('DELIVERED', 'FAILED')",
                Integer.class, batchId
        );
        return n == null ? 0 : n;
    }

    private List<Map<String, Object>> loadPending(String batchId) {
        return jdbc.query(
                """
                SELECT id, event_id, subscription_id, webhook_url, attempt_count, status
                FROM deliveries
                WHERE batch_id = ? AND status NOT IN ('DELIVERED', 'FAILED')
                """,
                (rs, i) -> {
                    Map<String, Object> m = new HashMap<>();
                    m.put("id", rs.getString("id"));
                    m.put("eventId", rs.getString("event_id"));
                    m.put("subscriptionId", rs.getString("subscription_id"));
                    m.put("url", rs.getString("webhook_url"));
                    m.put("attempts", rs.getInt("attempt_count"));
                    m.put("status", rs.getString("status"));
                    return m;
                },
                batchId
        );
    }

    private void ensureDelivery(String deliveryId, String batchId, String eventId, String subId, String url) {
        String now = Instant.now().toString();
        jdbc.update(
                """
                INSERT OR IGNORE INTO deliveries
                (id, batch_id, event_id, subscription_id, status, attempt_count, webhook_url, created_at, updated_at)
                VALUES (?,?,?,?, 'PENDING', 0, ?, ?, ?)
                """,
                deliveryId, batchId, eventId, subId, url, now, now
        );
    }

    private void deliverOne(Map<String, Object> d, Path batchFile) {
        String deliveryId = (String) d.get("id");
        String url = (String) d.get("url");
        String eventId = (String) d.get("eventId");
        int attempts = (int) d.get("attempts");

        // load event line for body (simple: re-read file for matching id)
        Map<String, Object> event;
        try {
            event = findEvent(batchFile, eventId);
        } catch (Exception e) {
            recordAttempt(deliveryId, null, e.getMessage());
            fail(deliveryId);
            return;
        }

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("deliveryId", deliveryId);
        body.put("eventId", eventId);
        body.put("type", event.get("type"));
        body.put("source", event.get("source"));
        body.put("payload", event.get("payload"));

        jdbc.update(
                "UPDATE deliveries SET status = 'INFLIGHT', updated_at = ? WHERE id = ?",
                Instant.now().toString(), deliveryId
        );

        try {
            var response = http.post()
                    .uri(url)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
            int code = response.getStatusCode().value();
            recordAttempt(deliveryId, code, null);
            if (response.getStatusCode().is2xxSuccessful()) {
                jdbc.update(
                        "UPDATE deliveries SET status = 'DELIVERED', attempt_count = ?, updated_at = ? WHERE id = ?",
                        attempts + 1, Instant.now().toString(), deliveryId
                );
            } else {
                retryOrFail(deliveryId, attempts + 1);
            }
        } catch (Exception ex) {
            recordAttempt(deliveryId, null, ex.getMessage());
            retryOrFail(deliveryId, attempts + 1);
        }
    }

    private void retryOrFail(String deliveryId, int newAttempts) {
        String now = Instant.now().toString();
        if (newAttempts >= MAX_ATTEMPTS) {
            jdbc.update(
                    "UPDATE deliveries SET status = 'FAILED', attempt_count = ?, updated_at = ? WHERE id = ?",
                    newAttempts, now, deliveryId
            );
        } else {
            jdbc.update(
                    "UPDATE deliveries SET status = 'PENDING', attempt_count = ?, updated_at = ? WHERE id = ?",
                    newAttempts, now, deliveryId
            );
        }
    }

    private void fail(String deliveryId) {
        jdbc.update(
                "UPDATE deliveries SET status = 'FAILED', updated_at = ? WHERE id = ?",
                Instant.now().toString(), deliveryId
        );
    }

    private void recordAttempt(String deliveryId, Integer httpStatus, String error) {
        jdbc.update(
                "INSERT INTO delivery_attempts(delivery_id, attempted_at, http_status, error) VALUES (?,?,?,?)",
                deliveryId, Instant.now().toString(), httpStatus, error
        );
    }

    private Map<String, Object> findEvent(Path file, String eventId) throws Exception {
        for (String line : Files.readAllLines(file)) {
            if (line.isBlank()) {
                continue;
            }
            Map<String, Object> event = mapper.readValue(line, new TypeReference<>() {
            });
            if (eventId.equals(String.valueOf(event.get("id")))) {
                return event;
            }
        }
        throw new IllegalStateException("event not found in batch: " + eventId);
    }
}
