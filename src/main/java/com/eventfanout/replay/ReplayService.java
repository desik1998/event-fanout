package com.eventfanout.replay;

import com.eventfanout.match.FilterMatcher;
import com.eventfanout.store.SubscriptionStore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Re-fanout a durable event to current matching subscriptions by resetting
 * delivery rows to {@code PENDING} and reopening the batch as {@code READY}
 * so {@link com.eventfanout.worker.BatchWorker} delivers again (at-least-once).
 */
@Service
public class ReplayService {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final SubscriptionStore subscriptions;

    public ReplayService(JdbcTemplate jdbc, ObjectMapper mapper, SubscriptionStore subscriptions) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.subscriptions = subscriptions;
    }

    public Map<String, Object> replay(String customerId, String eventId, String subscriptionId) {
        if (eventId == null || eventId.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "eventId is required");
        }

        LocatedEvent located = findEvent(eventId);
        if (located == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "event not found: " + eventId);
        }
        if (!customerId.equals(located.customerId())) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "event not found: " + eventId);
        }

        String type = String.valueOf(located.event().get("type"));
        String source = String.valueOf(located.event().get("source"));
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = located.event().get("payload") instanceof Map<?, ?> p
                ? (Map<String, Object>) p
                : Map.of();

        List<Map<String, Object>> targets = new ArrayList<>();
        if (subscriptionId != null && !subscriptionId.isBlank()) {
            Map<String, Object> sub = findSubscriptionForCustomer(customerId, subscriptionId);
            if (sub == null) {
                throw new ResponseStatusException(HttpStatus.NOT_FOUND, "subscription not found: " + subscriptionId);
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> filter = (Map<String, Object>) sub.get("filter");
            if (!FilterMatcher.matches(filter, type, source, payload)) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "subscription filter does not match event"
                );
            }
            targets.add(sub);
        } else {
            for (Map<String, Object> sub : subscriptions.listByCustomer(customerId)) {
                @SuppressWarnings("unchecked")
                Map<String, Object> filter = (Map<String, Object>) sub.get("filter");
                if (FilterMatcher.matches(filter, type, source, payload)) {
                    targets.add(sub);
                }
            }
        }

        if (targets.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "no matching subscriptions for replay");
        }

        String now = Instant.now().toString();
        List<Map<String, Object>> replayed = new ArrayList<>();
        for (Map<String, Object> sub : targets) {
            String subId = (String) sub.get("id");
            String url = (String) sub.get("url");
            String deliveryId = eventId + "__" + subId;
            upsertPendingDelivery(deliveryId, located.batchId(), eventId, subId, url, now);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("deliveryId", deliveryId);
            item.put("subscriptionId", subId);
            item.put("status", "PENDING");
            replayed.add(item);
        }

        reopenBatch(located.batchId());

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("eventId", eventId);
        resp.put("customerId", customerId);
        resp.put("batchId", located.batchId());
        resp.put("replayed", replayed);
        return resp;
    }

    private Map<String, Object> findSubscriptionForCustomer(String customerId, String subscriptionId) {
        return subscriptions.listByCustomer(customerId).stream()
                .filter(s -> subscriptionId.equals(s.get("id")))
                .findFirst()
                .orElse(null);
    }

    private void upsertPendingDelivery(
            String deliveryId, String batchId, String eventId, String subId, String url, String now
    ) {
        int updated = jdbc.update(
                """
                UPDATE deliveries
                SET status = 'PENDING', attempt_count = 0, webhook_url = ?, updated_at = ?
                WHERE id = ?
                """,
                url, now, deliveryId
        );
        if (updated == 0) {
            jdbc.update(
                    """
                    INSERT INTO deliveries
                    (id, batch_id, event_id, subscription_id, status, attempt_count, webhook_url, created_at, updated_at)
                    VALUES (?,?,?,?, 'PENDING', 0, ?, ?, ?)
                    """,
                    deliveryId, batchId, eventId, subId, url, now, now
            );
        }
    }

    private void reopenBatch(String batchId) {
        jdbc.update(
                """
                UPDATE batches
                SET status = 'READY', owner_host = NULL, started_at = NULL, completed_at = NULL
                WHERE id = ?
                """,
                batchId
        );
    }

    private LocatedEvent findEvent(String eventId) {
        List<Map<String, Object>> batches = jdbc.query(
                "SELECT id, path FROM batches ORDER BY created_at",
                (rs, i) -> {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("id", rs.getString("id"));
                    m.put("path", rs.getString("path"));
                    return m;
                }
        );
        for (Map<String, Object> batch : batches) {
            Path path = Path.of((String) batch.get("path"));
            if (!Files.isRegularFile(path)) {
                continue;
            }
            try {
                for (String line : Files.readAllLines(path)) {
                    if (line.isBlank()) {
                        continue;
                    }
                    Map<String, Object> event = mapper.readValue(line, new TypeReference<>() {
                    });
                    if (eventId.equals(String.valueOf(event.get("id")))) {
                        String customer = event.get("customerId") == null
                                ? ""
                                : String.valueOf(event.get("customerId"));
                        return new LocatedEvent((String) batch.get("id"), path, customer, event);
                    }
                }
            } catch (Exception ex) {
                throw new IllegalStateException("failed reading batch " + batch.get("id") + ": " + ex.getMessage(), ex);
            }
        }
        return null;
    }

    private record LocatedEvent(String batchId, Path path, String customerId, Map<String, Object> event) {
    }
}
