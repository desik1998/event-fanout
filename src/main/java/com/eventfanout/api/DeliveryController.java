package com.eventfanout.api;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/deliveries")
public class DeliveryController {

    private final JdbcTemplate jdbc;

    public DeliveryController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping
    public List<Map<String, Object>> list(
            @RequestParam(required = false) String eventId,
            @RequestParam(required = false) String subscriptionId
    ) {
        if ((eventId == null || eventId.isBlank()) && (subscriptionId == null || subscriptionId.isBlank())) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "eventId or subscriptionId is required");
        }

        String sql;
        Object arg;
        if (eventId != null && !eventId.isBlank()) {
            sql = """
                SELECT id, batch_id, event_id, subscription_id, status, attempt_count,
                       webhook_url, created_at, updated_at
                FROM deliveries WHERE event_id = ? ORDER BY created_at
                """;
            arg = eventId;
        } else {
            sql = """
                SELECT id, batch_id, event_id, subscription_id, status, attempt_count,
                       webhook_url, created_at, updated_at
                FROM deliveries WHERE subscription_id = ? ORDER BY created_at
                """;
            arg = subscriptionId;
        }

        List<Map<String, Object>> rows = jdbc.query(sql, (rs, i) -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", rs.getString("id"));
            m.put("batchId", rs.getString("batch_id"));
            m.put("eventId", rs.getString("event_id"));
            m.put("subscriptionId", rs.getString("subscription_id"));
            m.put("status", rs.getString("status"));
            m.put("attemptCount", rs.getInt("attempt_count"));
            m.put("webhookUrl", rs.getString("webhook_url"));
            m.put("createdAt", rs.getString("created_at"));
            m.put("updatedAt", rs.getString("updated_at"));
            return m;
        }, arg);

        for (Map<String, Object> row : rows) {
            row.put("attempts", loadAttempts((String) row.get("id")));
        }
        return rows;
    }

    private List<Map<String, Object>> loadAttempts(String deliveryId) {
        return jdbc.query(
                """
                SELECT attempted_at, http_status, error
                FROM delivery_attempts WHERE delivery_id = ? ORDER BY id
                """,
                (rs, i) -> {
                    Map<String, Object> a = new LinkedHashMap<>();
                    a.put("attemptedAt", rs.getString("attempted_at"));
                    a.put("httpStatus", rs.getObject("http_status"));
                    a.put("error", rs.getString("error"));
                    return a;
                },
                deliveryId
        );
    }
}
