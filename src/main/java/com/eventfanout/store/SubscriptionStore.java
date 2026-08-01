package com.eventfanout.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
public class SubscriptionStore {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;

    public SubscriptionStore(JdbcTemplate jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public Map<String, Object> create(String customerId, String url, Map<String, Object> filter) {
        String id = UUID.randomUUID().toString();
        String now = Instant.now().toString();
        try {
            String filterJson = mapper.writeValueAsString(filter == null ? Map.of() : filter);
            jdbc.update(
                    "INSERT INTO subscriptions(id, customer_id, url, filter_json, created_at) VALUES (?,?,?,?,?)",
                    id, customerId, url, filterJson, now
            );
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid filter: " + e.getMessage(), e);
        }
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("customerId", customerId);
        row.put("url", url);
        row.put("filter", filter == null ? Map.of() : filter);
        row.put("createdAt", now);
        return row;
    }

    /** Subscriptions for one customer only. */
    public List<Map<String, Object>> listByCustomer(String customerId) {
        return jdbc.query(
                """
                SELECT id, customer_id, url, filter_json, created_at
                FROM subscriptions WHERE customer_id = ? ORDER BY created_at
                """,
                this::mapRow,
                customerId
        );
    }

    /** All subscriptions (worker fanout). */
    public List<Map<String, Object>> listAll() {
        return jdbc.query(
                "SELECT id, customer_id, url, filter_json, created_at FROM subscriptions ORDER BY created_at",
                this::mapRow
        );
    }

    /** @return true if a row owned by this customer was deleted */
    public boolean delete(String customerId, String id) {
        int n = jdbc.update(
                "DELETE FROM subscriptions WHERE id = ? AND customer_id = ?",
                id, customerId
        );
        return n > 0;
    }

    private Map<String, Object> mapRow(java.sql.ResultSet rs, int i) throws java.sql.SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", rs.getString("id"));
        row.put("customerId", rs.getString("customer_id"));
        row.put("url", rs.getString("url"));
        row.put("filter", readFilter(rs.getString("filter_json")));
        row.put("createdAt", rs.getString("created_at"));
        return row;
    }

    private Map<String, Object> readFilter(String json) {
        try {
            return mapper.readValue(json, new TypeReference<>() {
            });
        } catch (Exception e) {
            return Map.of();
        }
    }
}
