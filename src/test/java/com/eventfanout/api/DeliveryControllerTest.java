package com.eventfanout.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = DeliveryController.class)
@Import(ApiExceptionHandler.class)
class DeliveryControllerTest {

    @Autowired MockMvc mvc;
    @MockBean JdbcTemplate jdbc;

    @Test
    void requiresEventIdOrSubscriptionId() throws Exception {
        mvc.perform(get("/api/v1/deliveries"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void listByEventIdIncludesAttempts() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("id", "d1");
        row.put("batchId", "b1");
        row.put("eventId", "e1");
        row.put("subscriptionId", "s1");
        row.put("status", "DELIVERED");
        row.put("attemptCount", 1);
        row.put("webhookUrl", "http://x");
        row.put("createdAt", "t");
        row.put("updatedAt", "t");

        Map<String, Object> attempt = new HashMap<>();
        attempt.put("attemptedAt", "t");
        attempt.put("httpStatus", 200);
        attempt.put("error", null);

        when(jdbc.query(contains("event_id = ?"), any(RowMapper.class), anyString()))
                .thenReturn(List.of(row));
        when(jdbc.query(contains("delivery_attempts"), any(RowMapper.class), anyString()))
                .thenReturn(List.of(attempt));

        mvc.perform(get("/api/v1/deliveries").param("eventId", "e1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value("d1"))
                .andExpect(jsonPath("$[0].attempts[0].httpStatus").value(200));
    }

    @Test
    void listBySubscriptionId() throws Exception {
        when(jdbc.query(contains("subscription_id = ?"), any(RowMapper.class), anyString()))
                .thenReturn(List.of());
        mvc.perform(get("/api/v1/deliveries").param("subscriptionId", "s1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }
}
