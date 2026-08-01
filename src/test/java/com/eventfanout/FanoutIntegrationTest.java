package com.eventfanout;

import com.eventfanout.api.CustomerAuth;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class FanoutIntegrationTest {

    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;

    MockWebServer webhook;

    @BeforeEach
    void setUp() throws Exception {
        cleanTablesAndBatches();
        webhook = new MockWebServer();
        webhook.start();
        webhook.enqueue(new MockResponse().setResponseCode(200));
    }

    @AfterEach
    void tearDown() throws Exception {
        webhook.shutdown();
        cleanTablesAndBatches();
    }

    @Test
    void ingestMatchDeliverAndAudit() throws Exception {
        String hook = webhook.url("/hook").toString();
        mvc.perform(post("/api/v1/subscriptions")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"url":"%s","filter":{"types":["order.*"],"sources":["billing"]}}
                            """.formatted(hook)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.customerId").value("acme"));

        MvcResult created = mvc.perform(post("/api/v1/events")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"type":"order.created","source":"billing","payload":{"status":"paid"}}
                            """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").exists())
                .andExpect(jsonPath("$.customerId").value("acme"))
                .andReturn();

        String body = created.getResponse().getContentAsString();
        String eventId = body.replaceAll("(?s).*\"eventId\"\\s*:\\s*\"([^\"]+)\".*", "$1");

        boolean delivered = false;
        for (int i = 0; i < 40; i++) {
            Integer n = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM deliveries WHERE event_id = ? AND status = 'DELIVERED'",
                    Integer.class, eventId
            );
            if (n != null && n > 0) {
                delivered = true;
                break;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        assertThat(delivered).as("delivery should complete").isTrue();
        assertThat(webhook.takeRequest(2, TimeUnit.SECONDS)).isNotNull();

        mvc.perform(get("/api/v1/deliveries").param("eventId", eventId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].status").value("DELIVERED"));
    }

    @Test
    void listSubscriptionsIsTenantScoped() throws Exception {
        mvc.perform(post("/api/v1/subscriptions")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"url":"https://acme.example/hook","filter":{}}
                            """))
                .andExpect(status().isCreated());
        mvc.perform(post("/api/v1/subscriptions")
                        .header(CustomerAuth.HEADER, "globex")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"url":"https://globex.example/hook","filter":{}}
                            """))
                .andExpect(status().isCreated());

        mvc.perform(get("/api/v1/subscriptions").header(CustomerAuth.HEADER, "acme"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(1))
                .andExpect(jsonPath("$[0].customerId").value("acme"));
    }

    private void cleanTablesAndBatches() throws Exception {
        jdbc.update("DELETE FROM delivery_attempts");
        jdbc.update("DELETE FROM deliveries");
        jdbc.update("DELETE FROM batches");
        jdbc.update("DELETE FROM subscriptions");
        Path dir = Path.of("data/batches");
        Files.createDirectories(dir);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path p : stream) {
                Files.deleteIfExists(p);
            }
        }
    }
}
