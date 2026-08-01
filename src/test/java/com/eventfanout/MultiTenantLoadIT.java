package com.eventfanout;

import com.eventfanout.api.CustomerAuth;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Correctness soak: 10K events across 5 customers.
 * Verifies durable ingest, tenant isolation, filter matching, drain, and at-least-once delivery.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Tag("load")
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class MultiTenantLoadIT {

    private static final int CUSTOMERS = 5;
    private static final int EVENTS_PER_CUSTOMER = 2_000;
    private static final int BATCH_SIZE = 100;
    /** Every 10th event is intentionally non-matching (type user.*). */
    private static final int MATCH_EVERY = 10;

    @Autowired MockMvc mvc;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;

    private final List<MockWebServer> hooks = new ArrayList<>();
    private final List<String> customerIds = new ArrayList<>();
    private final Map<String, AtomicInteger> webhookHitsByCustomer = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> crossTenantHits = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() throws Exception {
        cleanTablesAndBatches();
        for (int i = 1; i <= CUSTOMERS; i++) {
            String customerId = "customer-" + i;
            customerIds.add(customerId);
            webhookHitsByCustomer.put(customerId, new AtomicInteger());
            crossTenantHits.put(customerId, new AtomicInteger());

            MockWebServer server = new MockWebServer();
            String expectedCustomer = customerId;
            server.setDispatcher(new Dispatcher() {
                @Override
                public MockResponse dispatch(RecordedRequest request) {
                    try {
                        JsonNode root = mapper.readTree(request.getBody().readUtf8());
                        String payloadCustomer = root.path("payload").path("customer").asText("");
                        if (expectedCustomer.equals(payloadCustomer)) {
                            webhookHitsByCustomer.get(expectedCustomer).incrementAndGet();
                        } else {
                            crossTenantHits.get(expectedCustomer).incrementAndGet();
                        }
                    } catch (Exception ignored) {
                        crossTenantHits.get(expectedCustomer).incrementAndGet();
                    }
                    return new MockResponse().setResponseCode(200);
                }
            });
            server.start();
            hooks.add(server);

            mvc.perform(post("/api/v1/subscriptions")
                            .header(CustomerAuth.HEADER, customerId)
                            .contentType(MediaType.APPLICATION_JSON)
                            .content("""
                                {"url":"%s","filter":{"types":["order.*"],"sources":["billing"]}}
                                """.formatted(server.url("/hook"))))
                    .andExpect(status().isCreated());
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        for (MockWebServer server : hooks) {
            server.shutdown();
        }
        hooks.clear();
        customerIds.clear();
        webhookHitsByCustomer.clear();
        crossTenantHits.clear();
        cleanTablesAndBatches();
    }

    @Test
    void tenThousandEventsAcrossFiveCustomers() throws Exception {
        int matchingPerCustomer = EVENTS_PER_CUSTOMER - (EVENTS_PER_CUSTOMER / MATCH_EVERY);
        int expectedDeliveries = CUSTOMERS * matchingPerCustomer;

        ExecutorService pool = Executors.newFixedThreadPool(CUSTOMERS);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (String customerId : customerIds) {
                futures.add(pool.submit(() -> ingestForCustomer(customerId)));
            }
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }

        waitUntilDrained(expectedDeliveries);

        // --- Completeness / audit ---
        Integer delivered = jdbc.queryForObject(
                "SELECT COUNT(*) FROM deliveries WHERE status = 'DELIVERED'", Integer.class);
        Integer nonTerminal = jdbc.queryForObject(
                "SELECT COUNT(*) FROM deliveries WHERE status NOT IN ('DELIVERED', 'FAILED')", Integer.class);
        Integer failed = jdbc.queryForObject(
                "SELECT COUNT(*) FROM deliveries WHERE status = 'FAILED'", Integer.class);
        Integer notDoneBatches = jdbc.queryForObject(
                "SELECT COUNT(*) FROM batches WHERE status != 'DONE'", Integer.class);
        Integer totalDeliveryRows = jdbc.queryForObject(
                "SELECT COUNT(*) FROM deliveries", Integer.class);

        assertThat(delivered).as("matching events should be DELIVERED").isEqualTo(expectedDeliveries);
        assertThat(totalDeliveryRows).as("no deliveries for non-matching events").isEqualTo(expectedDeliveries);
        assertThat(nonTerminal).as("no stuck deliveries").isZero();
        assertThat(failed).as("hooks always 200 — no FAILED").isZero();
        assertThat(notDoneBatches).as("all batches drained to DONE").isZero();

        // --- Durable ingest: every accepted event landed on disk ---
        assertThat(countJsonlEvents()).as("jsonl event lines").isEqualTo(CUSTOMERS * EVENTS_PER_CUSTOMER);

        // --- Tenant isolation + at-least-once webhook receipt ---
        for (String customerId : customerIds) {
            assertThat(crossTenantHits.get(customerId).get())
                    .as("customer %s must not receive foreign events", customerId)
                    .isZero();
            assertThat(webhookHitsByCustomer.get(customerId).get())
                    .as("customer %s at-least-once for matching events", customerId)
                    .isGreaterThanOrEqualTo(matchingPerCustomer);
        }
    }

    private void ingestForCustomer(String customerId) {
        try {
            for (int offset = 0; offset < EVENTS_PER_CUSTOMER; offset += BATCH_SIZE) {
                StringBuilder events = new StringBuilder();
                events.append("{\"events\":[");
                for (int i = 0; i < BATCH_SIZE; i++) {
                    int n = offset + i;
                    boolean match = n % MATCH_EVERY != 0;
                    String type = match ? "order.created" : "user.created";
                    if (i > 0) {
                        events.append(',');
                    }
                    events.append("""
                            {"type":"%s","source":"billing","payload":{"customer":"%s","n":%d}}
                            """.formatted(type, customerId, n).trim());
                }
                events.append("]}");

                mvc.perform(post("/api/v1/events/batch")
                                .header(CustomerAuth.HEADER, customerId)
                                .contentType(MediaType.APPLICATION_JSON)
                                .content(events.toString()))
                        .andExpect(status().isAccepted())
                        .andExpect(jsonPath("$.accepted.length()").value(BATCH_SIZE))
                        .andExpect(jsonPath("$.rejected.length()").value(0));
            }
        } catch (Exception e) {
            throw new RuntimeException("ingest failed for " + customerId, e);
        }
    }

    private void waitUntilDrained(int expectedDeliveries) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(8);
        while (System.nanoTime() < deadline) {
            Integer delivered = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM deliveries WHERE status = 'DELIVERED'", Integer.class);
            Integer pending = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM deliveries WHERE status NOT IN ('DELIVERED', 'FAILED')", Integer.class);
            Integer notDone = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM batches WHERE status != 'DONE'", Integer.class);
            if (delivered != null && delivered == expectedDeliveries
                    && pending != null && pending == 0
                    && notDone != null && notDone == 0) {
                return;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        Integer delivered = jdbc.queryForObject(
                "SELECT COUNT(*) FROM deliveries WHERE status = 'DELIVERED'", Integer.class);
        Integer pending = jdbc.queryForObject(
                "SELECT COUNT(*) FROM deliveries WHERE status NOT IN ('DELIVERED', 'FAILED')", Integer.class);
        Integer batches = jdbc.queryForObject("SELECT COUNT(*) FROM batches", Integer.class);
        Integer notDone = jdbc.queryForObject(
                "SELECT COUNT(*) FROM batches WHERE status != 'DONE'", Integer.class);
        throw new AssertionError(
                "timed out waiting for drain: delivered=" + delivered
                        + " expected=" + expectedDeliveries
                        + " pending=" + pending
                        + " batches=" + batches
                        + " notDone=" + notDone
        );
    }

    private long countJsonlEvents() throws Exception {
        Path dir = Path.of("data/batches");
        if (!Files.isDirectory(dir)) {
            return 0;
        }
        long count = 0;
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*.jsonl")) {
            for (Path file : stream) {
                try (var lines = Files.lines(file)) {
                    count += lines.filter(l -> !l.isBlank()).count();
                }
            }
        }
        return count;
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
