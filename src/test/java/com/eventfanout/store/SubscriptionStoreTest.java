package com.eventfanout.store;

import com.eventfanout.support.TestDb;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SubscriptionStoreTest {

    @TempDir Path temp;
    SubscriptionStore store;

    @BeforeEach
    void setUp() throws Exception {
        JdbcTemplate jdbc = TestDb.create(temp.resolve("t.db"));
        store = new SubscriptionStore(jdbc, new ObjectMapper());
    }

    @Test
    void createAndListScopedByCustomer() {
        store.create("cust-a", "http://example.com/a", Map.of("types", List.of("order.*")));
        store.create("cust-b", "http://example.com/b", Map.of());

        assertThat(store.listByCustomer("cust-a")).hasSize(1);
        assertThat(store.listByCustomer("cust-b")).hasSize(1);
        assertThat(store.listAll()).hasSize(2);
        assertThat(store.listByCustomer("cust-a").get(0).get("customerId")).isEqualTo("cust-a");
    }

    @Test
    void deleteOnlyOwnSubscription() {
        var a = store.create("cust-a", "http://example.com/a", Map.of());
        String id = (String) a.get("id");
        assertThat(store.delete("cust-b", id)).isFalse();
        assertThat(store.delete("cust-a", id)).isTrue();
        assertThat(store.listByCustomer("cust-a")).isEmpty();
    }

    @Test
    void createWithNullFilterDefaultsEmpty() {
        var created = store.create("cust-a", "http://example.com/b", null);
        assertThat(created.get("filter")).isEqualTo(Map.of());
    }
}
