package com.eventfanout.match;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilterMatcherTest {

    @Test
    void nullOrEmptyFilterMatchesAll() {
        assertTrue(FilterMatcher.matches(null, "a", "b", Map.of()));
        assertTrue(FilterMatcher.matches(Map.of(), "a", "b", Map.of("x", 1)));
    }

    @Test
    void typeExactAndWildcard() {
        var filter = Map.<String, Object>of("types", List.of("order.created", "invoice.*"));
        assertTrue(FilterMatcher.matches(filter, "order.created", "x", Map.of()));
        assertTrue(FilterMatcher.matches(filter, "invoice.paid", "x", Map.of()));
        assertFalse(FilterMatcher.matches(filter, "user.created", "x", Map.of()));
    }

    @Test
    void emptyTypesListMatchesAllTypes() {
        var filter = Map.<String, Object>of("types", List.of());
        assertTrue(FilterMatcher.matches(filter, "anything", "x", Map.of()));
    }

    @Test
    void sourceAndPayload() {
        var filter = Map.<String, Object>of(
                "sources", List.of("billing"),
                "payload", Map.of("status", "paid")
        );
        assertTrue(FilterMatcher.matches(filter, "t", "billing", Map.of("status", "paid")));
        assertFalse(FilterMatcher.matches(filter, "t", "billing", Map.of("status", "open")));
        assertFalse(FilterMatcher.matches(filter, "t", "crm", Map.of("status", "paid")));
    }

    @Test
    void nullPayloadTreatedAsEmpty() {
        var filter = Map.<String, Object>of("payload", Map.of("status", "paid"));
        assertFalse(FilterMatcher.matches(filter, "t", "s", null));
    }

    @Test
    void typeMatchesHelper() {
        assertTrue(FilterMatcher.typeMatches("order.*", "order.created"));
        assertTrue(FilterMatcher.typeMatches("order.created", "order.created"));
        assertFalse(FilterMatcher.typeMatches("order.*", "user.created"));
        assertFalse(FilterMatcher.typeMatches(null, "x"));
        assertFalse(FilterMatcher.typeMatches("x", null));
    }
}
