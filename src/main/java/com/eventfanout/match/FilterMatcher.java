package com.eventfanout.match;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Matches an event against subscription filter rules. */
public final class FilterMatcher {

    private FilterMatcher() {
    }

    /**
     * filter keys (all optional):
     * - types: list of strings, trailing * wildcard ok; empty/missing = all
     * - sources: list of strings; empty/missing = all
     * - payload: map of top-level exact equality checks; empty/missing = all
     */
    @SuppressWarnings("unchecked")
    public static boolean matches(Map<String, Object> filter, String type, String source, Map<String, Object> payload) {
        if (filter == null || filter.isEmpty()) {
            return true;
        }

        Object typesObj = filter.get("types");
        if (typesObj instanceof List<?> types && !types.isEmpty()) {
            boolean typeOk = false;
            for (Object t : types) {
                if (t != null && typeMatches(String.valueOf(t), type)) {
                    typeOk = true;
                    break;
                }
            }
            if (!typeOk) {
                return false;
            }
        }

        Object sourcesObj = filter.get("sources");
        if (sourcesObj instanceof List<?> sources && !sources.isEmpty()) {
            boolean sourceOk = false;
            for (Object s : sources) {
                if (s != null && Objects.equals(String.valueOf(s), source)) {
                    sourceOk = true;
                    break;
                }
            }
            if (!sourceOk) {
                return false;
            }
        }

        Object payloadFilterObj = filter.get("payload");
        if (payloadFilterObj instanceof Map<?, ?> payloadFilter && !payloadFilter.isEmpty()) {
            Map<String, Object> eventPayload = payload == null ? Map.of() : payload;
            for (Map.Entry<?, ?> e : payloadFilter.entrySet()) {
                String key = String.valueOf(e.getKey());
                Object expected = e.getValue();
                Object actual = eventPayload.get(key);
                if (!Objects.equals(expected, actual)) {
                    return false;
                }
            }
        }

        return true;
    }

    static boolean typeMatches(String pattern, String type) {
        if (pattern == null || type == null) {
            return false;
        }
        if (pattern.endsWith("*")) {
            String prefix = pattern.substring(0, pattern.length() - 1);
            return type.startsWith(prefix);
        }
        return pattern.equals(type);
    }
}
