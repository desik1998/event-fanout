package com.eventfanout.api;

import com.eventfanout.store.SubscriptionStore;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/subscriptions")
public class SubscriptionController {

    private final SubscriptionStore store;

    public SubscriptionController(SubscriptionStore store) {
        this.store = store;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(
            @RequestHeader(value = CustomerAuth.HEADER, required = false) String customerHeader,
            @Valid @RequestBody CreateBody body
    ) {
        String customerId = CustomerAuth.requireCustomerId(customerHeader);
        validateUrl(body.url());
        Map<String, Object> filter = body.filter() == null ? Map.of() : body.filter();
        validateFilter(filter);
        return ResponseEntity.status(201).body(store.create(customerId, body.url().trim(), filter));
    }

    @GetMapping
    public List<Map<String, Object>> list(
            @RequestHeader(value = CustomerAuth.HEADER, required = false) String customerHeader
    ) {
        String customerId = CustomerAuth.requireCustomerId(customerHeader);
        return store.listByCustomer(customerId);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(
            @RequestHeader(value = CustomerAuth.HEADER, required = false) String customerHeader,
            @PathVariable String id
    ) {
        String customerId = CustomerAuth.requireCustomerId(customerHeader);
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id is required");
        }
        if (!store.delete(customerId, id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "subscription not found");
        }
        return ResponseEntity.noContent().build();
    }

    private static void validateUrl(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("url is required");
        }
        try {
            URI uri = URI.create(url.trim());
            String scheme = uri.getScheme();
            if (scheme == null || (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))) {
                throw new IllegalArgumentException("url must start with http:// or https://");
            }
            if (uri.getHost() == null || uri.getHost().isBlank()) {
                throw new IllegalArgumentException("url must include a host");
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("url is invalid: " + e.getMessage());
        }
    }

    private static void validateFilter(Map<String, Object> filter) {
        if (filter.containsKey("types") && !(filter.get("types") instanceof List<?>)) {
            throw new IllegalArgumentException("filter.types must be a list of strings");
        }
        if (filter.containsKey("sources") && !(filter.get("sources") instanceof List<?>)) {
            throw new IllegalArgumentException("filter.sources must be a list of strings");
        }
        if (filter.containsKey("payload") && !(filter.get("payload") instanceof Map<?, ?>)) {
            throw new IllegalArgumentException("filter.payload must be an object");
        }
        if (filter.get("types") instanceof List<?> types) {
            for (Object t : types) {
                if (!(t instanceof String) || ((String) t).isBlank()) {
                    throw new IllegalArgumentException("filter.types entries must be non-blank strings");
                }
            }
        }
        if (filter.get("sources") instanceof List<?> sources) {
            for (Object s : sources) {
                if (!(s instanceof String) || ((String) s).isBlank()) {
                    throw new IllegalArgumentException("filter.sources entries must be non-blank strings");
                }
            }
        }
    }

    public record CreateBody(
            @NotBlank(message = "url is required") String url,
            Map<String, Object> filter
    ) {
    }
}
