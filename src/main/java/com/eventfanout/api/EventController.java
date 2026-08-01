package com.eventfanout.api;

import com.eventfanout.ingest.EventBuffer;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/v1/events")
public class EventController {

    /** Max JSON-serialized payload size per event. */
    static final int MAX_PAYLOAD_BYTES = 64 * 1024;

    private final EventBuffer buffer;
    private final ObjectMapper mapper;

    public EventController(EventBuffer buffer, ObjectMapper mapper) {
        this.buffer = buffer;
        this.mapper = mapper;
    }

    @PostMapping
    public ResponseEntity<Map<String, String>> sendOne(
            @RequestHeader(value = CustomerAuth.HEADER, required = false) String customerHeader,
            @Valid @RequestBody EventBody body
    ) throws Exception {
        String customerId = CustomerAuth.requireCustomerId(customerHeader);
        rejectIfPayloadTooLarge(body.payload());
        try {
            EventBuffer.Enqueued e = buffer.enqueue(customerId, body.type(), body.source(), body.payload());
            String batchId = e.future().get(35, TimeUnit.SECONDS);
            Map<String, String> resp = new LinkedHashMap<>();
            resp.put("eventId", e.eventId());
            resp.put("batchId", batchId);
            resp.put("customerId", customerId);
            return ResponseEntity.accepted().body(resp);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "interrupted while flushing");
        }
    }

    @PostMapping("/batch")
    public ResponseEntity<Map<String, Object>> sendBatch(
            @RequestHeader(value = CustomerAuth.HEADER, required = false) String customerHeader,
            @Valid @RequestBody BatchBody body
    ) throws Exception {
        String customerId = CustomerAuth.requireCustomerId(customerHeader);
        if (body.events().size() > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "max 100 events per batch");
        }

        List<Map<String, Object>> accepted = new ArrayList<>();
        List<Map<String, Object>> rejected = new ArrayList<>();
        List<EventBuffer.Enqueued> pending = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();

        for (int i = 0; i < body.events().size(); i++) {
            EventBody ev = body.events().get(i);
            if (blank(ev.type()) || blank(ev.source()) || ev.payload() == null) {
                Map<String, Object> rej = new LinkedHashMap<>();
                rej.put("index", i);
                rej.put("error", "type, source, payload required");
                rejected.add(rej);
                continue;
            }
            if (payloadBytes(ev.payload()) > MAX_PAYLOAD_BYTES) {
                Map<String, Object> rej = new LinkedHashMap<>();
                rej.put("index", i);
                rej.put("error", payloadTooLargeMessage());
                rejected.add(rej);
                continue;
            }
            try {
                pending.add(buffer.enqueue(customerId, ev.type(), ev.source(), ev.payload()));
                indexes.add(i);
            } catch (RuntimeException ex) {
                Map<String, Object> rej = new LinkedHashMap<>();
                rej.put("index", i);
                rej.put("error", ex.getMessage() == null ? "enqueue failed" : ex.getMessage());
                rejected.add(rej);
            }
        }

        if (!pending.isEmpty()) {
            try {
                CompletableFuture.allOf(
                        pending.stream().map(EventBuffer.Enqueued::future).toArray(CompletableFuture[]::new)
                ).get(35, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "interrupted while flushing");
            }

            for (int j = 0; j < pending.size(); j++) {
                EventBuffer.Enqueued e = pending.get(j);
                try {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("eventId", e.eventId());
                    item.put("index", indexes.get(j));
                    item.put("batchId", e.future().get());
                    item.put("customerId", customerId);
                    accepted.add(item);
                } catch (Exception ex) {
                    Map<String, Object> rej = new LinkedHashMap<>();
                    rej.put("index", indexes.get(j));
                    rej.put("eventId", e.eventId());
                    rej.put("error", rootMessage(ex));
                    rejected.add(rej);
                }
            }
        }

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("accepted", accepted);
        resp.put("rejected", rejected);
        if (accepted.isEmpty() && !rejected.isEmpty()) {
            return ResponseEntity.badRequest().body(resp);
        }
        return ResponseEntity.accepted().body(resp);
    }

    private static String rootMessage(Exception ex) {
        Throwable c = ex;
        while (c.getCause() != null) {
            c = c.getCause();
        }
        return c.getMessage() == null ? "flush failed" : c.getMessage();
    }

    private void rejectIfPayloadTooLarge(Map<String, Object> payload) {
        if (payloadBytes(payload) > MAX_PAYLOAD_BYTES) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, payloadTooLargeMessage());
        }
    }

    private int payloadBytes(Map<String, Object> payload) {
        try {
            return mapper.writeValueAsBytes(payload).length;
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to serialize payload: " + e.getMessage(), e);
        }
    }

    private static String payloadTooLargeMessage() {
        return "payload exceeds max size of " + MAX_PAYLOAD_BYTES + " bytes";
    }

    private static boolean blank(String s) {
        return s == null || s.isBlank();
    }

    public record EventBody(
            @NotBlank String type,
            @NotBlank String source,
            @NotNull Map<String, Object> payload
    ) {
    }

    /** Items are validated manually so partial accept/reject is possible. */
    public record BatchBody(@NotEmpty List<EventBody> events) {
    }
}
