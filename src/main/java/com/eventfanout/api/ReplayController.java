package com.eventfanout.api;

import com.eventfanout.replay.ReplayService;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/replay")
public class ReplayController {

    private final ReplayService replayService;

    public ReplayController(ReplayService replayService) {
        this.replayService = replayService;
    }

    /**
     * Re-queue fanout for a durable event (at-least-once).
     * Optional {@code subscriptionId} limits replay to one subscription.
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> replay(
            @RequestHeader(value = CustomerAuth.HEADER, required = false) String customerHeader,
            @RequestBody ReplayBody body
    ) {
        String customerId = CustomerAuth.requireCustomerId(customerHeader);
        if (body == null || body.eventId() == null || body.eventId().isBlank()) {
            throw new org.springframework.web.server.ResponseStatusException(
                    org.springframework.http.HttpStatus.BAD_REQUEST,
                    "eventId is required"
            );
        }
        Map<String, Object> result = replayService.replay(customerId, body.eventId(), body.subscriptionId());
        return ResponseEntity.accepted().body(result);
    }

    public record ReplayBody(
            @NotBlank String eventId,
            String subscriptionId
    ) {
    }
}
