package com.eventfanout.api;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class ApiExceptionHandlerTest {

    private final ApiExceptionHandler handler = new ApiExceptionHandler();

    @Test
    void timeoutMapsTo504() {
        ResponseEntity<Map<String, Object>> resp = handler.timeout(new TimeoutException());
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.GATEWAY_TIMEOUT);
        assertThat(resp.getBody()).containsEntry("error", "flush_timeout");
    }

    @Test
    void illegalArgMapsTo400() {
        ResponseEntity<Map<String, Object>> resp = handler.illegalArg(new IllegalArgumentException("bad"));
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(resp.getBody()).containsEntry("message", "bad");
    }

    @Test
    void statusExceptionPreservesCode() {
        ResponseEntity<Map<String, Object>> resp = handler.status(
                new ResponseStatusException(HttpStatus.NOT_FOUND, "missing")
        );
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(resp.getBody()).containsEntry("message", "missing");
    }

    @Test
    void genericMapsTo500() {
        ResponseEntity<Map<String, Object>> resp = handler.generic(new RuntimeException("boom"));
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
        assertThat(resp.getBody()).containsEntry("error", "internal_error");
    }
}
