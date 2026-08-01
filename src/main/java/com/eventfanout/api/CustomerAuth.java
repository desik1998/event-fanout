package com.eventfanout.api;

import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** Resolves tenant from {@code X-Customer-Id} header (stand-in for auth). */
public final class CustomerAuth {

    public static final String HEADER = "X-Customer-Id";

    private CustomerAuth() {
    }

    public static String requireCustomerId(String customerId) {
        if (customerId == null || customerId.isBlank()) {
            throw new ResponseStatusException(
                    HttpStatus.UNAUTHORIZED,
                    "missing " + HEADER + " header"
            );
        }
        return customerId.trim();
    }
}
