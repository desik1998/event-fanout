package com.eventfanout.api;

import com.eventfanout.ingest.EventBuffer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = EventController.class)
@Import(ApiExceptionHandler.class)
class EventControllerTest {

    @Autowired MockMvc mvc;
    @MockBean EventBuffer buffer;

    @Test
    void sendOneRequiresCustomer() throws Exception {
        mvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"type":"order.created","source":"billing","payload":{"id":1}}
                            """))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void sendOneAccepted() throws Exception {
        when(buffer.enqueue(eq("acme"), anyString(), anyString(), anyMap()))
                .thenReturn(new EventBuffer.Enqueued("e1", CompletableFuture.completedFuture("b1")));

        mvc.perform(post("/api/v1/events")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"type":"order.created","source":"billing","payload":{"id":1}}
                            """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value("e1"))
                .andExpect(jsonPath("$.customerId").value("acme"));
    }

    @Test
    void sendOneValidationFails() throws Exception {
        mvc.perform(post("/api/v1/events")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"type":"","source":"billing","payload":{}}
                            """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("validation_failed"));
    }

    @Test
    void sendBatchPartialReject() throws Exception {
        when(buffer.enqueue(eq("acme"), anyString(), anyString(), anyMap()))
                .thenReturn(new EventBuffer.Enqueued("e1", CompletableFuture.completedFuture("b1")));

        mvc.perform(post("/api/v1/events/batch")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"events":[
                              {"type":"order.created","source":"billing","payload":{}},
                              {"type":"order.created","source":"billing","payload":null}
                            ]}
                            """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted.length()").value(1))
                .andExpect(jsonPath("$.rejected.length()").value(1));
    }

    @Test
    void sendBatchTooLarge() throws Exception {
        StringBuilder sb = new StringBuilder("{\"events\":[");
        for (int i = 0; i < 101; i++) {
            if (i > 0) sb.append(',');
            sb.append("{\"type\":\"t\",\"source\":\"s\",\"payload\":{}}");
        }
        sb.append("]}");

        mvc.perform(post("/api/v1/events/batch")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(sb.toString()))
                .andExpect(status().isBadRequest());
    }

    @Test
    void sendOnePayloadTooLarge() throws Exception {
        String huge = "x".repeat(EventController.MAX_PAYLOAD_BYTES + 1);
        mvc.perform(post("/api/v1/events")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"type":"order.created","source":"billing","payload":{"blob":"%s"}}
                            """.formatted(huge)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(
                        "payload exceeds max size of " + EventController.MAX_PAYLOAD_BYTES + " bytes"));
    }

    @Test
    void sendBatchPayloadTooLarge() throws Exception {
        when(buffer.enqueue(eq("acme"), anyString(), anyString(), anyMap()))
                .thenReturn(new EventBuffer.Enqueued("e1", CompletableFuture.completedFuture("b1")));

        String huge = "x".repeat(EventController.MAX_PAYLOAD_BYTES + 1);
        mvc.perform(post("/api/v1/events/batch")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"events":[
                              {"type":"t","source":"s","payload":{}},
                              {"type":"t","source":"s","payload":{"blob":"%s"}}
                            ]}
                            """.formatted(huge)))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.accepted.length()").value(1))
                .andExpect(jsonPath("$.rejected.length()").value(1))
                .andExpect(jsonPath("$.rejected[0].index").value(1))
                .andExpect(jsonPath("$.rejected[0].error").value(
                        "payload exceeds max size of " + EventController.MAX_PAYLOAD_BYTES + " bytes"));
    }
}
