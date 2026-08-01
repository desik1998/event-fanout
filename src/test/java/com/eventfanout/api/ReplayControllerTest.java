package com.eventfanout.api;

import com.eventfanout.replay.ReplayService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = ReplayController.class)
@Import(ApiExceptionHandler.class)
class ReplayControllerTest {

    @Autowired MockMvc mvc;
    @MockBean ReplayService replayService;

    @Test
    void requiresCustomer() throws Exception {
        mvc.perform(post("/api/v1/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":\"e1\"}"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void accepted() throws Exception {
        when(replayService.replay(eq("acme"), eq("e1"), isNull()))
                .thenReturn(Map.of(
                        "eventId", "e1",
                        "customerId", "acme",
                        "batchId", "b1",
                        "replayed", List.of(Map.of("deliveryId", "e1__s1", "subscriptionId", "s1", "status", "PENDING"))
                ));

        mvc.perform(post("/api/v1/replay")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":\"e1\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value("e1"))
                .andExpect(jsonPath("$.replayed.length()").value(1));
    }
}
