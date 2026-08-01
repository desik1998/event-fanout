package com.eventfanout.api;

import com.eventfanout.store.SubscriptionStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = SubscriptionController.class)
@Import(ApiExceptionHandler.class)
class SubscriptionControllerTest {

    @Autowired MockMvc mvc;
    @MockBean SubscriptionStore store;

    @Test
    void createRequiresCustomerHeader() throws Exception {
        mvc.perform(post("/api/v1/subscriptions")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"url":"https://example.com/hook","filter":{}}
                            """))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void createValid() throws Exception {
        when(store.create(eq("acme"), anyString(), any())).thenReturn(Map.of(
                "id", "s1",
                "customerId", "acme",
                "url", "https://example.com/hook",
                "filter", Map.of(),
                "createdAt", "t"
        ));

        mvc.perform(post("/api/v1/subscriptions")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"url":"https://example.com/hook","filter":{"types":["order.*"]}}
                            """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.customerId").value("acme"));
    }

    @Test
    void listScoped() throws Exception {
        when(store.listByCustomer("acme")).thenReturn(List.of(Map.of("id", "s1", "customerId", "acme")));
        mvc.perform(get("/api/v1/subscriptions").header(CustomerAuth.HEADER, "acme"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].id").value("s1"));
        verify(store).listByCustomer("acme");
    }

    @Test
    void deleteFound() throws Exception {
        when(store.delete("acme", "s1")).thenReturn(true);
        mvc.perform(delete("/api/v1/subscriptions/s1").header(CustomerAuth.HEADER, "acme"))
                .andExpect(status().isNoContent());
    }

    @Test
    void deleteMissingForCustomer() throws Exception {
        when(store.delete("acme", "missing")).thenReturn(false);
        mvc.perform(delete("/api/v1/subscriptions/missing").header(CustomerAuth.HEADER, "acme"))
                .andExpect(status().isNotFound());
    }

    @Test
    void createRejectsNonHttpUrl() throws Exception {
        mvc.perform(post("/api/v1/subscriptions")
                        .header(CustomerAuth.HEADER, "acme")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"url":"ftp://example.com/hook","filter":{}}
                            """))
                .andExpect(status().isBadRequest());
    }
}
