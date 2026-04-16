package com.isums.paymentservice.controllers;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@DisplayName("PaymentOpenApiForwardController")
class PaymentOpenApiForwardControllerTest {

    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.standaloneSetup(new PaymentOpenApiForwardController()).build();
    }

    @Test
    @DisplayName("forwards prefixed api-docs root to springdoc root")
    void forwardsRoot() throws Exception {
        mvc.perform(get("/api/payments/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/v3/api-docs"));
    }

    @Test
    @DisplayName("forwards prefixed nested api-docs path to springdoc nested path")
    void forwardsNestedPath() throws Exception {
        mvc.perform(get("/api/payments/v3/api-docs/swagger-config"))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/v3/api-docs/swagger-config"));
    }
}
