package com.isums.paymentservice.controllers;

import com.isums.paymentservice.configurations.OpenApiConfig;
import com.isums.paymentservice.configurations.OpenApiStripServersConfig;
import com.isums.paymentservice.configurations.SecurityConfig;
import com.isums.paymentservice.exceptions.GlobalExceptionHandler;
import com.isums.paymentservice.infrastructures.Abtracts.PaymentService;
import com.isums.paymentservice.services.PaymentTokenService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.beans.factory.annotation.Value;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(
        classes = PaymentOpenApiDocsTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "springdoc.api-docs.path=/v3/api-docs",
                "springdoc.swagger-ui.enabled=true",
                "spring.grpc.server.port=0",
                "eureka.client.enabled=false",
                "spring.kafka.listener.auto-startup=false",
                "spring.task.scheduling.enabled=false"
        }
)
@DisplayName("Payment OpenAPI docs")
class PaymentOpenApiDocsTest {

    private final HttpClient httpClient = HttpClient.newHttpClient();

    @Value("${local.server.port}")
    private int port;

    @MockitoBean
    private PaymentService paymentService;

    @MockitoBean
    private PaymentTokenService paymentTokenService;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    @DisplayName("GET /v3/api-docs returns generated OpenAPI document on repeated requests")
    void returnsOpenApiDocument() throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/v3/api-docs"))
                .GET()
                .build();

        HttpResponse<String> first =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> second =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, first.statusCode());
        assertTrue(first.headers().firstValue("Content-Type").orElse("").contains("application/json"));
        assertTrue(first.body().contains("\"openapi\""));
        assertTrue(first.body().contains("\"/api/payments/invoices\""));

        assertEquals(200, second.statusCode());
        assertTrue(second.headers().firstValue("Content-Type").orElse("").contains("application/json"));
        assertTrue(second.body().contains("\"openapi\""));
        assertTrue(second.body().contains("\"/api/payments/invoices\""));
    }

    @TestConfiguration
    @EnableAutoConfiguration
    @Import({
            PaymentController.class,
            PaymentOpenApiForwardController.class,
            SecurityConfig.class,
            OpenApiConfig.class,
            OpenApiStripServersConfig.class,
            GlobalExceptionHandler.class
    })
    static class TestApp {
    }
}
