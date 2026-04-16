package com.isums.paymentservice.controllers;

import com.isums.paymentservice.configurations.OpenApiConfig;
import com.isums.paymentservice.configurations.OpenApiStripServersConfig;
import com.isums.paymentservice.configurations.SecurityConfig;
import com.isums.paymentservice.exceptions.GlobalExceptionHandler;
import com.isums.paymentservice.infrastructures.Abtracts.PaymentService;
import com.isums.paymentservice.services.PaymentTokenService;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springdoc.core.configuration.SpringDocConfiguration;
import org.springdoc.webmvc.core.configuration.SpringDocWebMvcConfiguration;
import org.springdoc.webmvc.ui.SwaggerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.grpc.client.autoconfigure.CompositeChannelFactoryAutoConfiguration;
import org.springframework.boot.grpc.client.autoconfigure.GrpcChannelFactoryConfigurations;
import org.springframework.boot.grpc.client.autoconfigure.GrpcClientAutoConfiguration;
import org.springframework.boot.grpc.client.autoconfigure.GrpcClientObservationAutoConfiguration;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerAutoConfiguration;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerFactoryAutoConfiguration;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerFactoryConfigurations;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerObservationAutoConfiguration;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerReflectionAutoConfiguration;
import org.springframework.boot.grpc.server.autoconfigure.exception.GrpcExceptionHandlerAutoConfiguration;
import org.springframework.boot.grpc.server.autoconfigure.health.GrpcServerHealthAutoConfiguration;
import org.springframework.boot.grpc.server.autoconfigure.security.GrpcSecurityAutoConfiguration;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(
        classes = PaymentOpenApiDocsTest.TestApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.MOCK,
        properties = {
                "springdoc.api-docs.path=/v3/api-docs",
                "springdoc.swagger-ui.enabled=true",
                "eureka.client.enabled=false",
                "spring.cloud.discovery.enabled=false"
        }
)
@AutoConfigureMockMvc
@DisplayName("Payment OpenAPI docs")
class PaymentOpenApiDocsTest {

    @Autowired
    private MockMvc mvc;

    @MockitoBean
    private PaymentService paymentService;

    @MockitoBean
    private PaymentTokenService paymentTokenService;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    @DisplayName("GET /v3/api-docs returns generated OpenAPI document")
    void returnsOpenApiDocument() throws Exception {
        mvc.perform(get("/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.openapi").value(Matchers.startsWith("3.")))
                .andExpect(jsonPath("$.paths['/api/payments/invoices']").exists());
    }

    @Test
    @DisplayName("GET /api/payments/v3/api-docs forwards to generated OpenAPI document")
    void returnsForwardedOpenApiDocument() throws Exception {
        mvc.perform(get("/api/payments/v3/api-docs"))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith("application/json"))
                .andExpect(jsonPath("$.openapi").value(Matchers.startsWith("3.")))
                .andExpect(jsonPath("$.paths['/api/payments/invoices']").exists());
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = {
            DataSourceAutoConfiguration.class,
            HibernateJpaAutoConfiguration.class,
            JpaRepositoriesAutoConfiguration.class,
            RedisAutoConfiguration.class,
            RedisRepositoriesAutoConfiguration.class,
            KafkaAutoConfiguration.class,
            GrpcClientAutoConfiguration.class,
            CompositeChannelFactoryAutoConfiguration.class,
            GrpcChannelFactoryConfigurations.class,
            GrpcClientObservationAutoConfiguration.class,
            GrpcServerAutoConfiguration.class,
            GrpcServerFactoryAutoConfiguration.class,
            GrpcServerFactoryConfigurations.class,
            GrpcServerObservationAutoConfiguration.class,
            GrpcServerReflectionAutoConfiguration.class,
            GrpcExceptionHandlerAutoConfiguration.class,
            GrpcServerHealthAutoConfiguration.class,
            GrpcSecurityAutoConfiguration.class
    })
    @ImportAutoConfiguration({
            SpringDocConfiguration.class,
            SpringDocWebMvcConfiguration.class,
            SwaggerConfig.class
    })
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
