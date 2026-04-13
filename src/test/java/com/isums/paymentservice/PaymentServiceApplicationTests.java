package com.isums.paymentservice;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Disabled("Requires Keycloak/Postgres/Kafka/Redis/gRPC infrastructure; run as integration test with Testcontainers")
class PaymentServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}
