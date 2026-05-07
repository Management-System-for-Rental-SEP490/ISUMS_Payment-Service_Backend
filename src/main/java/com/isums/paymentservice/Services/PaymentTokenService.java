package com.isums.paymentservice.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentTokenService {

    private final StringRedisTemplate redis;

    @Value("${app.payment.token-ttl-days:7}")
    private long ttlDays;

    private static final String PREFIX = "payment:token:";

    public String generateToken(UUID invoiceId, UUID tenantId) {
        String token = UUID.randomUUID().toString().replace("-", "");
        redis.opsForValue().set(
                PREFIX + token,
                invoiceId + ":" + tenantId,
                Duration.ofDays(ttlDays));
        log.info("[PaymentToken] Generated invoiceId={} ttl={}d", invoiceId, ttlDays);
        return token;
    }

    public void validateToken(String token, UUID invoiceId) {
        if (token == null || token.isBlank())
            throw new IllegalArgumentException("Payment token must not be blank.");

        String value = redis.opsForValue().get(PREFIX + token);
        if (value == null)
            throw new IllegalArgumentException("Payment link has expired. Please contact the landlord for a new link.");

        String[] parts = value.split(":");
        if (parts.length != 2)
            throw new IllegalArgumentException("Token is invalid.");

        UUID tokenInvoiceId = UUID.fromString(parts[0]);
        UUID tenantId       = UUID.fromString(parts[1]);

        if (!tokenInvoiceId.equals(invoiceId))
            throw new IllegalArgumentException("Token does not match the invoice.");

    }

    public void refreshTtl(String token) {
        if (token == null || token.isBlank()) return;
        redis.expire(PREFIX + token, Duration.ofDays(ttlDays));
    }

    public void invalidateToken(String token) {
        if (token == null || token.isBlank()) return;
        redis.delete(PREFIX + token);
        log.info("[PaymentToken] Invalidated token");
    }
}