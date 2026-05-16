package com.isums.paymentservice.services;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentTokenService")
class PaymentTokenServiceTest {

    @Mock private StringRedisTemplate redis;
    @Mock private ValueOperations<String, String> valueOps;

    @InjectMocks private PaymentTokenService service;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "ttlDays", 7L);
    }

    @Nested
    @DisplayName("generateToken")
    class Generate {

        @Test
        @DisplayName("writes '<invoice>:<tenant>' to Redis with TTL and returns 32-hex token")
        void generates() {
            when(redis.opsForValue()).thenReturn(valueOps);
            UUID invoiceId = UUID.randomUUID();
            UUID tenantId = UUID.randomUUID();

            String token = service.generateToken(invoiceId, tenantId);

            assertThat(token).hasSize(32);
            verify(valueOps).set(
                    eq("payment:token:" + token),
                    eq(invoiceId + ":" + tenantId),
                    eq(Duration.ofDays(7)));
        }
    }

    @Nested
    @DisplayName("validateToken")
    class Validate {

        @Test
        @DisplayName("throws when token null")
        void tokenNull() {
            assertThatThrownBy(() -> service.validateToken(null, UUID.randomUUID()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("blank");
            verifyNoInteractions(redis);
        }

        @Test
        @DisplayName("throws when token blank")
        void tokenBlank() {
            assertThatThrownBy(() -> service.validateToken("   ", UUID.randomUUID()))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("throws expired-link error when Redis returns null")
        void expired() {
            when(redis.opsForValue()).thenReturn(valueOps);
            when(valueOps.get("payment:token:t1")).thenReturn(null);

            assertThatThrownBy(() -> service.validateToken("t1", UUID.randomUUID()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("expired");
        }

        @Test
        @DisplayName("throws when redis value malformed")
        void malformed() {
            when(redis.opsForValue()).thenReturn(valueOps);
            when(valueOps.get("payment:token:t2")).thenReturn("only-one-segment");

            assertThatThrownBy(() -> service.validateToken("t2", UUID.randomUUID()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("invalid");
        }

        @Test
        @DisplayName("throws when token invoice does not match requested invoice")
        void invoiceMismatch() {
            when(redis.opsForValue()).thenReturn(valueOps);
            UUID tokenInvoice = UUID.randomUUID();
            UUID tenantId = UUID.randomUUID();
            when(valueOps.get("payment:token:t3")).thenReturn(tokenInvoice + ":" + tenantId);

            assertThatThrownBy(() -> service.validateToken("t3", UUID.randomUUID()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("does not match");
        }

        @Test
        @DisplayName("passes when token matches requested invoice")
        void matches() {
            when(redis.opsForValue()).thenReturn(valueOps);
            UUID invoiceId = UUID.randomUUID();
            UUID tenantId = UUID.randomUUID();
            when(valueOps.get("payment:token:t4")).thenReturn(invoiceId + ":" + tenantId);

            service.validateToken("t4", invoiceId); // no exception
        }
    }

    @Nested
    @DisplayName("refreshTtl")
    class RefreshTtl {

        @Test
        @DisplayName("no-op when token null or blank")
        void noOp() {
            service.refreshTtl(null);
            service.refreshTtl("");
            verifyNoInteractions(redis);
        }

        @Test
        @DisplayName("extends TTL when token valid")
        void extends_() {
            service.refreshTtl("t1");
            verify(redis).expire("payment:token:t1", Duration.ofDays(7));
        }
    }

    @Nested
    @DisplayName("invalidateToken")
    class Invalidate {

        @Test
        @DisplayName("no-op when token null or blank")
        void noOp() {
            service.invalidateToken(null);
            service.invalidateToken("");
            verifyNoInteractions(redis);
        }

        @Test
        @DisplayName("deletes key when token present")
        void deletes() {
            service.invalidateToken("t1");
            verify(redis).delete("payment:token:t1");
        }
    }
}
