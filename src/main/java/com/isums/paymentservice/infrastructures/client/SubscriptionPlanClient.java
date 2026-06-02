package com.isums.paymentservice.infrastructures.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.util.UUID;

/**
 * Lightweight REST client for Notification-Service's plan catalogue.
 * Used by Payment-Service to fetch authoritative price + duration_days
 * for a {@code planId} so the FE can never lie about the amount.
 *
 * <p>Forwards the caller's bearer token so the Notification-Service
 * security filter sees the same authenticated user — the public list
 * endpoint accepts any authenticated user.
 *
 * <p>Builds its own {@link RestClient} via {@code RestClient.create()}
 * rather than injecting {@code RestClient.Builder} — the auto-configured
 * Builder bean isn't always present in this build's dependency set, so
 * a self-built client keeps the wiring simple.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SubscriptionPlanClient {

    private final ObjectMapper objectMapper;

    @Value("${app.notification-service.base-url:http://localhost:8085}")
    private String baseUrl;

    private RestClient client;

    @PostConstruct
    void init() {
        client = RestClient.create(baseUrl);
        log.info("[SubscriptionPlanClient] init baseUrl={}", baseUrl);
    }

    public PlanInfo fetchPlan(UUID planId, String bearerToken) {
        try {
            String body = client.get()
                    .uri("/api/notifications/subscriptions/plans/{id}", planId)
                    .header(HttpHeaders.AUTHORIZATION,
                            bearerToken != null ? bearerToken : "")
                    .retrieve()
                    .body(String.class);

            JsonNode root = objectMapper.readTree(body == null ? "{}" : body);
            JsonNode data = root.path("data");
            if (data.isMissingNode() || data.isNull()) {
                throw new IllegalStateException("Plan not found: " + planId);
            }
            return new PlanInfo(
                    UUID.fromString(data.path("id").asText()),
                    data.path("code").asText(""),
                    data.path("durationDays").asInt(0),
                    data.path("priceVnd").asInt(0),
                    data.path("isActive").asBoolean(false)
            );
        } catch (Exception e) {
            log.error("[PlanClient] fetch failed planId={}: {}", planId, e.getMessage(), e);
            throw new IllegalStateException(
                    "Không thể lấy thông tin gói đăng ký. Vui lòng thử lại sau.", e);
        }
    }

    public record PlanInfo(UUID id, String code, int durationDays, int priceVnd, boolean active) {}
}
