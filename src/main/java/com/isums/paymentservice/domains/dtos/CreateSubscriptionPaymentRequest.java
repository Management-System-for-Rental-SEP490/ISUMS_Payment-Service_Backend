package com.isums.paymentservice.domains.dtos;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

/**
 * Body for {@code POST /api/payments/vnpay/subscription}. Used by
 * Notification-Service's PREMIUM upgrade flow — months drives both the
 * total amount (months × price) and the activation duration emitted in
 * the {@code payment.subscription-activated} Kafka event after IPN.
 *
 * <p>Reuses the same {@code bankCode} + {@code locale} optional fields
 * as {@link CreatePaymentRequest} so VNPay UX is consistent across all
 * payment surfaces in the app.
 */
/**
 * Two purchase shapes:
 * <ol>
 *   <li>{@code planId} present → look up the curated plan in
 *       Notification-Service (price + duration_days authoritative).</li>
 *   <li>{@code months} present (legacy) → flat 19k × months.</li>
 * </ol>
 * If both arrive {@code planId} wins; if neither, request is rejected.
 */
public record CreateSubscriptionPaymentRequest(
        java.util.UUID planId,
        @jakarta.validation.constraints.NotNull(message = "houseId is required")
        java.util.UUID houseId,
        @Min(value = 1,  message = "months must be >= 1")
        @Max(value = 24, message = "months must be <= 24")
        Integer months,
        String bankCode,
        String locale
) {
    public String localeOrDefault() {
        return locale != null && !locale.isBlank() ? locale : "vn";
    }
}
