package com.isums.paymentservice.domains.enums;

public enum ReferenceType {
    HOUSE,
    ISSUE,
    QUOTE,
    INVOICE,
    MAINTENANCE,
    UTILITY,
    MULTI_INVOICE,
    /**
     * Notification-Service PREMIUM subscription. The {@code reference_id}
     * stays null for this purpose; we carry the {@code months} via a
     * dedicated request DTO and emit Kafka {@code payment.subscription-activated}
     * after the IPN confirms.
     */
    SUBSCRIPTION
}