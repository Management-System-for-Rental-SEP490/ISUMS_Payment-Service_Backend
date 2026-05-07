package com.isums.paymentservice.domains.events;

import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Value
@Builder
public class QuoteCashPaymentConfirmedEvent {
    UUID quoteId;
    UUID issueId;
    UUID tenantId;
    BigDecimal amount;
    String txnNo;
    Instant paidAt;
}
