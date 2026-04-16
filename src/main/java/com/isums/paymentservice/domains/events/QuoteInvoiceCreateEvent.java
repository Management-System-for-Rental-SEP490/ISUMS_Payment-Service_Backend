package com.isums.paymentservice.domains.events;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.UUID;

@Builder
@Data
public class QuoteInvoiceCreateEvent {
    private UUID quoteId;
    private UUID issueId;
    private UUID tenantId;
    private UUID houseId;
    private BigDecimal totalPrice;
    private String tenantEmail;
}
