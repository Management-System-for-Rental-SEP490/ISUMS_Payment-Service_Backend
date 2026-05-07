package com.isums.paymentservice.domains.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CancelDepositInvoiceRequestedEvent {
    private String messageId;
    private UUID contractId;
    private UUID tenantId;
    private UUID houseId;
    private String reason;
    private Instant requestedAt;
}
