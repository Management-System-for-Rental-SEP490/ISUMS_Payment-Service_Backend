package com.isums.paymentservice.domains.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TerminationRequestedEvent {
    private UUID contractId;
    private UUID houseId;
    private UUID tenantId;
    private UUID invoiceId;
    private String reason;
    private String messageId;
}