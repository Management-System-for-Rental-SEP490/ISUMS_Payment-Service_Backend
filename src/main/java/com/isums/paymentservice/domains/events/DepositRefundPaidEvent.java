package com.isums.paymentservice.domains.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DepositRefundPaidEvent {
    private UUID contractId;
    private UUID houseId;
    private UUID tenantId;
    private Long refundAmount;
    private Instant paidAt;
    private String messageId;
}
