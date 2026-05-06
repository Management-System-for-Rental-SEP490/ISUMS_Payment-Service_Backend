package com.isums.paymentservice.domains.events;

import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

@Builder
public record ContractCashDepositConfirmedEvent(
        UUID contractId,
        UUID tenantId,
        UUID houseId,
        Long amount,
        String receiptNumber,
        Instant paidAt,
        UUID confirmedByUserId,
        String note,
        String messageId
) {}
