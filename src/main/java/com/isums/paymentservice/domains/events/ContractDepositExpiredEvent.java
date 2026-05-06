package com.isums.paymentservice.domains.events;

import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

@Builder
public record ContractDepositExpiredEvent(
        UUID contractId,
        UUID tenantId,
        UUID houseId,
        UUID landlordId,
        String tenantEmail,
        String tenantName,
        String contractNo,
        Long depositAmount,
        Instant depositDueAt,
        Instant expiredAt,
        String messageId
) {}
