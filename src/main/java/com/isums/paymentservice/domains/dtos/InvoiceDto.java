package com.isums.paymentservice.domains.dtos;

import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;

import java.time.Instant;
import java.util.UUID;

public record InvoiceDto(
        UUID id,
        UUID contractId,
        UUID houseId,
        InvoiceType type,
        String periodKey,
        Long totalAmount,
        Long baseAmount,
        Long penaltyAmount,
        InvoiceStatus status,
        Instant dueDate,
        Instant paidAt,
        Instant createdAt
) {
}
