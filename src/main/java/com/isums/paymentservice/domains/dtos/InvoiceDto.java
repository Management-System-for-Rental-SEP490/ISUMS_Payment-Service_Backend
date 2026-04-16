package com.isums.paymentservice.domains.dtos;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;

import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceDto(
        UUID id,
        UUID contractId,
        UUID houseId,
        UUID tenantId,
        InvoiceType type,
        String periodKey,
        Long baseAmount,
        Long serviceAmount,
        Long penaltyAmount,
        Long totalAmount,
        InvoiceStatus status,
        Instant dueDate,
        Instant paidAt,
        Instant createdAt
) {}