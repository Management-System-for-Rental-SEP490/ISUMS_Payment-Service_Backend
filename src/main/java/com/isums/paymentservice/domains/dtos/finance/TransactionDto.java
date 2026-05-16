package com.isums.paymentservice.domains.dtos.finance;

import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

@Builder
public record TransactionDto(
        UUID invoiceId,
        UUID contractId,
        UUID houseId,
        String houseName,
        InvoiceType type,
        InvoiceStatus status,
        long amount,
        Instant paidAt,
        Instant dueDate,
        String tenantEmail
) {}
