package com.isums.paymentservice.domains.dtos.finance;

import com.isums.paymentservice.domains.enums.InvoiceType;
import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

@Builder
public record OutstandingInvoiceDto(
        UUID invoiceId,
        UUID contractId,
        UUID houseId,
        String houseName,
        InvoiceType type,
        long amount,
        Instant dueDate,
        long daysOverdue,
        String tenantEmail
) {}
