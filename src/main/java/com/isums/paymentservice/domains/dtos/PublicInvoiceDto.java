package com.isums.paymentservice.domains.dtos;

import com.isums.paymentservice.domains.enums.InvoiceStatus;

import java.util.UUID;

public record PublicInvoiceDto(
        UUID invoiceId,
        String invoiceType,
        Long amount,
        String dueDate,
        InvoiceStatus status
) {}