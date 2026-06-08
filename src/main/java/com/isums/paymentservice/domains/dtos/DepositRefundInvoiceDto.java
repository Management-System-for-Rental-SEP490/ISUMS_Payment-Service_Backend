package com.isums.paymentservice.domains.dtos;

import java.time.Instant;
import java.util.UUID;

public record DepositRefundInvoiceDto(
        UUID invoiceId,
        UUID contractId,
        Long refundAmount,
        String status,
        String paymentMethod,
        Instant paidAt,
        String note
) {
}
