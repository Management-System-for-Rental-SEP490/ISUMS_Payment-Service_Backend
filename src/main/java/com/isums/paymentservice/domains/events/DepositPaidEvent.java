package com.isums.paymentservice.domains.events;

import com.isums.paymentservice.domains.enums.InvoiceType;
import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

@Builder
public record DepositPaidEvent(
        UUID invoiceId,
        UUID contractId,
        UUID tenantId,
        UUID houseId,
        Long amount,
        InvoiceType invoiceType,
        String txnNo,
        Instant paidAt
) {}