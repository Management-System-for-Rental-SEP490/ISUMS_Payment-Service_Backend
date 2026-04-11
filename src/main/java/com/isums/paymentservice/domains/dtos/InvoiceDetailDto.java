package com.isums.paymentservice.domains.dtos;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.enums.PaymentMethod;
import com.isums.paymentservice.domains.enums.PaymentStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record InvoiceDetailDto(
        UUID id,
        UUID contractId,
        UUID quoteId,
        InvoiceType type,
        String periodKey,
        Long baseAmount,
        Long serviceAmount,
        Long penaltyAmount,
        Long totalAmount,
        InvoiceStatus status,
        Instant dueDate,
        Instant paidAt,
        Instant createdAt,

        UUID tenantId,
        String tenantName,
        String tenantEmail,
        String tenantPhone,

        UUID houseId,
        String houseName,
        String houseAddress,

        List<PaymentRecord> payments,

        UUID issueId,
        List<IssueItemDto> issueItems
) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record PaymentRecord(
            UUID id,
            Long amount,
            PaymentMethod method,
            PaymentStatus status,
            String gatewayTxnId,
            Instant paidAt,
            Instant createdAt
    ) {}
}