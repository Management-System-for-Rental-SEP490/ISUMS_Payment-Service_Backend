package com.isums.paymentservice.domains.events;

import com.isums.paymentservice.domains.enums.InvoiceType;
import lombok.Builder;

import java.time.Instant;
import java.util.UUID;

/**
 * Event gửi qua Kafka sau khi thanh toán thành công (IPN).
 * Topic: payment-paid-topic
 *
 * <p>Consumers:
 * - notification-service: gửi email receipt (mọi loại invoice)
 * - user-service: kích hoạt tài khoản (chỉ khi DEPOSIT)
 */
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