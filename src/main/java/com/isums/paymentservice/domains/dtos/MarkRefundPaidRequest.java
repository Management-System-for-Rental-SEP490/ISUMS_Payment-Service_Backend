package com.isums.paymentservice.domains.dtos;

import com.isums.paymentservice.domains.enums.RefundPaymentMethod;
import jakarta.validation.constraints.NotNull;

public record MarkRefundPaidRequest(
        @NotNull RefundPaymentMethod paymentMethod,
        String note
) {
}
