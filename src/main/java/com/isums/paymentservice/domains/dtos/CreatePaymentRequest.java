package com.isums.paymentservice.domains.dtos;

import jakarta.validation.constraints.NotBlank;

public record CreatePaymentRequest(

        @NotBlank String invoiceId,

        String bankCode,

        String locale
) {
    public String localeOrDefault() {
        return locale != null && !locale.isBlank() ? locale : "vn";
    }
}
