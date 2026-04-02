package com.isums.paymentservice.domains.dtos;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

public record CreatePaymentRequest(
        @NotEmpty(message = "invoiceIds must not be empty")
        @Size(min = 1, message = "At least one invoiceId required")
        List<@NotBlank(message = "invoiceId must not be blank") String> invoiceIds,

        String bankCode,
        String locale
) {
    public String localeOrDefault() {
        return (locale != null && !locale.isBlank()) ? locale : "vn";
    }

    public String bankCodeOrDefault() {
        return (bankCode != null && !bankCode.isBlank()) ? bankCode : "VNBANK";
    }
}
