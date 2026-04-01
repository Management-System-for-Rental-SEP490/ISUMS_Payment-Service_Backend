package com.isums.paymentservice.domains.dtos;

import jakarta.validation.constraints.NotBlank;

import java.util.List;

public record CreatePaymentRequest(

        @NotBlank List<String> invoiceIds,

        String bankCode,

        String locale
) {
    public String localeOrDefault() {
        return locale != null && !locale.isBlank() ? locale : "vn";
    }
}
