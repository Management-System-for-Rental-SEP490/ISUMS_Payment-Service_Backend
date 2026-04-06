package com.isums.paymentservice.domains.dtos;

import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.validation.constraints.AssertTrue;

import java.util.List;

public record CreatePaymentRequest(
        List<String> invoiceIds,
        String quoteId,
        String bankCode,
        String locale
) {
    @JsonIgnore
    @AssertTrue(message = "Phải cung cấp invoiceIds hoặc quoteId, không cả hai")
    public boolean isValidPaymentTarget() {
        boolean hasInvoices = invoiceIds != null && !invoiceIds.isEmpty();
        boolean hasQuote    = quoteId != null && !quoteId.isBlank();
        return hasInvoices ^ hasQuote;
    }

    @JsonIgnore
    public boolean isQuotePayment() {
        return quoteId != null && !quoteId.isBlank();
    }

    public String localeOrDefault() {
        return locale != null && !locale.isBlank() ? locale : "vn";
    }

    public String bankCodeOrDefault() {
        return bankCode != null ? bankCode : "VNPAY_BANK";
    }
}