package com.isums.paymentservice.domains.dtos;

import java.util.UUID;

public record IssueQuoteItemResponse(
        UUID id,
        String itemName,
        Long price
) {
}
