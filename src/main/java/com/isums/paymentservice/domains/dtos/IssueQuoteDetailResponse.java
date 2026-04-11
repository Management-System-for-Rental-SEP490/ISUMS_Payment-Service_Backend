package com.isums.paymentservice.domains.dtos;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record IssueQuoteDetailResponse(
        UUID id,
        UUID issueId,
        UUID tenantId,
        BigDecimal totalPrice,
        String status,
        List<IssueQuoteItemResponse> items
) {
}
