package com.isums.paymentservice.domains.dtos;

import java.util.UUID;

public record IssueItemDto(
        UUID id,
        String itemName,
        Long price
) {
}
