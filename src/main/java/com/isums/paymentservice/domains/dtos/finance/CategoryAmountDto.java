package com.isums.paymentservice.domains.dtos.finance;

import lombok.Builder;

@Builder
public record CategoryAmountDto(
        String category,
        long amount,
        double percent
) {}
