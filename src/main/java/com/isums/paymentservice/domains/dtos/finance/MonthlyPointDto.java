package com.isums.paymentservice.domains.dtos.finance;

import lombok.Builder;

@Builder
public record MonthlyPointDto(
        String month,
        long revenue,
        long expense,
        long profit
) {}
