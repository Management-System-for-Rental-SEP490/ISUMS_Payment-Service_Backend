package com.isums.paymentservice.domains.dtos.finance;

import lombok.Builder;

import java.util.UUID;

@Builder
public record RegionFinanceSummaryDto(
        UUID regionId,
        long totalHouses,
        long totalRevenue,
        long totalExpense,
        long netProfit,
        long outstandingAmount,
        int outstandingCount
) {}
