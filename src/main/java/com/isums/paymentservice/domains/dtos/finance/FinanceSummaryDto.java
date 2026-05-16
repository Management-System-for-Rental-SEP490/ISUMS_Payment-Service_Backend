package com.isums.paymentservice.domains.dtos.finance;

import lombok.Builder;

@Builder
public record FinanceSummaryDto(
        long totalRevenue,
        long totalExpense,
        long netProfit,
        long outstandingAmount,
        int outstandingCount,
        Long previousRevenue,
        Long previousExpense,
        Long previousNetProfit,
        Double revenueChangePercent,
        Double expenseChangePercent,
        Double netProfitChangePercent
) {}
