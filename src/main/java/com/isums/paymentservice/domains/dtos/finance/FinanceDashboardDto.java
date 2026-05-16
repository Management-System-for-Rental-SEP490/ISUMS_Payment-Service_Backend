package com.isums.paymentservice.domains.dtos.finance;

import lombok.Builder;

import java.time.Instant;
import java.util.List;

@Builder
public record FinanceDashboardDto(
        Instant periodFrom,
        Instant periodTo,
        Instant previousPeriodFrom,
        Instant previousPeriodTo,
        FinanceSummaryDto summary,
        List<CategoryAmountDto> revenueBreakdown,
        List<CategoryAmountDto> expenseBreakdown,
        List<MonthlyPointDto> monthlyTrend,
        List<TopHouseStatDto> topHouses,
        List<TransactionDto> recentTransactions,
        List<OutstandingInvoiceDto> outstandingInvoices,
        long totalManagedHouses
) {}
