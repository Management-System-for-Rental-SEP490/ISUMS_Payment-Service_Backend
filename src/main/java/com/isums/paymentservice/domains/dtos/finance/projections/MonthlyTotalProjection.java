package com.isums.paymentservice.domains.dtos.finance.projections;

public interface MonthlyTotalProjection {
    String getMonth();

    String getType();

    Long getAmount();
}
