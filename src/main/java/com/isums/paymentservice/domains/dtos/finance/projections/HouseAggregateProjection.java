package com.isums.paymentservice.domains.dtos.finance.projections;

import java.util.UUID;

public interface HouseAggregateProjection {
    UUID getHouseId();

    Long getRevenue();

    Long getExpense();
}
