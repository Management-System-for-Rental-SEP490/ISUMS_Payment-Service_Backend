package com.isums.paymentservice.domains.dtos;

import java.util.List;

public record TariffConfigDto(
        String metric,
        String plan,
        String region,
        String currency,
        String unit,
        List<TariffTierDto> tiers,
        double vatRate,
        double surchargeRate,
        String surchargeLabel,
        String source,
        String effectiveFrom,
        String version,
        String notes
) {}
