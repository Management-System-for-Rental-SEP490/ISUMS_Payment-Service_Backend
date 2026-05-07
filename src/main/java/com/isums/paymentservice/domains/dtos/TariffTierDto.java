package com.isums.paymentservice.domains.dtos;

public record TariffTierDto(
        int index,
        String label,
        double fromUnit,
        Double toUnit,
        long pricePerUnitVnd
) {}
