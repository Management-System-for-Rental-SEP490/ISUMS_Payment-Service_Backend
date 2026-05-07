package com.isums.paymentservice.domains.dtos;

import java.time.Instant;
import java.util.UUID;

public record TariffConfigVersionDto(
        UUID id,
        String metric,
        String plan,
        String region,
        String version,
        TariffConfigDto config,
        Instant effectiveFrom,
        Instant expiredAt,
        String notes,
        UUID createdBy,
        Instant createdAt,
        UUID expiredBy,
        boolean isActive
) {}
