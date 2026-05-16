package com.isums.paymentservice.domains.dtos.finance;

import lombok.Builder;

import java.util.UUID;

@Builder
public record TopHouseStatDto(
        UUID houseId,
        String houseName,
        String address,
        long revenue,
        long expense,
        long profit
) {}
