package com.isums.paymentservice.domains.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MapUserToHouseEvent {
    private UUID userId;
    private UUID houseId;
    private Instant handoverDate;
}
