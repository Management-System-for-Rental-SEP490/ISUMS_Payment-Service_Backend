package com.isums.paymentservice.domains.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ForceTerminationEvent {
    private UUID contractId;
    private UUID houseId;
    private UUID tenantId;
    private String reason;
    private UUID actorId;
    private Instant terminatedAt;
    private String messageId;
}
