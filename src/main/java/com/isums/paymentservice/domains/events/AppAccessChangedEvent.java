package com.isums.paymentservice.domains.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AppAccessChangedEvent {
    private UUID tenantId;
    private UUID houseId;
    private UUID contractId;
    private boolean restricted;
    private String reason;
    private String messageId;
}
