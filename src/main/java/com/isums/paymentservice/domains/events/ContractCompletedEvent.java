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
public class ContractCompletedEvent {
    private UUID contractId;
    private UUID tenantId;
    private String tenantEmail;
    private UUID houseId;
    private UUID landlordId;
    private Boolean isNewAccount;
    private Long depositAmount;
    private Long rentAmount;
    private Integer payDate;
    private Instant startAt;
    private Instant endAt;
    private Instant completedAt;
}