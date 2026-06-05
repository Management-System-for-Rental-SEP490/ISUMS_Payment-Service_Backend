package com.isums.paymentservice.domains.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContractReplacedEvent {
    private String messageId;
    private UUID oldContractId;
    private UUID newContractId;
    private UUID oldHouseId;
    private UUID newHouseId;
    private UUID tenantId;
    private boolean keepHouseUnavailable;
    private String depositHandling;
    private Long transferredDepositAmount;
    private String reason;
    private Instant replacedAt;
    private Instant newHandoverDate;
}
