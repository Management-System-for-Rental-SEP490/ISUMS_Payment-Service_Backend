package com.isums.paymentservice.domains.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public record IssueQuoteResponse(
        UUID id,
        UUID issueId,
        UUID tenantId,
        BigDecimal totalPrice,
        String status
) {}