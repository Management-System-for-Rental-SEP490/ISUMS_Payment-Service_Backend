package com.isums.paymentservice.domains.dtos;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record CreateTariffConfigRequest(
        @NotBlank @Size(max = 40) String metric,
        @NotBlank @Size(max = 40) String plan,
        @NotBlank @Size(max = 40) String region,
        @NotBlank @Size(max = 80) String version,
        JsonNode configJson,
        @Size(max = 1000) String notes
) {}
