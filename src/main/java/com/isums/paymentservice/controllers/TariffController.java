package com.isums.paymentservice.controllers;

import com.isums.paymentservice.Services.TariffService;
import com.isums.paymentservice.domains.dtos.ApiResponse;
import com.isums.paymentservice.domains.dtos.ApiResponses;
import com.isums.paymentservice.domains.dtos.CreateTariffConfigRequest;
import com.isums.paymentservice.domains.dtos.TariffConfigDto;
import com.isums.paymentservice.domains.dtos.TariffConfigVersionDto;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@Tag(name = "Tariff", description = "Versioned utility tariff configuration (DB-backed, immutable history)")
@RestController
@RequestMapping("/api/payments/tariffs")
@RequiredArgsConstructor
public class TariffController {

    private final TariffService tariffService;

    @Operation(
            summary = "Get electricity residential tariff",
            description = "Returns EVN residential tier pricing with effective date and source citation."
    )
    @GetMapping("/electricity/residential")
    public ApiResponse<TariffConfigDto> getElectricityResidential() {
        return ApiResponses.ok(tariffService.getElectricityResidentialTariff(), "Success");
    }

    @Operation(
            summary = "Get water residential tariff for a region",
            description = "Defaults to HCMC SAWACO. region: HCM | HN | DN."
    )
    @GetMapping("/water/residential")
    public ApiResponse<TariffConfigDto> getWaterResidential(
            @RequestParam(value = "region", required = false) String region) {
        return ApiResponses.ok(tariffService.getWaterResidentialTariff(region), "Success");
    }

    @Operation(
            summary = "Tariff version history (admin)",
            description = "Newest first. Pass metric/plan/region as query params."
    )
    @GetMapping("/history")
    @PreAuthorize("hasAnyRole('LANDLORD','MANAGER')")
    public ApiResponse<List<TariffConfigVersionDto>> history(
            @RequestParam String metric,
            @RequestParam(defaultValue = "residential") String plan,
            @RequestParam(defaultValue = "VN") String region) {
        return ApiResponses.ok(tariffService.getHistory(metric, plan, region), "Success");
    }

    @Operation(
            summary = "Publish new tariff version (LANDLORD)",
            description = "Creates immutable new row; auto-expires previously active row of same (metric,plan,region). " +
                    "Validates: tiers[] non-empty, vatRate in [0,1]."
    )
    @PostMapping
    @PreAuthorize("hasRole('LANDLORD')")
    public ApiResponse<TariffConfigVersionDto> create(
            @AuthenticationPrincipal Jwt jwt,
            @RequestBody @Valid CreateTariffConfigRequest request) {
        return ApiResponses.created(
                tariffService.createVersion(actorId(jwt), request),
                "Tariff version published");
    }

    @Operation(summary = "Manually expire a tariff version (LANDLORD)")
    @PatchMapping("/{id}/expire")
    @PreAuthorize("hasRole('LANDLORD')")
    public ApiResponse<TariffConfigVersionDto> expire(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable UUID id) {
        return ApiResponses.ok(
                tariffService.expireVersion(id, actorId(jwt)),
                "Tariff version expired");
    }

    private UUID actorId(Jwt jwt) {
        try {
            return UUID.fromString(jwt.getSubject());
        } catch (Exception ex) {
            throw new IllegalStateException("Invalid JWT subject");
        }
    }
}
