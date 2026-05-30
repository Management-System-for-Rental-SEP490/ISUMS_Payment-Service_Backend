package com.isums.paymentservice.controllers;

import com.isums.paymentservice.domains.dtos.ApiResponse;
import com.isums.paymentservice.domains.dtos.ApiResponses;
import com.isums.paymentservice.domains.dtos.finance.FinanceDashboardDto;
import com.isums.paymentservice.services.FinanceDashboardService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@Tag(name = "Finance", description = "Finance dashboard for landlord and manager: revenue, expense, profit and outstanding receivables")
@RestController
@RequestMapping("/api/payments/finance")
@RequiredArgsConstructor
public class FinanceController {

    private final FinanceDashboardService financeDashboardService;

    @Operation(
            summary = "Finance dashboard",
            description = "Returns aggregated revenue, expense, net profit, monthly trend, top houses, recent transactions and outstanding invoices for the calling actor. Landlord sees all houses; manager only sees houses in regions they manage."
    )
    @GetMapping("/dashboard")
    @PreAuthorize("hasAnyRole('LANDLORD', 'MANAGER')")
    public ApiResponse<FinanceDashboardDto> getDashboard(
            @AuthenticationPrincipal Jwt jwt,
            @RequestParam("from") String from,
            @RequestParam("to") String to,
            @RequestParam(value = "compare", defaultValue = "false") boolean compare,
            @RequestParam(value = "regionId", required = false) UUID regionId) {
        FinanceDashboardDto dto = financeDashboardService.getDashboard(jwt.getSubject(), from, to, compare, regionId);
        return ApiResponses.ok(dto, "Finance dashboard");
    }
}
