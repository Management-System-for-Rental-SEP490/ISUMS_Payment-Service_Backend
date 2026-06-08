package com.isums.paymentservice.controllers;

import com.isums.paymentservice.domains.dtos.*;
import com.isums.paymentservice.infrastructures.Abtracts.PaymentService;
import com.isums.paymentservice.services.PaymentTokenService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Tag(name = "Payment", description = "Manage VNPay payments and rental invoices")
@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentTokenService paymentTokenService;

    @Operation(
            summary = "Get the tenant's invoice list",
            description = "Returns the current tenant's invoices ordered by dueDate ascending. " +
                    "Pass houseId to filter by a specific house."
    )
    @GetMapping("/invoices")
    public ApiResponse<List<InvoiceDto>> getMyInvoices(
            @AuthenticationPrincipal Jwt jwt,
            @RequestParam(required = false) UUID houseId) {

        String keycloakId = jwt.getSubject();
        return ApiResponses.ok(paymentService.getMyInvoices(keycloakId, houseId), "Success");
    }

    @Operation(
            summary = "Get available payment methods",
            description = "Returns the list of payment methods whitelisted for the issue flow: cash and bank transfer."
    )
    @GetMapping("/methods")
    public ApiResponse<List<PaymentMethodOptionDto>> getAvailablePaymentMethods() {
        return ApiResponses.ok(paymentService.getAvailablePaymentMethods(), "Success");
    }

    @Operation(
            summary = "View invoice details",
            description = "Tenants can only view their own invoices. Returns full tenant information, house, and payment history."
    )
    @GetMapping("/invoices/{invoiceId}")
    public ApiResponse<InvoiceDetailDto> getInvoiceById(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable UUID invoiceId) {

        String keycloakId = jwt.getSubject();
        return ApiResponses.ok(paymentService.getInvoiceById(invoiceId, keycloakId), "Success to get invoice detail");
    }

    @Operation(
            summary = "Create a VNPay payment link",
            description = """
                    Supports 2 flows:
                    - **Invoice**: provide `invoiceIds` (one or more rental/deposit invoices).
                    - **Quote**: provide `quoteId` (an approved repair quote, ticket in WAITING_PAYMENT status).
                    
                    Only one of the two may be provided, not both.
                    """
    )
    @PostMapping("/vnpay")
    public ApiResponse<String> createPaymentLink(
            @AuthenticationPrincipal Jwt jwt,
            @RequestBody @Valid CreatePaymentRequest request,
            HttpServletRequest httpRequest) {

        String keycloakId = jwt.getSubject();
        return ApiResponses.ok(paymentService.createPaymentVNPayLink(request, httpRequest, keycloakId), "VNPay payment link");
    }

    @Operation(
            summary = "[VNPay] IPN callback",
            description = "Endpoint that VNPay calls after payment. JWT is not required."
    )
    @GetMapping("/vnpay/ipn")
    public VNPayIpnResponse handleIpn(VNPayIpnRequest ipn) {
        return paymentService.handleIpn(ipn);
    }

    @Operation(
            summary = "Create a VNPay link for Notification PREMIUM subscription",
            description = """
                    Distinct from {@code POST /vnpay} (which pays an invoice
                    or repair quote). Body specifies how many months of
                    PREMIUM to buy; total amount = months × monthly price.

                    On IPN success the service emits Kafka
                    {@code payment.subscription-activated} which Notification
                    Service consumes to flip the user's tier.
                    """
    )
    @PostMapping("/vnpay/subscription")
    public ApiResponse<String> createSubscriptionPaymentLink(
            @AuthenticationPrincipal Jwt jwt,
            @RequestBody @Valid CreateSubscriptionPaymentRequest request,
            HttpServletRequest httpRequest) {

        String keycloakId = jwt.getSubject();
        return ApiResponses.ok(
                paymentService.createSubscriptionPaymentLink(request, httpRequest, keycloakId),
                "VNPay subscription payment link"
        );
    }

    @Operation(
            summary = "[VNPay] Return URL redirect",
            description = "The browser is redirected here after a VNPay payment. JWT is not required. Redirects onward to the frontend."
    )
    @GetMapping("/vnpay/return")
    public ResponseEntity<Void> handleReturn(VNPayIpnRequest params) {
        String redirectUrl = paymentService.handleReturn(params);
        return ResponseEntity.status(HttpStatus.FOUND)
                .header("Location", redirectUrl)
                .build();
    }

    @Operation(
            summary = "[OUTSYSTEM] View invoice details",
            description = "Tenant uses the link from email to view the invoice without logging in."
    )
    @GetMapping("/outsystem/invoices/{invoiceId}")
    public ApiResponse<PublicInvoiceDto> getPublicInvoice(
            @PathVariable UUID invoiceId,
            @RequestParam String token) {

        paymentTokenService.validateToken(token, invoiceId);
        return ApiResponses.ok(paymentService.getPublicInvoice(invoiceId), "Success");
    }

    @Operation(
            summary = "[OUTSYSTEM] Create a payment link from email",
            description = "Creates a VNPay link from an email notification (no login required)."
    )
    @PostMapping("/outsystem/vnpay")
    public ApiResponse<String> createPaymentLinkOutsystem(
            @RequestParam UUID invoiceId,
            @RequestParam(required = false) String bankCode,
            @RequestParam(required = false) String locale,
            HttpServletRequest httpRequest) {

        return ApiResponses.ok(
                paymentService.createPaymentVNPayLinkOutsystem(invoiceId, bankCode, locale, httpRequest),
                "VNPay payment link");
    }

    @Operation(summary = "Resend the payment notification email")
    @PostMapping("/invoices/{invoiceId}/resend")
    public ApiResponse<Void> resendNotification(@PathVariable UUID invoiceId) {
        paymentService.resendPaymentNotification(invoiceId);
        return ApiResponses.ok(null, "Payment notification has been resent");
    }

    @GetMapping("/contracts/{contractId}/deposit-refund-invoice")
    @PreAuthorize("hasAnyRole('LANDLORD', 'MANAGER')")
    public ApiResponse<DepositRefundInvoiceDto> getDepositRefundInvoice(@PathVariable UUID contractId) {
        return ApiResponses.ok(paymentService.getDepositRefundInvoice(contractId), "Success");
    }

    @PutMapping("/{invoiceId}/mark-refund-paid")
    @PreAuthorize("hasAnyRole('LANDLORD', 'MANAGER')")
    public ApiResponse<Void> markRefundPaid(
            @PathVariable UUID invoiceId,
            @RequestBody @Valid MarkRefundPaidRequest req,
            @AuthenticationPrincipal Jwt jwt) {
        paymentService.markDepositRefundPaid(invoiceId, req, jwt.getSubject());
        return ApiResponses.ok(null, "Deposit refund confirmed");
    }
}