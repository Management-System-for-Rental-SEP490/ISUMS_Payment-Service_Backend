package com.isums.paymentservice.controllers;

import com.isums.paymentservice.domains.dtos.*;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.infrastructures.Abtracts.PaymentService;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.isums.paymentservice.services.PaymentTokenService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@Tag(name = "Payment", description = "Quản lý thanh toán VNPay và hóa đơn thuê nhà")
@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentService paymentService;
    private final PaymentTokenService paymentTokenService;
    private final RentalInvoiceRepository invoiceRepository;

    @Operation(
            summary = "Lấy danh sách hóa đơn của tenant",
            description = "Trả về tất cả hóa đơn của tenant hiện tại theo thứ tự dueDate tăng dần."
    )
    @GetMapping("/invoices")
//    @PreAuthorize("hasRole('TENANT')")
    public ApiResponse<List<RentalInvoice>> getMyInvoices(@AuthenticationPrincipal Jwt jwt) {
        UUID tenantId = UUID.fromString(jwt.getSubject());
        return ApiResponses.ok(
                invoiceRepository.findByTenantIdOrderByDueDateAsc(tenantId),
                "Success");
    }

    @Operation(summary = "Tạo link thanh toán VNPay (tenant đã login)")
    @PostMapping("/vnpay")
//    @PreAuthorize("hasRole('TENANT')")
    public ApiResponse<String> createPaymentLink(@AuthenticationPrincipal Jwt jwt, @RequestBody @Valid CreatePaymentRequest request, HttpServletRequest httpRequest) {
        return ApiResponses.ok(paymentService.createPaymentVNPayLink(request, httpRequest), "Link thanh toán VNPay");
    }

    @Operation(
            summary = "[OUTSYSTEM] Xem chi tiết hóa đơn",
            description = """
                    Tenant dùng link từ email để xem hóa đơn mà không cần đăng nhập.
                    """
    )
    @GetMapping("/outsystem/invoices/{invoiceId}")
    public ApiResponse<PublicInvoiceDto> getInvoiceOutsystem(@PathVariable UUID invoiceId, @RequestParam String token) {

        paymentTokenService.validateToken(token, invoiceId);
        paymentTokenService.refreshTtl(token);
        return ApiResponses.ok(paymentService.getPublicInvoice(invoiceId), "Success");
    }

    @Operation(
            summary = "[OUTSYSTEM] Tạo link thanh toán VNPay",
            description = """
                    Tenant dùng link từ email để thanh toán mà không cần đăng nhập.
                    
                    **Response:** VNPay payment URL để redirect tenant.
                    """
    )
    @PostMapping("/outsystem/vnpay")
    public ApiResponse<String> createPaymentOutsystem(@RequestParam UUID invoiceId, @RequestParam String token, @RequestBody(required = false) CreatePaymentRequest request,
                                                      HttpServletRequest httpRequest) {

        paymentTokenService.validateToken(token, invoiceId);

        return ApiResponses.ok(
                paymentService.createPaymentVNPayLinkOutsystem(
                        invoiceId,
                        request != null ? request.bankCode() : null,
                        request != null ? request.locale() : null,
                        httpRequest),
                "Link thanh toán VNPay");
    }

    @Operation(
            summary = "VNPay IPN callback",
            description = "Endpoint VNPay gọi để thông báo kết quả thanh toán. Không gọi trực tiếp từ FE."
    )
    @GetMapping("/vnpay/ipn")
    public VNPayIpnResponse handleIpn(VNPayIpnRequest ipn) {
        return paymentService.handleIpn(ipn);
    }

    @Operation(
            summary = "VNPay Return URL",
            description = "VNPay redirect tenant về đây sau khi thanh toán. FE đọc query params để hiển thị kết quả."
    )
    @GetMapping("/vnpay/return")
    public ApiResponse<String> handleReturn(VNPayIpnRequest ipn) {
        boolean success = "00".equals(ipn.getVnp_ResponseCode());
        return ApiResponses.ok(
                success ? "PAYMENT_SUCCESS" : "PAYMENT_FAILED",
                success ? "Thanh toán thành công" : "Thanh toán thất bại");
    }

    @PostMapping("/invoices/{invoiceId}/resend")
//    @PreAuthorize("hasAnyRole('LANDLORD','MANAGER')")
    public ApiResponse<Void> resendPaymentNotification(@PathVariable UUID invoiceId) {
        paymentService.resendPaymentNotification(invoiceId);
        return ApiResponses.ok(null, "Đã gửi lại thông báo thanh toán");
    }

    private String getClientIp(HttpServletRequest request) {
        String xff = request.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) return xff.split(",")[0].trim();
        return request.getRemoteAddr();
    }
}