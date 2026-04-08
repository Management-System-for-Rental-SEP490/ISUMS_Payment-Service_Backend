package com.isums.paymentservice.controllers;

import com.isums.paymentservice.domains.dtos.*;
import com.isums.paymentservice.infrastructures.Abtracts.PaymentService;
import com.isums.paymentservice.services.PaymentTokenService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
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

    @Operation(
            summary = "Lấy danh sách hóa đơn của tenant",
            description = "Trả về hóa đơn của tenant hiện tại theo thứ tự dueDate tăng dần. " +
                    "Truyền houseId để lọc theo nhà cụ thể."
    )
    @GetMapping("/invoices")
    public ApiResponse<List<InvoiceDto>> getMyInvoices(
            @AuthenticationPrincipal Jwt jwt,
            @RequestParam(required = false) UUID houseId) {

        String keycloakId = jwt.getSubject();
        return ApiResponses.ok(paymentService.getMyInvoices(keycloakId, houseId), "Success");
    }

    @Operation(
            summary = "Xem chi tiết hóa đơn",
            description = "Tenant chỉ xem được hóa đơn của chính mình. Trả về đầy đủ thông tin tenant, nhà, lịch sử thanh toán."
    )
    @GetMapping("/invoices/{invoiceId}")
    public ApiResponse<InvoiceDetailDto> getInvoiceById(
            @AuthenticationPrincipal Jwt jwt,
            @PathVariable UUID invoiceId) {

        String keycloakId = jwt.getSubject();
        return ApiResponses.ok(paymentService.getInvoiceById(invoiceId, keycloakId), "Success to get invoice detail");
    }

    @Operation(
            summary = "Tạo link thanh toán VNPay",
            description = """
                    Hỗ trợ 2 luồng:
                    - **Invoice**: cung cấp `invoiceIds` (một hoặc nhiều hóa đơn tiền thuê/cọc).
                    - **Quote**: cung cấp `quoteId` (báo giá sửa chữa đã được duyệt, ticket ở trạng thái WAITING_PAYMENT).
                    
                    Chỉ được cung cấp một trong hai, không cả hai.
                    """
    )
    @PostMapping("/vnpay")
    public ApiResponse<String> createPaymentLink(
            @AuthenticationPrincipal Jwt jwt,
            @RequestBody @Valid CreatePaymentRequest request,
            HttpServletRequest httpRequest) {

        String keycloakId = jwt.getSubject();
        return ApiResponses.ok(paymentService.createPaymentVNPayLink(request, httpRequest, keycloakId), "Link thanh toán VNPay");
    }

    @Operation(
            summary = "[VNPay] IPN callback",
            description = "Endpoint VNPay gọi sau khi thanh toán. Không cần JWT."
    )
    @GetMapping("/vnpay/ipn")
    public VNPayIpnResponse handleIpn(VNPayIpnRequest ipn) {
        return paymentService.handleIpn(ipn);
    }

    @Operation(
            summary = "[OUTSYSTEM] Xem chi tiết hóa đơn",
            description = "Tenant dùng link từ email để xem hóa đơn mà không cần đăng nhập."
    )
    @GetMapping("/outsystem/invoices/{invoiceId}")
    public ApiResponse<PublicInvoiceDto> getPublicInvoice(
            @PathVariable UUID invoiceId,
            @RequestParam String token) {

        paymentTokenService.validateToken(token, invoiceId);
        return ApiResponses.ok(paymentService.getPublicInvoice(invoiceId), "Success");
    }

    @Operation(
            summary = "[OUTSYSTEM] Tạo link thanh toán từ email",
            description = "Tạo link VNPay từ email notification (không cần đăng nhập)."
    )
    @PostMapping("/outsystem/vnpay")
    public ApiResponse<String> createPaymentLinkOutsystem(
            @RequestParam UUID invoiceId,
            @RequestParam(required = false) String bankCode,
            @RequestParam(required = false) String locale,
            HttpServletRequest httpRequest) {

        return ApiResponses.ok(
                paymentService.createPaymentVNPayLinkOutsystem(invoiceId, bankCode, locale, httpRequest),
                "Link thanh toán VNPay");
    }

    @Operation(summary = "Gửi lại email thông báo thanh toán")
    @PostMapping("/invoices/{invoiceId}/resend")
    public ApiResponse<Void> resendNotification(@PathVariable UUID invoiceId) {
        paymentService.resendPaymentNotification(invoiceId);
        return ApiResponses.ok(null, "Đã gửi lại thông báo thanh toán");
    }
}