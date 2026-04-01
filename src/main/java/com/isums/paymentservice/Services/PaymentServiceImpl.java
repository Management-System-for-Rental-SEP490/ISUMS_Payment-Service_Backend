package com.isums.paymentservice.Services;

import com.isums.paymentservice.domains.dtos.*;
import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.*;
import com.isums.paymentservice.domains.events.DepositPaidEvent;
import com.isums.paymentservice.domains.events.SendEmailEvent;
import com.isums.paymentservice.domains.factories.VNPayIpnResponseFactory;
import com.isums.paymentservice.infrastructures.Abtracts.PaymentService;
import com.isums.paymentservice.infrastructures.grpcs.UserGrpcService;
import com.isums.paymentservice.infrastructures.repositories.PaymentRepository;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import jakarta.persistence.EntityNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentServiceImpl implements PaymentService {

    private final PaymentRepository paymentRepository;
    private final RentalInvoiceRepository invoiceRepository;
    private final VNPayProperties vnPayProperties;
    private final KafkaTemplate<String, Object> kafka;

    private static final DateTimeFormatter VNPAY_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    private static final DateTimeFormatter DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(ZoneId.of("Asia/Ho_Chi_Minh"));
    private final com.isums.paymentservice.services.PaymentTokenService paymentTokenService;

    @Value("${app.payment.outsystem-url:https://outsystem.isums.pro/payments}")
    private String outsystemPaymentUrl;

    private final UserGrpcService userGrpcService;


    @Override
    @Transactional
    public String createPaymentVNPayLink(CreatePaymentRequest request, HttpServletRequest httpRequest) {

        // ── 1. Load & validate tất cả invoices ──────────────────────────────
        List<UUID> invoiceUuids = request.invoiceIds().stream()
                .map(UUID::fromString)
                .toList();

        List<RentalInvoice> invoices = invoiceUuids.stream()
                .map(id -> invoiceRepository.findById(id)
                        .orElseThrow(() -> new EntityNotFoundException("Invoice not found: " + id)))
                .toList();

        for (RentalInvoice inv : invoices) {
            if (inv.getStatus() != InvoiceStatus.UNPAID) {
                throw new IllegalStateException(
                        "Invoice " + inv.getId() + " không ở trạng thái UNPAID. Hiện tại: " + inv.getStatus());
            }
        }

        long totalAmount = invoices.stream()
                .mapToLong(RentalInvoice::getTotalAmount)
                .sum();

        UUID tenantId = invoices.getFirst().getTenantId();
        String invoiceIdsJson = invoiceUuids.stream()
                .map(UUID::toString)
                .collect(Collectors.joining(",", "[\"", "\"]"))
                .replace(",", "\",\"");

        Payment payment = Payment.builder()
                .referenceId(invoiceUuids.getFirst())
                .referenceType(invoiceUuids.size() > 1
                        ? ReferenceType.MULTI_INVOICE
                        : ReferenceType.INVOICE)
                .invoiceIds(invoiceIdsJson)
                .tenantId(tenantId)
                .payerUserId(tenantId)
                .amount(totalAmount)
                .method(resolveMethod(request.bankCode()))
                .status(PaymentStatus.PENDING)
                .note(invoices.size() == 1
                        ? buildOrderInfo(invoices.getFirst())
                        : "Thanh toan " + invoices.size() + " hoa don")
                .build();
        paymentRepository.save(payment);

        log.info("[VNPay] Created PENDING payment={} invoices={} totalAmount={}",
                payment.getId(), invoiceIdsJson, totalAmount);

        return buildVnpayUrl(payment, totalAmount, request, extractIp(httpRequest));
    }

    @Override
    @Transactional
    public VNPayIpnResponse handleIpn(VNPayIpnRequest ipn) {
        try {
            if (!isValidSignature(ipn)) {
                return VNPayIpnResponseFactory.from(VNPayIpnCode.INVALID_SIGNATURE);
            }

            UUID paymentId = UUID.fromString(ipn.getVnp_TxnRef());
            Payment payment = paymentRepository.findById(paymentId).orElse(null);

            if (payment == null) return VNPayIpnResponseFactory.from(VNPayIpnCode.ORDER_NOT_FOUND);
            if (payment.getStatus() != PaymentStatus.PENDING)
                return VNPayIpnResponseFactory.from(VNPayIpnCode.ALREADY_PROCESSED);

            long expectedAmount = payment.getAmount() * 100L;
            if (expectedAmount != ipn.getVnp_Amount())
                return VNPayIpnResponseFactory.from(VNPayIpnCode.INVALID_AMOUNT);

            boolean isSuccess = "00".equals(ipn.getVnp_ResponseCode())
                    && "00".equals(ipn.getVnp_TransactionStatus());

            Instant now = Instant.now();
            payment.setStatus(isSuccess ? PaymentStatus.SUCCESS : PaymentStatus.FAILED);
            payment.setGatewayTxnId(ipn.getVnp_TransactionNo());
            payment.setGatewayResponse(buildGatewayResponse(ipn));
            if (isSuccess) payment.setPaidAt(now);
            paymentRepository.save(payment);

            List<UUID> invoiceIds = parseInvoiceIds(payment.getInvoiceIds());
            for (UUID invoiceId : invoiceIds) {
                invoiceRepository.findById(invoiceId).ifPresent(invoice -> {
                    invoice.setStatus(isSuccess ? InvoiceStatus.PAID : InvoiceStatus.OVERDUE);
                    if (isSuccess) {
                        invoice.setPaidAt(now);
                        invoiceRepository.save(invoice);
                        handlePostPayment(invoice, ipn.getVnp_TransactionNo());
                    } else {
                        invoiceRepository.save(invoice);
                    }
                });
            }

            return VNPayIpnResponseFactory.from(VNPayIpnCode.SUCCESS);

        } catch (Exception e) {
            log.error("[VNPay IPN] Unexpected error: {}", e.getMessage(), e);
            return VNPayIpnResponseFactory.from(VNPayIpnCode.UNKNOWN_ERROR);
        }
    }

    @Override
    public void resendPaymentNotification(UUID invoiceId) {
        RentalInvoice invoice = invoiceRepository.findById(invoiceId)
                .orElseThrow(() -> new EntityNotFoundException("Invoice not found"));

        if (invoice.getStatus() == InvoiceStatus.PAID) {
            throw new IllegalStateException("Invoice đã thanh toán rồi.");
        }

        String token = paymentTokenService.generateToken(invoice.getId(), invoice.getTenantId());
        String paymentUrl = outsystemPaymentUrl + "?invoiceId=" + invoice.getId() + "&token=" + token;

        String getTenantEmail = userGrpcService.getTenantEmail(invoice.getTenantId());
        kafka.send("notification-email", SendEmailEvent.builder()
                .to(getTenantEmail)
                .templateCode("PAYMENT_INVOICE")
                .params(Map.of(
                        "invoiceType", translateType(invoice.getType().name()),
                        "amount", formatVnd(invoice.getTotalAmount()),
                        "dueDate", DMY.format(invoice.getDueDate()),
                        "paymentUrl", paymentUrl,
                        "expiresIn", "7 ngày"
                ))
                .build());

        log.info("[Payment] Resent notification invoiceId={}", invoiceId);
    }

    @Override
    @Transactional(readOnly = true)
    public PublicInvoiceDto getPublicInvoice(UUID invoiceId) {
        RentalInvoice invoice = invoiceRepository.findById(invoiceId)
                .orElseThrow(() -> new EntityNotFoundException("Invoice not found: " + invoiceId));

        return new PublicInvoiceDto(
                invoice.getId(),
                translateType(invoice.getType().name()),
                invoice.getTotalAmount(),
                DMY.format(invoice.getDueDate()),
                invoice.getStatus()
        );
    }

    @Override
    @Transactional
    public String createPaymentVNPayLinkOutsystem(UUID invoiceId, String bankCode, String locale, HttpServletRequest httpRequest) {
        CreatePaymentRequest request = new CreatePaymentRequest(
                List.of(invoiceId.toString()),
                bankCode,
                locale
        );
        return createPaymentVNPayLink(request, httpRequest);
    }

    private List<UUID> parseInvoiceIds(String json) {
        if (json == null || json.isBlank()) return List.of();
        return Arrays.stream(json.replaceAll("[\\[\\]\"]", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(UUID::fromString)
                .toList();
    }


    private void handlePostPayment(RentalInvoice invoice, String txnNo) {
        try {
            kafka.send("payment-paid-topic", DepositPaidEvent.builder()
                    .invoiceId(invoice.getId())
                    .contractId(invoice.getContractId())
                    .tenantId(invoice.getTenantId())
                    .houseId(invoice.getHouseId())
                    .amount(invoice.getTotalAmount())
                    .invoiceType(invoice.getType())
                    .txnNo(txnNo)
                    .paidAt(invoice.getPaidAt())
                    .build());

            log.info("[Payment] payment-paid-topic sent invoiceId={} type={}",
                    invoice.getId(), invoice.getType());

            if (invoice.getType() == InvoiceType.DEPOSIT) {
                kafka.send("deposit-paid-topic", DepositPaidEvent.builder()
                        .invoiceId(invoice.getId())
                        .contractId(invoice.getContractId())
                        .tenantId(invoice.getTenantId())
                        .houseId(invoice.getHouseId())
                        .amount(invoice.getTotalAmount())
                        .invoiceType(invoice.getType())
                        .txnNo(txnNo)
                        .paidAt(invoice.getPaidAt())
                        .build());

                log.info("[Payment] deposit-paid-topic sent invoiceId={}", invoice.getId());
            }

        } catch (Exception e) {
            log.error("[Payment] handlePostPayment failed invoiceId={}: {}",
                    invoice.getId(), e.getMessage(), e);
        }
    }

    private String translateType(String type) {
        return switch (type) {
            case "DEPOSIT" -> "Tiền cọc";
            case "MONTHLY_RENT" -> "Tiền thuê tháng";
            default -> "Hóa đơn";
        };
    }

    private String formatVnd(Long amount) {
        if (amount == null) return "0 ₫";
        return NumberFormat.getNumberInstance(Locale.of("vi", "VN")).format(amount) + " ₫";
    }

    private String buildVnpayUrl(Payment payment, long totalAmount,
                                 CreatePaymentRequest request, String ipAddr) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime expire = now.plusMinutes(vnPayProperties.getExpireMinutes());

        Map<String, String> params = new TreeMap<>();
        params.put("vnp_Version", vnPayProperties.getVersion());
        params.put("vnp_Command", vnPayProperties.getCommand());
        params.put("vnp_TmnCode", vnPayProperties.getTmnCode());
        params.put("vnp_Amount", String.valueOf(totalAmount * 100L));
        params.put("vnp_CreateDate", now.format(VNPAY_DATE_FORMAT));
        params.put("vnp_CurrCode", vnPayProperties.getCurrCode());
        params.put("vnp_IpAddr", ipAddr != null ? ipAddr : "127.0.0.1");
        params.put("vnp_Locale", request.localeOrDefault());
        params.put("vnp_OrderInfo", payment.getNote());
        params.put("vnp_OrderType", vnPayProperties.getOrderType());
        params.put("vnp_ReturnUrl", vnPayProperties.getReturnUrl());
        params.put("vnp_ExpireDate", expire.format(VNPAY_DATE_FORMAT));
        params.put("vnp_TxnRef", payment.getId().toString()); // ← dùng paymentId

        if (request.bankCode() != null && !request.bankCode().isBlank())
            params.put("vnp_BankCode", request.bankCode());

        String queryString = buildQueryString(params);
        String secureHash = hmacSHA512(vnPayProperties.getHashSecret(), queryString);
        return vnPayProperties.getPayUrl() + "?" + queryString + "&vnp_SecureHash=" + secureHash;
    }

    private String buildOrderInfo(RentalInvoice invoice) {
        return switch (invoice.getType()) {
            case DEPOSIT -> "Thanh toan tien coc hop dong";
            case MONTHLY_RENT -> "Thanh toan tien thue thang";
            case MAINTENANCE -> "Thanh toan phi sua chua";
            case UTILITY -> "Thanh toan phi tien ich";
            case PENALTY -> "Thanh toan tien phat";
            default -> "Thanh toan hoa don " + invoice.getId().toString().substring(0, 8);
        };
    }

    private String extractIp(HttpServletRequest req) {
        String xff = req.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) return xff.split(",")[0].trim();
        return req.getRemoteAddr();
    }

    private boolean isValidSignature(VNPayIpnRequest ipn) {
        Map<String, String> params = new TreeMap<>();
        params.put("vnp_Amount", String.valueOf(ipn.getVnp_Amount()));
        params.put("vnp_BankCode", ipn.getVnp_BankCode());
        params.put("vnp_OrderInfo", ipn.getVnp_OrderInfo());
        params.put("vnp_ResponseCode", ipn.getVnp_ResponseCode());
        params.put("vnp_TmnCode", ipn.getVnp_TmnCode());
        params.put("vnp_TransactionNo", ipn.getVnp_TransactionNo());
        params.put("vnp_TransactionStatus", ipn.getVnp_TransactionStatus());
        params.put("vnp_TxnRef", ipn.getVnp_TxnRef());
        if (ipn.getVnp_BankTranNo() != null) params.put("vnp_BankTranNo", ipn.getVnp_BankTranNo());
        if (ipn.getVnp_CardType() != null) params.put("vnp_CardType", ipn.getVnp_CardType());
        if (ipn.getVnp_PayDate() != null) params.put("vnp_PayDate", ipn.getVnp_PayDate());
        return hmacSHA512(vnPayProperties.getHashSecret(), buildQueryString(params))
                .equalsIgnoreCase(ipn.getVnp_SecureHash());
    }

    private String buildQueryString(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : params.entrySet()) {
            if (!sb.isEmpty()) sb.append('&');
            sb.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8));
            sb.append('=');
            sb.append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
        }
        return sb.toString();
    }

    private String hmacSHA512(String key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA512"));
            byte[] raw = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : raw) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (Exception e) {
            throw new RuntimeException("HMACSHA512 failed", e);
        }
    }

    private String buildGatewayResponse(VNPayIpnRequest ipn) {
        return String.format(
                "{\"vnp_TransactionNo\":\"%s\",\"vnp_ResponseCode\":\"%s\"," +
                        "\"vnp_TransactionStatus\":\"%s\",\"vnp_BankCode\":\"%s\"," +
                        "\"vnp_BankTranNo\":\"%s\",\"vnp_CardType\":\"%s\",\"vnp_PayDate\":\"%s\"}",
                ipn.getVnp_TransactionNo(), ipn.getVnp_ResponseCode(),
                ipn.getVnp_TransactionStatus(), ipn.getVnp_BankCode(),
                ipn.getVnp_BankTranNo(), ipn.getVnp_CardType(), ipn.getVnp_PayDate());
    }

    private PaymentMethod resolveMethod(String bankCode) {
        if (bankCode == null) return PaymentMethod.VNPAY;
        return switch (bankCode) {
            case "VNPAYQR" -> PaymentMethod.VNPAY_QR;
            case "VNBANK" -> PaymentMethod.VNPAY_BANK;
            case "INTCARD" -> PaymentMethod.VNPAY_INTCARD;
            default -> PaymentMethod.VNPAY;
        };
    }
}