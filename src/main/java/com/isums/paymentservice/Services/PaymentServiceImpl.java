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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

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

        UUID invoiceId = UUID.fromString(request.invoiceId());

        RentalInvoice invoice = invoiceRepository.findById(invoiceId)
                .orElseThrow(() -> new EntityNotFoundException("Invoice not found: " + invoiceId));

        if (invoice.getStatus() != InvoiceStatus.UNPAID) {
            throw new IllegalStateException("Invoice không ở trạng thái UNPAID. Trạng thái hiện tại: " + invoice.getStatus());
        }

        boolean hasPending = paymentRepository.existsByReferenceIdAndStatus(invoiceId, PaymentStatus.PENDING);
        if (hasPending) {
            Payment existing = paymentRepository
                    .findByReferenceIdAndStatus(invoiceId, PaymentStatus.PENDING).orElseThrow();
            log.info("[VNPay] Reusing PENDING payment={} invoice={}", existing.getId(), invoiceId);
            return buildVnpayUrl(existing, invoice, request, extractIp(httpRequest));
        }

        Payment payment = Payment.builder()
                .referenceId(invoiceId)
                .referenceType(ReferenceType.INVOICE)
                .tenantId(invoice.getTenantId())
                .payerUserId(invoice.getTenantId())
                .amount(invoice.getTotalAmount())
                .method(resolveMethod(request.bankCode()))
                .status(PaymentStatus.PENDING)
                .note(buildOrderInfo(invoice))
                .build();
        paymentRepository.save(payment);

        log.info("[VNPay] Created PENDING payment={} invoice={} type={} amount={}",
                payment.getId(), invoiceId, invoice.getType(), invoice.getTotalAmount());

        return buildVnpayUrl(payment, invoice, request, extractIp(httpRequest));
    }

    @Override
    @Transactional
    public VNPayIpnResponse handleIpn(VNPayIpnRequest ipn) {
        log.info("[VNPay IPN] txnRef={} responseCode={} transactionStatus={}",
                ipn.getVnp_TxnRef(), ipn.getVnp_ResponseCode(), ipn.getVnp_TransactionStatus());
        try {
            if (!isValidSignature(ipn)) {
                log.warn("[VNPay IPN] Invalid signature txnRef={}", ipn.getVnp_TxnRef());
                return VNPayIpnResponseFactory.from(VNPayIpnCode.INVALID_SIGNATURE);
            }

            UUID invoiceId = UUID.fromString(ipn.getVnp_TxnRef());
            Payment payment = paymentRepository.findByReferenceId(invoiceId).orElse(null);

            if (payment == null) {
                log.warn("[VNPay IPN] Payment not found invoiceId={}", invoiceId);
                return VNPayIpnResponseFactory.from(VNPayIpnCode.ORDER_NOT_FOUND);
            }
            if (payment.getStatus() != PaymentStatus.PENDING) {
                log.info("[VNPay IPN] Already processed payment={} status={}",
                        payment.getId(), payment.getStatus());
                return VNPayIpnResponseFactory.from(VNPayIpnCode.ALREADY_PROCESSED);
            }

            long expectedAmount = payment.getAmount() * 100L;
            if (expectedAmount != ipn.getVnp_Amount()) {
                log.warn("[VNPay IPN] Amount mismatch expected={} got={}",
                        expectedAmount, ipn.getVnp_Amount());
                return VNPayIpnResponseFactory.from(VNPayIpnCode.INVALID_AMOUNT);
            }

            boolean isSuccess = "00".equals(ipn.getVnp_ResponseCode())
                    && "00".equals(ipn.getVnp_TransactionStatus());

            Instant now = Instant.now();
            payment.setStatus(isSuccess ? PaymentStatus.SUCCESS : PaymentStatus.FAILED);
            payment.setGatewayTxnId(ipn.getVnp_TransactionNo());
            payment.setGatewayResponse(buildGatewayResponse(ipn));
            if (isSuccess) payment.setPaidAt(now);
            paymentRepository.save(payment);

            invoiceRepository.findById(invoiceId).ifPresent(invoice -> {
                invoice.setStatus(isSuccess ? InvoiceStatus.PAID : InvoiceStatus.OVERDUE);
                if (isSuccess) {
                    invoice.setPaidAt(now);
                    invoiceRepository.save(invoice);

                    // Sau khi PAID → gửi invoice receipt email + xử lý hậu payment
                    handlePostPayment(invoice, ipn.getVnp_TransactionNo());
                } else {
                    invoiceRepository.save(invoice);
                }
            });

            log.info("[VNPay IPN] payment={} updated to status={}", payment.getId(), payment.getStatus());
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
        CreatePaymentRequest request = new CreatePaymentRequest(invoiceId.toString(), bankCode, locale);
        return createPaymentVNPayLink(request, httpRequest);
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

            log.info("[Payment] PostPayment event sent invoiceId={} type={}",
                    invoice.getId(), invoice.getType());
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
        return java.text.NumberFormat.getNumberInstance(new java.util.Locale("vi", "VN"))
                .format(amount) + " ₫";
    }

    private String buildVnpayUrl(Payment payment, RentalInvoice invoice,
                                 CreatePaymentRequest request, String ipAddr) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime expire = now.plusMinutes(vnPayProperties.getExpireMinutes());

        Map<String, String> params = new TreeMap<>();
        params.put("vnp_Version",    vnPayProperties.getVersion());
        params.put("vnp_Command",    vnPayProperties.getCommand());
        params.put("vnp_TmnCode",    vnPayProperties.getTmnCode());
        params.put("vnp_Amount",     String.valueOf(invoice.getTotalAmount() * 100L));
        params.put("vnp_CreateDate", now.format(VNPAY_DATE_FORMAT));
        params.put("vnp_CurrCode",   vnPayProperties.getCurrCode());
        params.put("vnp_IpAddr",     ipAddr != null ? ipAddr : "127.0.0.1");
        params.put("vnp_Locale",     request.localeOrDefault());
        params.put("vnp_OrderInfo",  payment.getNote() != null ? payment.getNote() : buildOrderInfo(invoice));
        params.put("vnp_OrderType",  vnPayProperties.getOrderType());
        params.put("vnp_ReturnUrl",  vnPayProperties.getReturnUrl());
        params.put("vnp_ExpireDate", expire.format(VNPAY_DATE_FORMAT));
        params.put("vnp_TxnRef",     invoice.getId().toString());

        if (request.bankCode() != null && !request.bankCode().isBlank())
            params.put("vnp_BankCode", request.bankCode());

        params.forEach((k, v) -> {
            if (v == null) log.error("[VNPay] NULL param key={}", k);
        });

        String queryString = buildQueryString(params);
        String secureHash  = hmacSHA512(vnPayProperties.getHashSecret(), queryString);
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