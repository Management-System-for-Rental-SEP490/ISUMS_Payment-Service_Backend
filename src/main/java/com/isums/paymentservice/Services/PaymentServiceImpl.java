package com.isums.paymentservice.services;

import com.isums.paymentservice.domains.dtos.*;
import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.*;
import com.isums.paymentservice.domains.events.*;
import com.isums.paymentservice.domains.factories.VNPayIpnResponseFactory;
import com.isums.paymentservice.infrastructures.Abtracts.PaymentService;
import com.isums.paymentservice.infrastructures.grpcs.IssueGrpcClient;
import com.isums.paymentservice.infrastructures.grpcs.HouseGrpcClient;
import com.isums.paymentservice.infrastructures.grpcs.UserGrpcService;
import com.isums.paymentservice.infrastructures.mappers.InvoiceMapper;
import com.isums.paymentservice.infrastructures.repositories.PaymentRepository;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.isums.userservice.grpc.UserResponse;
import jakarta.persistence.EntityNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.time.Instant;
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
    private final com.isums.paymentservice.services.PaymentTokenService paymentTokenService;
    private final UserGrpcService userGrpcService;
    private final InvoiceMapper invoiceMapper;
    private final IssueGrpcClient issueGrpcClient;
    private final HouseGrpcClient houseGrpcClient;
    private final com.isums.paymentservice.infrastructures.client.SubscriptionPlanClient planClient;
    private final com.isums.paymentservice.infrastructures.repositories.LatePaymentActionLogRepository latePaymentLogRepo;

    private static final ZoneId VNPAY_TZ = ZoneId.of("Asia/Ho_Chi_Minh");
    private static final DateTimeFormatter VNPAY_DATE_FORMAT =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(VNPAY_TZ);
    private static final DateTimeFormatter DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy")
            .withZone(VNPAY_TZ);

    @Value("${app.payment.outsystem-url:https://outsystem.isums.pro/payments}")
    private String outsystemPaymentUrl;

    /**
     * PREMIUM subscription price per month in VND. Source of truth lives
     * here in Payment-Service; Notification-Service mirrors the same
     * default for FE display purposes only.
     */
    @Value("${app.payment.subscription.price-vnd-per-month:19000}")
    private long subscriptionPricePerMonth;

    @Override
    @Transactional
    public String createPaymentVNPayLink(CreatePaymentRequest request, HttpServletRequest httpRequest, String keycloakId) {
        UUID callerId = resolveInternalTenantId(keycloakId);
        if (request.isQuotePayment()) {
            return createQuotePaymentLink(request, httpRequest, callerId);
        }
        return createInvoicePaymentLink(request, httpRequest, callerId);
    }

    @Override
    @Transactional
    public String createSubscriptionPaymentLink(CreateSubscriptionPaymentRequest request,
                                                  HttpServletRequest httpRequest,
                                                  String keycloakId) {
        UUID callerId = resolveInternalTenantId(keycloakId);

        // Tapping "Mua gói" again means the user wants a fresh checkout —
        // cancel any open PENDING SUBSCRIPTION rows so we don't accumulate
        // dangling intents and don't block the new request behind a TTL.
        //
        // Safe even if the cancelled row ends up paid concurrently: the IPN
        // handler in {@code handleIpn} reads {@code status == PENDING} before
        // flipping to SUCCESS, so a row marked FAILED here stays FAILED and
        // returns {@code ALREADY_PROCESSED} to VNPay. No double-credit risk.
        var openPending = paymentRepository
                .findByReferenceIdAndReferenceTypeAndStatusOrderByCreatedAtDesc(
                        callerId, ReferenceType.SUBSCRIPTION, PaymentStatus.PENDING);
        for (Payment prev : openPending) {
            prev.setStatus(PaymentStatus.FAILED);
            prev.setGatewayResponse("{\"reason\":\"superseded_by_new_attempt\"}");
            paymentRepository.save(prev);
            log.info("[VNPay] Superseded PENDING subscription={} userId={} (user started new attempt)",
                    prev.getId(), callerId);
        }

        // Resolve price + duration. planId is authoritative (looked up
        // from Notification-Service's curated catalogue); months is the
        // legacy fallback (19k × N) we keep for API back-compat.
        long amount;
        int durationDays;
        String planCode;
        if (request.planId() != null) {
            String bearer = httpRequest.getHeader("Authorization");
            var plan = planClient.fetchPlan(request.planId(), bearer);
            if (!plan.active()) {
                throw new IllegalStateException("Gói đăng ký đã ngừng bán: " + plan.code());
            }
            amount       = plan.priceVnd();
            durationDays = plan.durationDays();
            planCode     = plan.code();
        } else if (request.months() != null) {
            amount       = subscriptionPricePerMonth * (long) request.months();
            durationDays = request.months() * 30;
            planCode     = "LEGACY_" + request.months() + "M";
        } else {
            throw new IllegalArgumentException("Either planId or months is required");
        }

        Payment payment = Payment.builder()
                .referenceId(callerId)
                .referenceType(ReferenceType.SUBSCRIPTION)
                .tenantId(callerId)
                .payerUserId(callerId)
                .amount(amount)
                .method(resolveMethod(request.bankCode()))
                .status(PaymentStatus.PENDING)
                .note("ISUMS PREMIUM " + planCode)
                // `invoice_ids` doubles as the metadata blob for the IPN
                // handler — encoding planCode + durationDays + keycloakId
                // here avoids another DB column for one-off subscription
                // data. keycloakId is needed because the cross-service
                // contract with Notification-Service uses Keycloak `sub`
                // as the user key (its tables are keyed by JWT.sub, not
                // our internal user UUID).
                .invoiceIds(String.format(
                        "{\"planCode\":\"%s\",\"durationDays\":%d,\"planId\":\"%s\",\"keycloakId\":\"%s\",\"houseId\":\"%s\"}",
                        planCode, durationDays,
                        request.planId() != null ? request.planId() : "",
                        keycloakId,
                        request.houseId() != null ? request.houseId() : ""))
                .build();
        paymentRepository.save(payment);

        log.info("[VNPay] Created PENDING subscription payment={} userId={} plan={} duration={}d amount={}",
                payment.getId(), callerId, planCode, durationDays, amount);

        CreatePaymentRequest adapter = new CreatePaymentRequest(
                null, null, request.bankCode(), request.locale());
        return buildVnpayUrl(payment, amount, adapter, extractIp(httpRequest));
    }

    @Override
    @Transactional
    @org.springframework.cache.annotation.CacheEvict(value = "finance-dashboard", allEntries = true)
    public VNPayIpnResponse handleIpn(VNPayIpnRequest ipn) {
        try {
            if (!isValidSignature(ipn)) {
                return VNPayIpnResponseFactory.from(VNPayIpnCode.INVALID_SIGNATURE);
            }

            UUID paymentId = UUID.fromString(ipn.getVnp_TxnRef());
            Payment payment = paymentRepository.findByIdForUpdate(paymentId).orElse(null);

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

            if (isSuccess) {
                switch (payment.getReferenceType()) {
                    case QUOTE        -> handlePostQuotePayment(payment, ipn.getVnp_TransactionNo());
                    case SUBSCRIPTION -> handlePostSubscriptionPayment(payment, ipn.getVnp_TransactionNo());
                    default            -> handlePostInvoicePayments(payment, ipn.getVnp_TransactionNo(), now);
                }
            }

            return VNPayIpnResponseFactory.from(VNPayIpnCode.SUCCESS);

        } catch (Exception e) {
            log.error("[VNPay IPN] Unexpected error: {}", e.getMessage(), e);
            return VNPayIpnResponseFactory.from(VNPayIpnCode.UNKNOWN_ERROR);
        }
    }

    @Override
    public String handleReturn(VNPayIpnRequest request) {
        boolean signatureValid = isValidSignature(request);
        boolean paymentSuccess = signatureValid
                && "00".equals(request.getVnp_ResponseCode())
                && "00".equals(request.getVnp_TransactionStatus());

        String txnRef = request.getVnp_TxnRef() != null ? request.getVnp_TxnRef() : "";
        if (paymentSuccess) {
            return outsystemPaymentUrl + "/result?status=success&txnRef=" + txnRef;
        } else {
            String code = request.getVnp_ResponseCode() != null ? request.getVnp_ResponseCode() : "99";
            return outsystemPaymentUrl + "/result?status=failed&txnRef=" + txnRef + "&code=" + code;
        }
    }

    @Override
    public void resendPaymentNotification(UUID invoiceId) {
        RentalInvoice invoice = invoiceRepository.findById(invoiceId)
                .orElseThrow(() -> new EntityNotFoundException("Invoice not found"));

        if (invoice.getStatus() == InvoiceStatus.PAID) {
            throw new IllegalStateException("The invoice has already been paid.");
        }

        String token = paymentTokenService.generateToken(invoice.getId(), invoice.getTenantId());
        String paymentUrl = outsystemPaymentUrl + "?invoiceId=" + invoice.getId() + "&token=" + token;

        String tenantEmail = userGrpcService.getTenantEmail(invoice.getTenantId());
        kafka.send("notification-email", SendEmailEvent.builder()
                .to(tenantEmail)
                .templateCode("PAYMENT_INVOICE")
                .params(Map.of(
                        "invoiceType", translateType(invoice.getType().name()),
                        "amount", formatVnd(invoice.getTotalAmount()),
                        "dueDate", DMY.format(invoice.getDueDate()),
                        "paymentUrl", paymentUrl,
                        "expiresIn", "7 days"
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
    public String createPaymentVNPayLinkOutsystem(UUID invoiceId, String bankCode,
                                                  String locale, HttpServletRequest httpRequest) {
        RentalInvoice inv = invoiceRepository.findById(invoiceId)
                .orElseThrow(() -> new EntityNotFoundException("Invoice not found: " + invoiceId));
        CreatePaymentRequest request = new CreatePaymentRequest(
                List.of(invoiceId.toString()), null, bankCode, locale);
        return createInvoicePaymentLink(request, httpRequest, inv.getTenantId());
    }

    @Override
    @Transactional(readOnly = true)
    public List<PaymentMethodOptionDto> getAvailablePaymentMethods() {
        return List.of(
                new PaymentMethodOptionDto(PaymentMethod.VNPAY.name(),         "VNPay"),
                new PaymentMethodOptionDto(PaymentMethod.VNPAY_QR.name(),      "VNPay QR"),
                new PaymentMethodOptionDto(PaymentMethod.VNPAY_BANK.name(),    "VNPay Bank"),
                new PaymentMethodOptionDto(PaymentMethod.VNPAY_INTCARD.name(), "VNPay International Card"),
                new PaymentMethodOptionDto(PaymentMethod.BANK_TRANSFER.name(), "Bank transfer")
        );
    }

    @Override
    @Transactional(readOnly = true)
    public List<InvoiceDto> getMyInvoices(String keycloakId, @Nullable UUID houseId) {
        UUID tenantId = resolveInternalTenantId(keycloakId);
        List<RentalInvoice> invoices = houseId != null
                ? invoiceRepository.findByTenantIdAndHouseIdOrderByDueDateAsc(tenantId, houseId)
                : invoiceRepository.findByTenantIdOrderByDueDateAsc(tenantId);

        java.util.Set<UUID> closedContractIds = new java.util.HashSet<>();
        java.util.Set<UUID> activeContractIds = new java.util.HashSet<>();
        List<RentalInvoice> filtered = new java.util.ArrayList<>(invoices.size());
        for (RentalInvoice inv : invoices) {
            UUID cid = inv.getContractId();
            if (cid == null) {
                filtered.add(inv);
                continue;
            }
            if (closedContractIds.contains(cid)) {
                if (inv.getStatus() == InvoiceStatus.UNPAID || inv.getStatus() == InvoiceStatus.OVERDUE) {
                    continue;
                }
                filtered.add(inv);
                continue;
            }
            if (activeContractIds.contains(cid)) {
                filtered.add(inv);
                continue;
            }
            if (isContractClosedByRelocation(cid)) {
                closedContractIds.add(cid);
                if (inv.getStatus() == InvoiceStatus.UNPAID || inv.getStatus() == InvoiceStatus.OVERDUE) {
                    continue;
                }
            } else {
                activeContractIds.add(cid);
            }
            filtered.add(inv);
        }
        return invoiceMapper.toDtos(filtered);
    }

    @Override
    @Transactional(readOnly = true)
    public InvoiceDetailDto getInvoiceById(UUID invoiceId, String keycloakId) {

        RentalInvoice invoice = invoiceRepository.findById(invoiceId)
                .orElseThrow(() -> new EntityNotFoundException("Invoice not found: " + invoiceId));

        UserResponse user = userGrpcService.getUserIdAndRoleByKeyCloakId(keycloakId);
        UUID tenantId = UUID.fromString(user.getId());
        if (!invoice.getTenantId().equals(tenantId)) {
            throw new AccessDeniedException("You do not have permission to view this invoice.");
        }
        String tenantName = user.getName();
        String tenantPhone = user.getPhoneNumber();

        String houseName = null, houseAddress = null;
        try {
            var house = houseGrpcClient.getHouse(invoice.getHouseId());
            if (house != null) {
                houseName = house.getName();
                houseAddress = house.getAddress();
            }
        } catch (Exception e) {
            log.warn("[Invoice] Cannot fetch house info houseId={}: {}", invoice.getHouseId(), e.getMessage());
        }

        UUID issueId = null;
        List<IssueItemDto> issueItems = null;

        if (invoice.getType() == InvoiceType.ISSUE) {

            try {
                var quote = issueGrpcClient.getQuoteDetail(invoice.getQuoteId());

                    issueId = quote.issueId();

                    issueItems = quote.items().stream()
                            .map(i -> new IssueItemDto(
                                    i.id(),
                                    i.itemName(),
                                    i.price()
                            ))
                            .toList();
            } catch (Exception e) {
                log.warn("[Invoice] Cannot fetch issue items: {}", e.getMessage());
            }
        }

        List<InvoiceDetailDto.PaymentRecord> payments = paymentRepository.findByReferenceIdOrderByCreatedAtDesc(invoiceId)
                .stream()
                .map(p -> new InvoiceDetailDto.PaymentRecord(
                        p.getId(), p.getAmount(), p.getMethod(),
                        p.getStatus(), p.getGatewayTxnId(),
                        p.getPaidAt(), p.getCreatedAt()))
                .toList();

        return new InvoiceDetailDto(
                invoice.getId(), invoice.getContractId(),
                invoice.getQuoteId(),
                invoice.getType(), invoice.getPeriodKey(),
                invoice.getBaseAmount(), invoice.getServiceAmount(),
                invoice.getPenaltyAmount(), invoice.getTotalAmount(),
                invoice.getStatus(), invoice.getDueDate(),
                invoice.getPaidAt(), invoice.getCreatedAt(),
                tenantId, tenantName, invoice.getTenantEmail(), tenantPhone,
                invoice.getHouseId(), houseName, houseAddress,
                payments,issueId,issueItems
        );
    }

    @Transactional
    @org.springframework.cache.annotation.CacheEvict(value = "finance-dashboard", allEntries = true)
    public void markDepositRefundPaid(UUID invoiceId, MarkRefundPaidRequest req, String actorId) {
        RentalInvoice invoice = invoiceRepository.findById(invoiceId)
                .orElseThrow(() -> new EntityNotFoundException("Invoice not found"));

        if (invoice.getType() != InvoiceType.DEPOSIT_REFUND) {
            throw new IllegalStateException("Only applicable to deposit-refund invoices");
        }
        if (invoice.getStatus() == InvoiceStatus.PAID) {
            throw new IllegalStateException("Invoice has already been confirmed");
        }

        invoice.setStatus(InvoiceStatus.PAID);
        invoice.setPaidAt(Instant.now());
        invoice.setRefundPaymentMethod(req.paymentMethod().name());
        invoice.setRefundNote(req.note());
        invoiceRepository.save(invoice);

        kafka.send("deposit-refund-paid-topic",
                invoice.getContractId().toString(),
                DepositRefundPaidEvent.builder()
                        .contractId(invoice.getContractId())
                        .houseId(invoice.getHouseId())
                        .tenantId(invoice.getTenantId())
                        .tenantEmail(invoice.getTenantEmail())
                        .refundAmount(invoice.getTotalAmount())
                        .paymentMethod(invoice.getRefundPaymentMethod())
                        .note(invoice.getRefundNote())
                        .paidAt(invoice.getPaidAt())
                        .messageId(UUID.randomUUID().toString())
                        .build());

        log.info("[Payment] DEPOSIT_REFUND marked PAID invoiceId={} contractId={} by actor={}",
                invoiceId, invoice.getContractId(), actorId);
    }

    @Override
    @Transactional(readOnly = true)
    public DepositRefundInvoiceDto getDepositRefundInvoice(UUID contractId) {
        return invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT_REFUND)
                .map(inv -> new DepositRefundInvoiceDto(
                        inv.getId(),
                        inv.getContractId(),
                        inv.getTotalAmount(),
                        inv.getStatus() != null ? inv.getStatus().name() : null,
                        inv.getRefundPaymentMethod(),
                        inv.getPaidAt(),
                        inv.getRefundNote()))
                .orElse(null);
    }

    private String createInvoicePaymentLink(CreatePaymentRequest request,
                                            HttpServletRequest httpRequest,
                                            UUID callerId) {
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
                        "Invoice " + inv.getId() + " Not in UNPAID status. Currently: " + inv.getStatus());
            }
            if (isContractClosedByRelocation(inv.getContractId())) {
                throw new IllegalStateException(
                        "Invoice " + inv.getId() + " thuộc hợp đồng đã được thay thế qua đổi nhà — không thể thanh toán. Vui lòng dùng hợp đồng mới.");
            }
        }

        long totalAmount = invoices.stream().mapToLong(RentalInvoice::getTotalAmount).sum();
        UUID tenantId = invoices.getFirst().getTenantId();

        String invoiceIdsJson = invoiceUuids.stream()
                .map(UUID::toString)
                .collect(Collectors.joining(",", "[\"", "\"]"))
                .replace(",", "\",\"");

        Payment payment = Payment.builder()
                .referenceId(invoiceUuids.getFirst())
                .referenceType(invoiceUuids.size() > 1 ? ReferenceType.MULTI_INVOICE : ReferenceType.INVOICE)
                .invoiceIds(invoiceIdsJson)
                .tenantId(tenantId)
                .payerUserId(callerId)
                .amount(totalAmount)
                .method(resolveMethod(request.bankCode()))
                .status(PaymentStatus.PENDING)
                .note(invoices.size() == 1
                        ? buildOrderInfo(invoices.getFirst())
                        : "Pay " + invoices.size() + " invoice")
                .build();
        paymentRepository.save(payment);

        log.info("[VNPay] Created PENDING invoice payment={} invoices={} amount={}",
                payment.getId(), invoiceIdsJson, totalAmount);

        return buildVnpayUrl(payment, totalAmount, request, extractIp(httpRequest));
    }

    private boolean isContractClosedByRelocation(UUID contractId) {
        if (contractId == null) return false;
        return invoiceRepository
                .findByContractIdAndType(contractId, InvoiceType.DEPOSIT)
                .map(deposit -> {
                    InvoiceStatus s = deposit.getStatus();
                    return s == InvoiceStatus.TRANSFERRED
                            || s == InvoiceStatus.FORFEITED
                            || s == InvoiceStatus.REFUNDED;
                })
                .orElse(false);
    }

    private String createQuotePaymentLink(CreatePaymentRequest request,
                                          HttpServletRequest httpRequest,
                                          UUID callerId) {
        UUID quoteId = UUID.fromString(request.quoteId());

        if (paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.PENDING)) {
            throw new IllegalStateException("This quote has pending payment. Please complete the transaction or wait for a timeout.");
        }
        if (paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.SUCCESS)) {
            throw new IllegalStateException("This quote has been paid for.");
        }

        IssueQuoteResponse quote = issueGrpcClient.getQuote(quoteId);

        if (!"APPROVED".equals(quote.status())) {
            throw new IllegalStateException("The quotation has not been approved. Current status: " + quote.status());
        }

        if (quote.tenantId() != null && !quote.tenantId().equals(callerId)) {
            throw new AccessDeniedException("You are not eligible to pay this quote.");
        }

        long amount = quote.totalPrice().longValue();

        Payment payment = Payment.builder()
                .referenceId(quoteId)
                .referenceType(ReferenceType.QUOTE)
                .tenantId(callerId)
                .payerUserId(callerId)
                .amount(amount)
                .method(resolveMethod(request.bankCode()))
                .status(PaymentStatus.PENDING)
                .note("Repair-quote payment")
                .build();
        paymentRepository.save(payment);

        log.info("[VNPay] Created PENDING quote payment={} quoteId={} amount={}",
                payment.getId(), quoteId, amount);

        return buildVnpayUrl(payment, amount, request, extractIp(httpRequest));
    }

    private void handlePostInvoicePayments(Payment payment, String txnNo, Instant now) {
        List<UUID> invoiceIds = parseInvoiceIds(payment.getInvoiceIds());
        for (UUID invoiceId : invoiceIds) {
            invoiceRepository.findById(invoiceId).ifPresent(invoice -> {
                invoice.setStatus(InvoiceStatus.PAID);

                kafka.send("payment.app-access-changed",
                        invoice.getHouseId().toString(),
                        AppAccessChangedEvent.builder()
                                .tenantId(invoice.getTenantId())
                                .houseId(invoice.getHouseId())
                                .contractId(invoice.getContractId())
                                .restricted(false)
                                .reason("PAYMENT_RECEIVED")
                                .messageId(UUID.randomUUID().toString())
                                .build());

                boolean hadPowerCut = latePaymentLogRepo.existsByInvoiceIdAndActionType(
                        invoice.getId(), com.isums.paymentservice.domains.enums.LatePaymentAction.POWER_CUT_REQUEST);
                if (hadPowerCut) {
                    kafka.send("payment.power-restore-requested",
                            invoice.getContractId().toString(),
                            PowerRestoreRequestEvent.builder()
                                    .invoiceId(invoice.getId())
                                    .contractId(invoice.getContractId())
                                    .houseId(invoice.getHouseId())
                                    .tenantId(invoice.getTenantId())
                                    .reason("PAYMENT_RECEIVED")
                                    .messageId(UUID.randomUUID().toString())
                                    .build());
                    log.info("[Payment] Power restore requested invoiceId={} houseId={}",
                            invoice.getId(), invoice.getHouseId());
                }

                invoice.setPaidAt(now);
                invoiceRepository.save(invoice);
                handlePostPayment(invoice, txnNo);
            });
        }
    }

    /**
     * Notification-Service PREMIUM activation. Emits Kafka topic
     * {@code payment.subscription-activated} which the notification
     * service consumes to flip the user's tier. Idempotent on the
     * consumer side, so a Kafka redelivery is safe.
     */
    private void handlePostSubscriptionPayment(Payment payment, String txnNo) {
        try {
            int durationDays = parseMetadataInt(payment.getInvoiceIds(), "durationDays", 30);
            String planCode  = parseMetadataString(payment.getInvoiceIds(), "planCode", "");
            String planId    = parseMetadataString(payment.getInvoiceIds(), "planId",   "");
            String keycloakId = parseMetadataString(payment.getInvoiceIds(), "keycloakId", "");
            String houseId    = parseMetadataString(payment.getInvoiceIds(), "houseId",   "");

            String eventUserId = !keycloakId.isBlank()
                    ? keycloakId
                    : payment.getPayerUserId().toString();

            if (houseId.isBlank()) {
                log.error("[Payment] Subscription payment {} has no houseId metadata — cannot activate per-house PREMIUM. Skipping event emission.",
                        payment.getId());
                return;
            }

            Map<String, Object> event = new HashMap<>();
            event.put("intentId",     payment.getId().toString());
            event.put("userId",       eventUserId);
            event.put("houseId",      houseId);
            event.put("purpose",      "NOTIFICATION_PREMIUM");
            event.put("durationDays", durationDays);
            event.put("planCode",     planCode);
            event.put("planId",       planId);
            event.put("amountVnd",    payment.getAmount());
            event.put("provider",     "VNPAY");
            event.put("txnRef",       payment.getId().toString());
            event.put("txnNo",        txnNo);
            event.put("paidAt",
                    payment.getPaidAt() != null ? payment.getPaidAt().toString()
                                                 : Instant.now().toString());
            kafka.send("payment.subscription-activated", eventUserId, event);
            log.info("[Payment] subscription-activated emitted keycloakUserId={} houseId={} internalPayerId={} plan={} duration={}d amount={}",
                    eventUserId, houseId, payment.getPayerUserId(), planCode, durationDays, payment.getAmount());
        } catch (Exception e) {
            log.error("[Payment] handlePostSubscriptionPayment failed paymentId={}: {}",
                    payment.getId(), e.getMessage(), e);
        }
    }

    private int parseMetadataInt(String json, String key, int fallback) {
        if (json == null || json.isBlank()) return fallback;
        try {
            var m = java.util.regex.Pattern.compile("\"" + key + "\"\\s*:\\s*(\\d+)").matcher(json);
            if (m.find()) return Integer.parseInt(m.group(1));
        } catch (Exception ignored) {}
        return fallback;
    }

    private String parseMetadataString(String json, String key, String fallback) {
        if (json == null || json.isBlank()) return fallback;
        try {
            var m = java.util.regex.Pattern.compile("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"").matcher(json);
            if (m.find()) return m.group(1);
        } catch (Exception ignored) {}
        return fallback;
    }

    private void handlePostQuotePayment(Payment payment, String txnNo) {
        try {
            String periodKey = "QUOTE-" + payment.getReferenceId();

            invoiceRepository.findByContractIdAndPeriodKey(payment.getReferenceId(), periodKey)
                    .ifPresent(invoice -> {
                        invoice.setStatus(InvoiceStatus.PAID);
                        invoice.setPaidAt(payment.getPaidAt());
                        invoiceRepository.save(invoice);
                        log.info("[Payment] Invoice PAID invoiceId={} quoteId={}",
                                invoice.getId(), payment.getReferenceId());
                    });

            kafka.send("quote-payment-completed", QuotePaymentCompletedEvent.builder()
                    .quoteId(payment.getReferenceId())
                    .issueId(null)
                    .tenantId(payment.getTenantId())
                    .amount(BigDecimal.valueOf(payment.getAmount()))
                    .txnNo(txnNo)
                    .paidAt(payment.getPaidAt())
                    .build());

            log.info("[Payment] quote-payment-completed sent quoteId={}", payment.getReferenceId());
        } catch (Exception e) {
            log.error("[Payment] handlePostQuotePayment failed quoteId={}: {}",
                    payment.getReferenceId(), e.getMessage(), e);
        }
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
                    .tenantEmail(invoice.getTenantEmail())
                    .build());

            log.info("[Payment] payment-paid-topic sent invoiceId={} type={} tenantEmail={}",
                    invoice.getId(), invoice.getType(), invoice.getTenantEmail());

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
                        .tenantEmail(invoice.getTenantEmail())
                        .relocationSourceContractId(invoice.getRelocationSourceContractId())
                        .build());
                log.info("[Payment] deposit-paid-topic sent invoiceId={} relocationSource={}",
                        invoice.getId(), invoice.getRelocationSourceContractId());
            }
        } catch (Exception e) {
            log.error("[Payment] handlePostPayment failed invoiceId={}: {}",
                    invoice.getId(), e.getMessage(), e);
        }
    }

    private UUID resolveInternalTenantId(String keycloakId) {
        UserResponse user = userGrpcService.getUserIdAndRoleByKeyCloakId(keycloakId);
        return UUID.fromString(user.getId());
    }

    private String buildVnpayUrl(Payment payment, long totalAmount,
                                 CreatePaymentRequest request, String ipAddr) {
        java.time.ZonedDateTime now = java.time.ZonedDateTime.now(VNPAY_TZ);
        java.time.ZonedDateTime expire = now.plusMinutes(vnPayProperties.getExpireMinutes());

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
        params.put("vnp_TxnRef", payment.getId().toString());

        if (request.bankCode() != null && !request.bankCode().isBlank()) {
            params.put("vnp_BankCode", request.bankCodeOrDefault());
        }

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

    private List<UUID> parseInvoiceIds(String json) {
        if (json == null || json.isBlank()) return List.of();
        return Arrays.stream(json.replaceAll("[\\[\\]\"]", "").split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .map(UUID::fromString)
                .toList();
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

    private String translateType(String type) {
        return switch (type) {
            case "DEPOSIT" -> "Tiền cọc";
            case "MONTHLY_RENT" -> "Tiền thuê tháng";
            case "DEPOSIT_REFUND" -> "Hoàn cọc";
            case "UTILITY" -> "Phí dịch vụ";
            case "MAINTENANCE" -> "Phí bảo trì";
            default -> "Hoá đơn";
        };
    }

    private String formatVnd(Long amount) {
        if (amount == null) return "0 ₫";
        return NumberFormat.getNumberInstance(Locale.of("vi", "VN")).format(amount) + " ₫";
    }
}
