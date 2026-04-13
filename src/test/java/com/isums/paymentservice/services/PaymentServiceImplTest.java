package com.isums.paymentservice.services;

import com.isums.houseservice.grpc.HouseResponse;
import com.isums.paymentservice.services.PaymentServiceImpl;
import com.isums.paymentservice.domains.events.SendEmailEvent;
import com.isums.paymentservice.domains.dtos.*;
import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.*;
import com.isums.paymentservice.infrastructures.grpcs.HouseGrpcClient;
import com.isums.paymentservice.infrastructures.grpcs.IssueGrpcClient;
import com.isums.paymentservice.infrastructures.grpcs.UserGrpcService;
import com.isums.paymentservice.infrastructures.mappers.InvoiceMapper;
import com.isums.paymentservice.infrastructures.repositories.PaymentRepository;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.isums.userservice.grpc.UserResponse;
import jakarta.persistence.EntityNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.test.util.ReflectionTestUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentServiceImpl")
class PaymentServiceImplTest {

    @Mock private PaymentRepository paymentRepository;
    @Mock private RentalInvoiceRepository invoiceRepository;
    @Mock private VNPayProperties vnPayProperties;
    @Mock private KafkaTemplate<String, Object> kafka;
    @Mock private com.isums.paymentservice.services.PaymentTokenService paymentTokenService;
    @Mock private UserGrpcService userGrpcService;
    @Mock private InvoiceMapper invoiceMapper;
    @Mock private IssueGrpcClient issueGrpcClient;
    @Mock private HouseGrpcClient houseGrpcClient;
    @Mock private HttpServletRequest httpRequest;

    @InjectMocks private PaymentServiceImpl service;

    private static final String HASH_SECRET = "test-hash-secret-123";
    private String keycloakId;
    private UUID tenantId;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(service, "outsystemPaymentUrl", "https://out.example/payments");
        keycloakId = UUID.randomUUID().toString();
        tenantId = UUID.randomUUID();
    }

    private void mockResolveTenant() {
        UserResponse resp = UserResponse.newBuilder().setId(tenantId.toString()).build();
        when(userGrpcService.getUserIdAndRoleByKeyCloakId(keycloakId)).thenReturn(resp);
    }

    private RentalInvoice invoice(UUID id, InvoiceStatus status, long total, InvoiceType type) {
        return RentalInvoice.builder()
                .id(id).contractId(UUID.randomUUID())
                .tenantId(tenantId).houseId(UUID.randomUUID())
                .type(type).periodKey("2026-04")
                .baseAmount(total).serviceAmount(0L).penaltyAmount(0L)
                .totalAmount(total).status(status)
                .dueDate(Instant.now().plusSeconds(86400))
                .build();
    }

    private void stubSaveAssignsId() {
        when(paymentRepository.save(any(Payment.class))).thenAnswer(a -> {
            Payment p = a.getArgument(0);
            if (p.getId() == null) p.setId(UUID.randomUUID());
            return p;
        });
    }

    private void stubVnPayProps() {
        when(vnPayProperties.getVersion()).thenReturn("2.1.0");
        when(vnPayProperties.getCommand()).thenReturn("pay");
        when(vnPayProperties.getTmnCode()).thenReturn("TEST");
        when(vnPayProperties.getCurrCode()).thenReturn("VND");
        when(vnPayProperties.getOrderType()).thenReturn("other");
        when(vnPayProperties.getReturnUrl()).thenReturn("https://return.example");
        when(vnPayProperties.getPayUrl()).thenReturn("https://vnpay.example/pay");
        when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
        when(vnPayProperties.getExpireMinutes()).thenReturn(15);
    }

    @Nested
    @DisplayName("createPaymentVNPayLink — invoice path")
    class CreateInvoiceLink {

        @Test
        @DisplayName("builds URL with 100x amount and correct txnRef for single invoice")
        void singleInvoice() {
            UUID invoiceId = UUID.randomUUID();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 500_000L, InvoiceType.MONTHLY_RENT);

            mockResolveTenant();
            stubVnPayProps();
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));
            when(httpRequest.getHeader("X-Forwarded-For")).thenReturn(null);
            when(httpRequest.getRemoteAddr()).thenReturn("10.0.0.1");
            stubSaveAssignsId();

            CreatePaymentRequest req = new CreatePaymentRequest(
                    List.of(invoiceId.toString()), null, "VNPAYQR", "vn");

            String url = service.createPaymentVNPayLink(req, httpRequest, keycloakId);

            assertThat(url).startsWith("https://vnpay.example/pay?");
            assertThat(url).contains("vnp_Amount=50000000");
            assertThat(url).contains("vnp_IpAddr=10.0.0.1");
            assertThat(url).contains("vnp_SecureHash=");
            assertThat(url).contains("vnp_BankCode=VNPAYQR");

            ArgumentCaptor<Payment> cap = ArgumentCaptor.forClass(Payment.class);
            verify(paymentRepository).save(cap.capture());
            Payment saved = cap.getValue();
            assertThat(saved.getAmount()).isEqualTo(500_000L);
            assertThat(saved.getReferenceType()).isEqualTo(ReferenceType.INVOICE);
            assertThat(saved.getMethod()).isEqualTo(PaymentMethod.VNPAY_QR);
            assertThat(saved.getStatus()).isEqualTo(PaymentStatus.PENDING);
        }

        @Test
        @DisplayName("sums amounts and uses MULTI_INVOICE referenceType for multi-invoice payment")
        void multiInvoice() {
            UUID id1 = UUID.randomUUID();
            UUID id2 = UUID.randomUUID();
            RentalInvoice inv1 = invoice(id1, InvoiceStatus.UNPAID, 300_000L, InvoiceType.MONTHLY_RENT);
            RentalInvoice inv2 = invoice(id2, InvoiceStatus.UNPAID, 200_000L, InvoiceType.UTILITY);

            mockResolveTenant();
            stubVnPayProps();
            when(invoiceRepository.findById(id1)).thenReturn(Optional.of(inv1));
            when(invoiceRepository.findById(id2)).thenReturn(Optional.of(inv2));
            when(httpRequest.getHeader("X-Forwarded-For")).thenReturn("1.2.3.4, 5.6.7.8");
            stubSaveAssignsId();

            CreatePaymentRequest req = new CreatePaymentRequest(
                    List.of(id1.toString(), id2.toString()), null, null, null);

            service.createPaymentVNPayLink(req, httpRequest, keycloakId);

            ArgumentCaptor<Payment> cap = ArgumentCaptor.forClass(Payment.class);
            verify(paymentRepository).save(cap.capture());
            Payment saved = cap.getValue();
            assertThat(saved.getAmount()).isEqualTo(500_000L);
            assertThat(saved.getReferenceType()).isEqualTo(ReferenceType.MULTI_INVOICE);
            assertThat(saved.getNote()).isEqualTo("Pay 2 invoice");
            assertThat(saved.getInvoiceIds()).contains(id1.toString()).contains(id2.toString());
            assertThat(saved.getMethod()).isEqualTo(PaymentMethod.VNPAY);
        }

        @Test
        @DisplayName("uses first X-Forwarded-For IP when header present")
        void usesXff() {
            UUID invoiceId = UUID.randomUUID();
            mockResolveTenant();
            stubVnPayProps();
            when(invoiceRepository.findById(invoiceId))
                    .thenReturn(Optional.of(invoice(invoiceId, InvoiceStatus.UNPAID, 100L, InvoiceType.DEPOSIT)));
            when(httpRequest.getHeader("X-Forwarded-For")).thenReturn("2.3.4.5, 6.7.8.9");
            stubSaveAssignsId();

            String url = service.createPaymentVNPayLink(
                    new CreatePaymentRequest(List.of(invoiceId.toString()), null, null, null),
                    httpRequest, keycloakId);

            assertThat(url).contains("vnp_IpAddr=2.3.4.5");
        }

        @Test
        @DisplayName("throws EntityNotFoundException when invoice missing")
        void invoiceNotFound() {
            UUID invoiceId = UUID.randomUUID();
            mockResolveTenant();
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.empty());

            CreatePaymentRequest req = new CreatePaymentRequest(
                    List.of(invoiceId.toString()), null, null, null);

            assertThatThrownBy(() -> service.createPaymentVNPayLink(req, httpRequest, keycloakId))
                    .isInstanceOf(EntityNotFoundException.class);
            verify(paymentRepository, never()).save(any());
        }

        @Test
        @DisplayName("throws IllegalStateException when invoice not UNPAID")
        void invoiceNotUnpaid() {
            UUID invoiceId = UUID.randomUUID();
            mockResolveTenant();
            when(invoiceRepository.findById(invoiceId))
                    .thenReturn(Optional.of(invoice(invoiceId, InvoiceStatus.PAID, 100L, InvoiceType.MONTHLY_RENT)));

            CreatePaymentRequest req = new CreatePaymentRequest(
                    List.of(invoiceId.toString()), null, null, null);

            assertThatThrownBy(() -> service.createPaymentVNPayLink(req, httpRequest, keycloakId))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Not in UNPAID status");
            verify(paymentRepository, never()).save(any());
        }
    }

    @Nested
    @DisplayName("createPaymentVNPayLink — quote path")
    class CreateQuoteLink {

        private IssueQuoteResponse approvedQuote(UUID quoteId, UUID quoteTenantId) {
            return new IssueQuoteResponse(quoteId, UUID.randomUUID(), quoteTenantId,
                    BigDecimal.valueOf(350_000L), "APPROVED");
        }

        @Test
        @DisplayName("creates QUOTE payment and builds URL when quote APPROVED for caller")
        void happy() {
            UUID quoteId = UUID.randomUUID();
            mockResolveTenant();
            stubVnPayProps();
            when(paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.PENDING)).thenReturn(false);
            when(paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.SUCCESS)).thenReturn(false);
            when(issueGrpcClient.getQuote(quoteId)).thenReturn(approvedQuote(quoteId, tenantId));
            when(httpRequest.getRemoteAddr()).thenReturn("9.9.9.9");
            stubSaveAssignsId();

            CreatePaymentRequest req = new CreatePaymentRequest(null, quoteId.toString(), null, null);

            String url = service.createPaymentVNPayLink(req, httpRequest, keycloakId);

            assertThat(url).contains("vnp_Amount=35000000"); // 350_000 * 100
            ArgumentCaptor<Payment> cap = ArgumentCaptor.forClass(Payment.class);
            verify(paymentRepository).save(cap.capture());
            assertThat(cap.getValue().getReferenceType()).isEqualTo(ReferenceType.QUOTE);
            assertThat(cap.getValue().getReferenceId()).isEqualTo(quoteId);
        }

        @Test
        @DisplayName("throws when there is already a PENDING payment for this quote")
        void pendingExists() {
            UUID quoteId = UUID.randomUUID();
            mockResolveTenant();
            when(paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.PENDING)).thenReturn(true);

            CreatePaymentRequest req = new CreatePaymentRequest(null, quoteId.toString(), null, null);

            assertThatThrownBy(() -> service.createPaymentVNPayLink(req, httpRequest, keycloakId))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("pending payment");
        }

        @Test
        @DisplayName("throws when quote already SUCCESS")
        void successExists() {
            UUID quoteId = UUID.randomUUID();
            mockResolveTenant();
            when(paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.PENDING)).thenReturn(false);
            when(paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.SUCCESS)).thenReturn(true);

            CreatePaymentRequest req = new CreatePaymentRequest(null, quoteId.toString(), null, null);

            assertThatThrownBy(() -> service.createPaymentVNPayLink(req, httpRequest, keycloakId))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("paid for");
        }

        @Test
        @DisplayName("throws when quote not APPROVED")
        void notApproved() {
            UUID quoteId = UUID.randomUUID();
            mockResolveTenant();
            when(paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.PENDING)).thenReturn(false);
            when(paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.SUCCESS)).thenReturn(false);
            when(issueGrpcClient.getQuote(quoteId)).thenReturn(new IssueQuoteResponse(
                    quoteId, UUID.randomUUID(), tenantId, BigDecimal.ONE, "REJECTED"));

            CreatePaymentRequest req = new CreatePaymentRequest(null, quoteId.toString(), null, null);

            assertThatThrownBy(() -> service.createPaymentVNPayLink(req, httpRequest, keycloakId))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("REJECTED");
        }

        @Test
        @DisplayName("throws AccessDeniedException when caller is not the quote tenant")
        void notQuoteTenant() {
            UUID quoteId = UUID.randomUUID();
            UUID otherTenant = UUID.randomUUID();
            mockResolveTenant();
            when(paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.PENDING)).thenReturn(false);
            when(paymentRepository.existsByReferenceIdAndStatus(quoteId, PaymentStatus.SUCCESS)).thenReturn(false);
            when(issueGrpcClient.getQuote(quoteId)).thenReturn(approvedQuote(quoteId, otherTenant));

            CreatePaymentRequest req = new CreatePaymentRequest(null, quoteId.toString(), null, null);

            assertThatThrownBy(() -> service.createPaymentVNPayLink(req, httpRequest, keycloakId))
                    .isInstanceOf(AccessDeniedException.class);
        }
    }

    @Nested
    @DisplayName("handleIpn")
    class HandleIpn {

        private VNPayIpnRequest buildIpn(UUID paymentId, long amountCents,
                                         String responseCode, String txnStatus) {
            VNPayIpnRequest ipn = new VNPayIpnRequest();
            ipn.setVnp_TmnCode("TEST");
            ipn.setVnp_Amount(amountCents);
            ipn.setVnp_BankCode("NCB");
            ipn.setVnp_OrderInfo("test");
            ipn.setVnp_ResponseCode(responseCode);
            ipn.setVnp_TransactionStatus(txnStatus);
            ipn.setVnp_TransactionNo("99999");
            ipn.setVnp_TxnRef(paymentId.toString());
            ipn.setVnp_PayDate("20260413120000");
            ipn.setVnp_CardType("ATM");
            ipn.setVnp_BankTranNo("BTN-1");
            ipn.setVnp_SecureHash(signIpn(ipn));
            return ipn;
        }

        private String signIpn(VNPayIpnRequest ipn) {
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

            StringBuilder sb = new StringBuilder();
            for (var e : params.entrySet()) {
                if (!sb.isEmpty()) sb.append('&');
                sb.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8));
                sb.append('=');
                sb.append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
            }
            try {
                Mac mac = Mac.getInstance("HmacSHA512");
                mac.init(new SecretKeySpec(HASH_SECRET.getBytes(StandardCharsets.UTF_8), "HmacSHA512"));
                byte[] raw = mac.doFinal(sb.toString().getBytes(StandardCharsets.UTF_8));
                StringBuilder hex = new StringBuilder();
                for (byte b : raw) hex.append(String.format("%02x", b));
                return hex.toString();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        @Test
        @DisplayName("returns INVALID_SIGNATURE when hash does not match")
        void invalidSignature() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            VNPayIpnRequest ipn = new VNPayIpnRequest();
            ipn.setVnp_TxnRef(UUID.randomUUID().toString());
            ipn.setVnp_Amount(100L);
            ipn.setVnp_BankCode("NCB");
            ipn.setVnp_ResponseCode("00");
            ipn.setVnp_TransactionStatus("00");
            ipn.setVnp_TmnCode("TEST");
            ipn.setVnp_OrderInfo("x");
            ipn.setVnp_TransactionNo("1");
            ipn.setVnp_SecureHash("BADHASH");

            VNPayIpnResponse res = service.handleIpn(ipn);
            assertThat(res.getRspCode()).isEqualTo("97");
        }

        @Test
        @DisplayName("returns ORDER_NOT_FOUND when payment missing")
        void orderNotFound() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            VNPayIpnRequest ipn = buildIpn(paymentId, 100L, "00", "00");
            when(paymentRepository.findByIdForUpdate(paymentId)).thenReturn(Optional.empty());

            assertThat(service.handleIpn(ipn).getRspCode()).isEqualTo("01");
        }

        @Test
        @DisplayName("returns ALREADY_PROCESSED when payment not PENDING")
        void alreadyProcessed() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            VNPayIpnRequest ipn = buildIpn(paymentId, 100L, "00", "00");
            Payment p = Payment.builder().id(paymentId).amount(1L)
                    .status(PaymentStatus.SUCCESS).referenceType(ReferenceType.INVOICE).build();
            when(paymentRepository.findByIdForUpdate(paymentId)).thenReturn(Optional.of(p));

            assertThat(service.handleIpn(ipn).getRspCode()).isEqualTo("02");
        }

        @Test
        @DisplayName("returns INVALID_AMOUNT when gateway amount mismatches")
        void invalidAmount() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            // service expects amount * 100. If payment.amount=500 → expected=50000, send 60000.
            VNPayIpnRequest ipn = buildIpn(paymentId, 60000L, "00", "00");
            Payment p = Payment.builder().id(paymentId).amount(500L)
                    .status(PaymentStatus.PENDING).referenceType(ReferenceType.INVOICE).build();
            when(paymentRepository.findByIdForUpdate(paymentId)).thenReturn(Optional.of(p));

            assertThat(service.handleIpn(ipn).getRspCode()).isEqualTo("04");
        }

        @Test
        @DisplayName("marks payment SUCCESS and publishes events for INVOICE payment")
        void invoiceSuccess() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            UUID invoiceId = UUID.randomUUID();
            VNPayIpnRequest ipn = buildIpn(paymentId, 50000L, "00", "00");

            Payment p = Payment.builder().id(paymentId).amount(500L)
                    .status(PaymentStatus.PENDING).referenceType(ReferenceType.INVOICE)
                    .invoiceIds("[\"" + invoiceId + "\"]").build();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 500L, InvoiceType.MONTHLY_RENT);

            when(paymentRepository.findByIdForUpdate(paymentId)).thenReturn(Optional.of(p));
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));

            VNPayIpnResponse res = service.handleIpn(ipn);

            assertThat(res.getRspCode()).isEqualTo("00");
            assertThat(p.getStatus()).isEqualTo(PaymentStatus.SUCCESS);
            assertThat(p.getPaidAt()).isNotNull();
            assertThat(inv.getStatus()).isEqualTo(InvoiceStatus.PAID);
            verify(paymentRepository).save(p);
            verify(invoiceRepository).save(inv);
            verify(kafka).send(eq("payment.app-access-changed"), anyString(), any());
            verify(kafka).send(eq("payment-paid-topic"), any());
        }

        @Test
        @DisplayName("publishes deposit-paid-topic when invoice type is DEPOSIT")
        void depositSuccess() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            UUID invoiceId = UUID.randomUUID();
            VNPayIpnRequest ipn = buildIpn(paymentId, 100000L, "00", "00");

            Payment p = Payment.builder().id(paymentId).amount(1000L)
                    .status(PaymentStatus.PENDING).referenceType(ReferenceType.INVOICE)
                    .invoiceIds("[\"" + invoiceId + "\"]").build();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 1000L, InvoiceType.DEPOSIT);

            when(paymentRepository.findByIdForUpdate(paymentId)).thenReturn(Optional.of(p));
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));

            service.handleIpn(ipn);

            verify(kafka).send(eq("payment-paid-topic"), any());
            verify(kafka).send(eq("deposit-paid-topic"), any());
        }

        @Test
        @DisplayName("publishes quote-payment-completed for QUOTE payment")
        void quoteSuccess() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            UUID quoteId = UUID.randomUUID();
            VNPayIpnRequest ipn = buildIpn(paymentId, 50000L, "00", "00");

            Payment p = Payment.builder().id(paymentId).amount(500L)
                    .status(PaymentStatus.PENDING).referenceType(ReferenceType.QUOTE)
                    .referenceId(quoteId).tenantId(tenantId).build();

            when(paymentRepository.findByIdForUpdate(paymentId)).thenReturn(Optional.of(p));
            when(invoiceRepository.findByContractIdAndPeriodKey(eq(quoteId), anyString()))
                    .thenReturn(Optional.empty());

            VNPayIpnResponse res = service.handleIpn(ipn);
            assertThat(res.getRspCode()).isEqualTo("00");
            verify(kafka).send(eq("quote-payment-completed"), any());
        }

        @Test
        @DisplayName("marks FAILED and does not publish kafka when VNPay signals failure")
        void failureResponse() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            VNPayIpnRequest ipn = buildIpn(paymentId, 50000L, "24", "02");

            Payment p = Payment.builder().id(paymentId).amount(500L)
                    .status(PaymentStatus.PENDING).referenceType(ReferenceType.INVOICE)
                    .invoiceIds("[]").build();
            when(paymentRepository.findByIdForUpdate(paymentId)).thenReturn(Optional.of(p));

            VNPayIpnResponse res = service.handleIpn(ipn);

            assertThat(res.getRspCode()).isEqualTo("00");
            assertThat(p.getStatus()).isEqualTo(PaymentStatus.FAILED);
            assertThat(p.getPaidAt()).isNull();
            verifyNoInteractions(kafka);
        }

        @Test
        @DisplayName("returns UNKNOWN_ERROR when unexpected exception occurs")
        void unknownError() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            VNPayIpnRequest ipn = buildIpn(paymentId, 100L, "00", "00");
            when(paymentRepository.findByIdForUpdate(paymentId))
                    .thenThrow(new RuntimeException("DB lock timeout"));

            assertThat(service.handleIpn(ipn).getRspCode()).isEqualTo("99");
        }
    }

    @Nested
    @DisplayName("handleReturn")
    class HandleReturn {

        @Test
        @DisplayName("redirects to success URL when signature valid and response 00/00")
        void success() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            VNPayIpnRequest ipn = new HandleIpn().buildIpn(paymentId, 50000L, "00", "00");

            String url = service.handleReturn(ipn);
            assertThat(url).contains("status=success");
            assertThat(url).contains("txnRef=" + paymentId);
        }

        @Test
        @DisplayName("redirects to failed URL with code when response not 00")
        void failed() {
            when(vnPayProperties.getHashSecret()).thenReturn(HASH_SECRET);
            UUID paymentId = UUID.randomUUID();
            VNPayIpnRequest ipn = new HandleIpn().buildIpn(paymentId, 50000L, "24", "02");

            String url = service.handleReturn(ipn);
            assertThat(url).contains("status=failed");
            assertThat(url).contains("code=24");
        }
    }

    @Nested
    @DisplayName("resendPaymentNotification")
    class ResendNotification {

        @Test
        @DisplayName("throws EntityNotFoundException when invoice missing")
        void notFound() {
            UUID invoiceId = UUID.randomUUID();
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.resendPaymentNotification(invoiceId))
                    .isInstanceOf(EntityNotFoundException.class);
        }

        @Test
        @DisplayName("throws IllegalStateException when invoice already paid")
        void alreadyPaid() {
            UUID invoiceId = UUID.randomUUID();
            when(invoiceRepository.findById(invoiceId))
                    .thenReturn(Optional.of(invoice(invoiceId, InvoiceStatus.PAID, 1000L, InvoiceType.MONTHLY_RENT)));

            assertThatThrownBy(() -> service.resendPaymentNotification(invoiceId))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        @DisplayName("generates token and sends notification email")
        void happy() {
            UUID invoiceId = UUID.randomUUID();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 1000L, InvoiceType.MONTHLY_RENT);
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));
            when(paymentTokenService.generateToken(invoiceId, tenantId)).thenReturn("tok-123");
            when(userGrpcService.getTenantEmail(tenantId)).thenReturn("alice@example.com");

            service.resendPaymentNotification(invoiceId);

            verify(kafka).send(eq("notification-email"), any(SendEmailEvent.class));
        }
    }

    @Nested
    @DisplayName("getPublicInvoice")
    class GetPublicInvoice {

        @Test
        @DisplayName("returns public dto when invoice exists")
        void returnsDto() {
            UUID invoiceId = UUID.randomUUID();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 500L, InvoiceType.MONTHLY_RENT);
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));

            PublicInvoiceDto dto = service.getPublicInvoice(invoiceId);
            assertThat(dto.invoiceId()).isEqualTo(invoiceId);
            assertThat(dto.invoiceType()).isEqualTo("Tiền thuê tháng");
            assertThat(dto.amount()).isEqualTo(500L);
        }

        @Test
        @DisplayName("throws EntityNotFoundException when invoice missing")
        void notFound() {
            UUID invoiceId = UUID.randomUUID();
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.getPublicInvoice(invoiceId))
                    .isInstanceOf(EntityNotFoundException.class);
        }
    }

    @Nested
    @DisplayName("createPaymentVNPayLinkOutsystem")
    class Outsystem {

        @Test
        @DisplayName("throws when invoice missing")
        void notFound() {
            UUID invoiceId = UUID.randomUUID();
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.createPaymentVNPayLinkOutsystem(invoiceId, null, null, httpRequest))
                    .isInstanceOf(EntityNotFoundException.class);
        }

        @Test
        @DisplayName("creates payment link using tenantId from invoice (no JWT)")
        void happy() {
            UUID invoiceId = UUID.randomUUID();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 750L, InvoiceType.MONTHLY_RENT);
            stubVnPayProps();
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));
            when(httpRequest.getRemoteAddr()).thenReturn("127.0.0.1");
            stubSaveAssignsId();

            String url = service.createPaymentVNPayLinkOutsystem(invoiceId, null, null, httpRequest);
            assertThat(url).contains("vnp_Amount=75000");
        }
    }

    @Nested
    @DisplayName("getMyInvoices")
    class GetMyInvoices {

        @Test
        @DisplayName("uses houseId filter when provided")
        void withHouseId() {
            UUID houseId = UUID.randomUUID();
            mockResolveTenant();
            RentalInvoice inv = invoice(UUID.randomUUID(), InvoiceStatus.UNPAID, 1000L, InvoiceType.MONTHLY_RENT);
            when(invoiceRepository.findByTenantIdAndHouseIdOrderByDueDateAsc(tenantId, houseId))
                    .thenReturn(List.of(inv));
            when(invoiceMapper.toDtos(List.of(inv))).thenReturn(List.of());

            service.getMyInvoices(keycloakId, houseId);
            verify(invoiceRepository).findByTenantIdAndHouseIdOrderByDueDateAsc(tenantId, houseId);
            verify(invoiceRepository, never()).findByTenantIdOrderByDueDateAsc(any());
        }

        @Test
        @DisplayName("fetches all invoices for tenant when houseId null")
        void withoutHouseId() {
            mockResolveTenant();
            when(invoiceRepository.findByTenantIdOrderByDueDateAsc(tenantId)).thenReturn(List.of());
            when(invoiceMapper.toDtos(List.of())).thenReturn(List.of());

            service.getMyInvoices(keycloakId, null);
            verify(invoiceRepository).findByTenantIdOrderByDueDateAsc(tenantId);
        }
    }

    @Nested
    @DisplayName("getInvoiceById")
    class GetInvoiceById {

        @Test
        @DisplayName("throws EntityNotFoundException when invoice missing")
        void notFound() {
            UUID invoiceId = UUID.randomUUID();
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.getInvoiceById(invoiceId, keycloakId))
                    .isInstanceOf(EntityNotFoundException.class);
        }

        @Test
        @DisplayName("throws AccessDeniedException when caller is not the invoice tenant")
        void accessDenied() {
            UUID invoiceId = UUID.randomUUID();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 500L, InvoiceType.MONTHLY_RENT);
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));

            UUID otherUser = UUID.randomUUID();
            UserResponse resp = UserResponse.newBuilder().setId(otherUser.toString()).setName("Bob").build();
            when(userGrpcService.getUserIdAndRoleByKeyCloakId(keycloakId)).thenReturn(resp);

            assertThatThrownBy(() -> service.getInvoiceById(invoiceId, keycloakId))
                    .isInstanceOf(AccessDeniedException.class);
        }

        @Test
        @DisplayName("returns details including tenant info and payment history on happy path")
        void happy() {
            UUID invoiceId = UUID.randomUUID();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 500L, InvoiceType.MONTHLY_RENT);
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));

            UserResponse resp = UserResponse.newBuilder()
                    .setId(tenantId.toString()).setName("Alice").setPhoneNumber("0900").build();
            when(userGrpcService.getUserIdAndRoleByKeyCloakId(keycloakId)).thenReturn(resp);

            HouseResponse house = HouseResponse.newBuilder()
                    .setName("H1").setAddress("addr").build();
            when(houseGrpcClient.getHouse(inv.getHouseId())).thenReturn(house);

            Payment p = Payment.builder().id(UUID.randomUUID()).amount(500L)
                    .method(PaymentMethod.VNPAY).status(PaymentStatus.SUCCESS)
                    .gatewayTxnId("txn-1").paidAt(Instant.now()).createdAt(Instant.now()).build();
            when(paymentRepository.findByReferenceIdOrderByCreatedAtDesc(invoiceId)).thenReturn(List.of(p));

            InvoiceDetailDto dto = service.getInvoiceById(invoiceId, keycloakId);
            assertThat(dto.id()).isEqualTo(invoiceId);
            assertThat(dto.tenantName()).isEqualTo("Alice");
            assertThat(dto.houseName()).isEqualTo("H1");
            assertThat(dto.payments()).hasSize(1);
        }

        @Test
        @DisplayName("tolerates null house from gRPC (still returns details)")
        void houseNull() {
            UUID invoiceId = UUID.randomUUID();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 500L, InvoiceType.MONTHLY_RENT);
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));

            UserResponse resp = UserResponse.newBuilder()
                    .setId(tenantId.toString()).setName("Alice").build();
            when(userGrpcService.getUserIdAndRoleByKeyCloakId(keycloakId)).thenReturn(resp);
            when(houseGrpcClient.getHouse(inv.getHouseId())).thenReturn(null);
            when(paymentRepository.findByReferenceIdOrderByCreatedAtDesc(invoiceId)).thenReturn(List.of());

            InvoiceDetailDto dto = service.getInvoiceById(invoiceId, keycloakId);
            assertThat(dto.houseName()).isNull();
            assertThat(dto.houseAddress()).isNull();
        }
    }

    @Nested
    @DisplayName("markDepositRefundPaid")
    class MarkDepositRefundPaid {

        @Test
        @DisplayName("throws EntityNotFoundException when invoice missing")
        void notFound() {
            UUID invoiceId = UUID.randomUUID();
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.empty());

            assertThatThrownBy(() -> service.markDepositRefundPaid(invoiceId,
                    new MarkRefundPaidRequest(RefundPaymentMethod.BANK_TRANSFER, "note")))
                    .isInstanceOf(EntityNotFoundException.class);
        }

        @Test
        @DisplayName("throws when invoice type is not DEPOSIT_REFUND")
        void wrongType() {
            UUID invoiceId = UUID.randomUUID();
            when(invoiceRepository.findById(invoiceId))
                    .thenReturn(Optional.of(invoice(invoiceId, InvoiceStatus.UNPAID, 1000L, InvoiceType.MONTHLY_RENT)));

            assertThatThrownBy(() -> service.markDepositRefundPaid(invoiceId,
                    new MarkRefundPaidRequest(RefundPaymentMethod.BANK_TRANSFER, null)))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("hoàn cọc");
        }

        @Test
        @DisplayName("throws when invoice already paid")
        void alreadyPaid() {
            UUID invoiceId = UUID.randomUUID();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.PAID, 1000L, InvoiceType.DEPOSIT_REFUND);
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));

            assertThatThrownBy(() -> service.markDepositRefundPaid(invoiceId,
                    new MarkRefundPaidRequest(RefundPaymentMethod.CASH, null)))
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        @DisplayName("marks PAID, saves refund method/note and publishes event")
        void happy() {
            UUID invoiceId = UUID.randomUUID();
            RentalInvoice inv = invoice(invoiceId, InvoiceStatus.UNPAID, 1000L, InvoiceType.DEPOSIT_REFUND);
            when(invoiceRepository.findById(invoiceId)).thenReturn(Optional.of(inv));

            MarkRefundPaidRequest req = new MarkRefundPaidRequest(RefundPaymentMethod.BANK_TRANSFER, "paid via bank");
            service.markDepositRefundPaid(invoiceId, req);

            assertThat(inv.getStatus()).isEqualTo(InvoiceStatus.PAID);
            assertThat(inv.getRefundPaymentMethod()).isEqualTo("BANK_TRANSFER");
            assertThat(inv.getRefundNote()).isEqualTo("paid via bank");
            assertThat(inv.getPaidAt()).isNotNull();
            verify(invoiceRepository).save(inv);
            verify(kafka).send(eq("deposit-refund-paid-topic"), anyString(), any());
        }
    }
}
