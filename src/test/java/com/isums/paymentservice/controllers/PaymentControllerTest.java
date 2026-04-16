package com.isums.paymentservice.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.isums.paymentservice.domains.dtos.*;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.enums.RefundPaymentMethod;
import com.isums.paymentservice.exceptions.GlobalExceptionHandler;
import com.isums.paymentservice.infrastructures.Abtracts.PaymentService;
import com.isums.paymentservice.services.PaymentTokenService;
import jakarta.persistence.EntityNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.support.WebDataBinderFactory;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodArgumentResolver;
import org.springframework.web.method.support.ModelAndViewContainer;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@DisplayName("PaymentController")
class PaymentControllerTest {

    @Mock private PaymentService paymentService;
    @Mock private PaymentTokenService paymentTokenService;

    @InjectMocks private PaymentController controller;

    private MockMvc mvc;
    private final ObjectMapper om = new ObjectMapper();
    private String keycloakId;

    @BeforeEach
    void setUp() {
        keycloakId = UUID.randomUUID().toString();
        Jwt jwt = Jwt.withTokenValue("t").header("alg","none").subject(keycloakId).build();

        HandlerMethodArgumentResolver jwtResolver = new HandlerMethodArgumentResolver() {
            @Override public boolean supportsParameter(MethodParameter p) {
                return Jwt.class.equals(p.getParameterType());
            }
            @Override public Object resolveArgument(MethodParameter p, ModelAndViewContainer m,
                                                    NativeWebRequest w, WebDataBinderFactory b) { return jwt; }
        };

        mvc = MockMvcBuilders.standaloneSetup(controller)
                .setCustomArgumentResolvers(jwtResolver)
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    @DisplayName("GET /invoices returns tenant invoices")
    void getMyInvoices() throws Exception {
        UUID id = UUID.randomUUID();
        InvoiceDto dto = new InvoiceDto(id, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                InvoiceType.MONTHLY_RENT, "2026-04", 500L, 0L, 0L, 500L,
                InvoiceStatus.UNPAID, Instant.now(), null, Instant.now());
        when(paymentService.getMyInvoices(keycloakId, null)).thenReturn(List.of(dto));

        mvc.perform(get("/api/payments/invoices"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data[0].id").value(id.toString()));
    }

    @Test
    @DisplayName("GET /invoices?houseId=X forwards houseId filter")
    void getMyInvoicesWithHouseId() throws Exception {
        UUID houseId = UUID.randomUUID();
        when(paymentService.getMyInvoices(keycloakId, houseId)).thenReturn(List.of());

        mvc.perform(get("/api/payments/invoices").param("houseId", houseId.toString()))
                .andExpect(status().isOk());

        verify(paymentService).getMyInvoices(keycloakId, houseId);
    }

    @Test
    @DisplayName("GET /invoices/{id} returns detail on happy path")
    void getInvoiceById() throws Exception {
        UUID id = UUID.randomUUID();
        InvoiceDetailDto dto = new InvoiceDetailDto(id, UUID.randomUUID(), null,
                InvoiceType.MONTHLY_RENT, "2026-04", 500L, 0L, 0L, 500L,
                InvoiceStatus.UNPAID, Instant.now(), null, Instant.now(),
                UUID.randomUUID(), "Alice", "a@b.com", "0900",
                UUID.randomUUID(), "H1", "addr", List.of(), null, List.of());
        when(paymentService.getInvoiceById(id, keycloakId)).thenReturn(dto);

        mvc.perform(get("/api/payments/invoices/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.tenantName").value("Alice"));
    }

    @Test
    @DisplayName("GET /invoices/{id} returns 403 when AccessDeniedException thrown")
    void getInvoiceAccessDenied() throws Exception {
        UUID id = UUID.randomUUID();
        when(paymentService.getInvoiceById(id, keycloakId))
                .thenThrow(new AccessDeniedException("denied"));

        mvc.perform(get("/api/payments/invoices/{id}", id))
                .andExpect(status().isForbidden());
    }

    @Test
    @DisplayName("GET /invoices/{id} returns 404 when invoice missing")
    void getInvoiceNotFound() throws Exception {
        UUID id = UUID.randomUUID();
        when(paymentService.getInvoiceById(id, keycloakId))
                .thenThrow(new EntityNotFoundException("Invoice not found"));

        mvc.perform(get("/api/payments/invoices/{id}", id))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("POST /vnpay returns 200 with payment URL")
    void createPaymentLink() throws Exception {
        CreatePaymentRequest req = new CreatePaymentRequest(
                List.of(UUID.randomUUID().toString()), null, null, null);
        when(paymentService.createPaymentVNPayLink(any(), any(), eq(keycloakId)))
                .thenReturn("https://vnpay.example/pay?...");

        mvc.perform(post("/api/payments/vnpay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsString(req)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").value("https://vnpay.example/pay?..."));
    }

    @Test
    @DisplayName("GET /vnpay/ipn returns response body from service (no JWT)")
    void ipn() throws Exception {
        VNPayIpnResponse res = new VNPayIpnResponse("00", "Confirm Success");
        when(paymentService.handleIpn(any())).thenReturn(res);

        mvc.perform(get("/api/payments/vnpay/ipn"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.RspCode").value("00"));
    }

    @Test
    @DisplayName("GET /vnpay/return redirects to URL from service")
    void vnpayReturn() throws Exception {
        when(paymentService.handleReturn(any()))
                .thenReturn("https://out.example/result?status=success");

        mvc.perform(get("/api/payments/vnpay/return"))
                .andExpect(status().isFound())
                .andExpect(header().string("Location", "https://out.example/result?status=success"));
    }

    @Test
    @DisplayName("GET /outsystem/invoices/{id} validates token before returning data")
    void publicInvoice() throws Exception {
        UUID id = UUID.randomUUID();
        PublicInvoiceDto dto = new PublicInvoiceDto(id, "Tiền thuê tháng", 500L, "13/04/2026", InvoiceStatus.UNPAID);
        when(paymentService.getPublicInvoice(id)).thenReturn(dto);

        mvc.perform(get("/api/payments/outsystem/invoices/{id}", id).param("token", "good"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data.invoiceId").value(id.toString()));

        verify(paymentTokenService).validateToken("good", id);
    }

    @Test
    @DisplayName("GET /outsystem/invoices/{id} returns 400 when token invalid")
    void publicInvoiceBadToken() throws Exception {
        UUID id = UUID.randomUUID();
        doThrow(new IllegalArgumentException("Link thanh toán đã hết hạn"))
                .when(paymentTokenService).validateToken("bad", id);

        mvc.perform(get("/api/payments/outsystem/invoices/{id}", id).param("token", "bad"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST /outsystem/vnpay returns payment URL without JWT")
    void outsystemPayment() throws Exception {
        UUID invoiceId = UUID.randomUUID();
        when(paymentService.createPaymentVNPayLinkOutsystem(eq(invoiceId), any(), any(), any()))
                .thenReturn("https://vnpay.example/pay");

        mvc.perform(post("/api/payments/outsystem/vnpay")
                        .param("invoiceId", invoiceId.toString()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.data").value("https://vnpay.example/pay"));
    }

    @Test
    @DisplayName("POST /invoices/{id}/resend returns 200 on happy path")
    void resend() throws Exception {
        UUID id = UUID.randomUUID();

        mvc.perform(post("/api/payments/invoices/{id}/resend", id))
                .andExpect(status().isOk());

        verify(paymentService).resendPaymentNotification(id);
    }

    @Test
    @DisplayName("POST /invoices/{id}/resend returns 400 when invoice already paid")
    void resendAlreadyPaid() throws Exception {
        UUID id = UUID.randomUUID();
        doThrow(new IllegalStateException("already paid"))
                .when(paymentService).resendPaymentNotification(id);

        mvc.perform(post("/api/payments/invoices/{id}/resend", id))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("PUT /{id}/mark-refund-paid returns 200 on happy path")
    void markRefundPaid() throws Exception {
        UUID id = UUID.randomUUID();
        MarkRefundPaidRequest req = new MarkRefundPaidRequest(RefundPaymentMethod.BANK_TRANSFER, "ok");

        mvc.perform(put("/api/payments/{id}/mark-refund-paid", id)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(om.writeValueAsString(req)))
                .andExpect(status().isOk());

        verify(paymentService).markDepositRefundPaid(eq(id), any());
    }
}
