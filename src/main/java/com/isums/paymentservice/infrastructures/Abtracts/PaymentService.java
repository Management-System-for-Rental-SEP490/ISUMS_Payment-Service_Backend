package com.isums.paymentservice.infrastructures.Abtracts;

import com.isums.paymentservice.domains.dtos.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.List;
import java.util.UUID;

public interface PaymentService {

    String createPaymentVNPayLink(CreatePaymentRequest request, HttpServletRequest httpRequest, String keycloakId);

    /**
     * Create a VNPay link for the Notification-Service PREMIUM
     * subscription. After IPN success the service emits Kafka topic
     * {@code payment.subscription-activated}; Notification-Service
     * consumes it and flips the user's tier to PREMIUM.
     */
    String createSubscriptionPaymentLink(CreateSubscriptionPaymentRequest request,
                                           HttpServletRequest httpRequest,
                                           String keycloakId);

    VNPayIpnResponse handleIpn(VNPayIpnRequest request);

    void resendPaymentNotification(@PathVariable UUID invoiceId);

    PublicInvoiceDto getPublicInvoice(UUID invoiceId);

    String createPaymentVNPayLinkOutsystem(UUID invoiceId, String bankCode, String locale, HttpServletRequest httpRequest);

    List<InvoiceDto> getMyInvoices(String keycloakId, UUID houseId);

    InvoiceDetailDto getInvoiceById(UUID invoiceId, String keycloakId);
    
    List<PaymentMethodOptionDto> getAvailablePaymentMethods();

    void markDepositRefundPaid(UUID invoiceId, MarkRefundPaidRequest req);

    String handleReturn(VNPayIpnRequest request);
}
