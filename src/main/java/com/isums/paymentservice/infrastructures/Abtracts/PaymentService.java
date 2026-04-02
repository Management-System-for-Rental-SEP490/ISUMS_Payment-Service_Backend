package com.isums.paymentservice.infrastructures.Abtracts;

import com.isums.paymentservice.domains.dtos.*;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import jakarta.servlet.http.HttpServletRequest;
import org.jspecify.annotations.Nullable;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.UUID;

public interface PaymentService {

    String createPaymentVNPayLink(CreatePaymentRequest request, HttpServletRequest httpRequest);

    VNPayIpnResponse handleIpn(VNPayIpnRequest request);

    void resendPaymentNotification(@PathVariable UUID invoiceId);

    PublicInvoiceDto getPublicInvoice(UUID invoiceId);

    String createPaymentVNPayLinkOutsystem(UUID invoiceId, String bankCode, String locale, HttpServletRequest httpRequest);

    List<InvoiceDto> getMyInvoices(String keycloakId, @Nullable UUID houseId);
}
