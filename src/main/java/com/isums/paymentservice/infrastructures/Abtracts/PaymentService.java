package com.isums.paymentservice.infrastructures.Abtracts;

import com.isums.paymentservice.domains.dtos.CreatePaymentRequest;
import com.isums.paymentservice.domains.dtos.PublicInvoiceDto;
import com.isums.paymentservice.domains.dtos.VNPayIpnRequest;
import com.isums.paymentservice.domains.dtos.VNPayIpnResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.UUID;

public interface PaymentService {

    String createPaymentVNPayLink(CreatePaymentRequest request, HttpServletRequest httpRequest);

    VNPayIpnResponse handleIpn(VNPayIpnRequest request);

    void resendPaymentNotification(@PathVariable UUID invoiceId);

    PublicInvoiceDto getPublicInvoice(UUID invoiceId);

    String createPaymentVNPayLinkOutsystem(UUID invoiceId, String bankCode, String locale, HttpServletRequest httpRequest);
}
