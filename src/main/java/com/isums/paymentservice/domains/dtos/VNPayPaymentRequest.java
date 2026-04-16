package com.isums.paymentservice.domains.dtos;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class VNPayPaymentRequest {

    private String vnp_Version = "2.1.0";
    private String vnp_Command = "pay";
    private String vnp_TmnCode;
    private Long vnp_Amount;
    private String vnp_BankCode;
    private String vnp_CreateDate;
    private String vnp_CurrCode = "VND";
    private String vnp_IpAddr;
    private String vnp_Locale;
    private String vnp_OrderInfo;
    private String vnp_OrderType;
    private String vnp_ReturnUrl;
    private String vnp_ExpireDate;
    private String vnp_TxnRef;
    private String vnp_SecureHash;
}
