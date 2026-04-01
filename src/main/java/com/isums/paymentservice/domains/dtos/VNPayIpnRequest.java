package com.isums.paymentservice.domains.dtos;

import lombok.Data;

@Data
public class VNPayIpnRequest {

    private String vnp_TmnCode;

    private Long vnp_Amount;

    private String vnp_BankCode;

    private String vnp_BankTranNo;

    private String vnp_CardType;

    private String vnp_PayDate;

    private String vnp_OrderInfo;

    private String vnp_TransactionNo;

    private String vnp_ResponseCode;

    private String vnp_TransactionStatus;

    private String vnp_TxnRef;

    private String vnp_SecureHash;
}