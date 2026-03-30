package com.isums.paymentservice.domains.enums;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum VNPayIpnCode {

    SUCCESS             ("00", "Confirm Success"),
    ALREADY_PROCESSED   ("02", "Order already confirmed"),

    // VNPAY sẽ retry
    ORDER_NOT_FOUND     ("01", "Order not found"),
    INVALID_AMOUNT      ("04", "Invalid amount"),
    INVALID_SIGNATURE   ("97", "Invalid checksum"),
    UNKNOWN_ERROR       ("99", "Unknown error");

    private final String rspCode;
    private final String message;
}
