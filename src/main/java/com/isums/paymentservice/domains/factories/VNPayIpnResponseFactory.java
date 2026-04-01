package com.isums.paymentservice.domains.factories;

import com.isums.paymentservice.domains.dtos.VNPayIpnResponse;
import com.isums.paymentservice.domains.enums.VNPayIpnCode;

public class VNPayIpnResponseFactory {

    private VNPayIpnResponseFactory() {
    }

    public static VNPayIpnResponse from(VNPayIpnCode code) {
        return new VNPayIpnResponse(code.getRspCode(), code.getMessage());
    }
}
