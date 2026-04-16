package com.isums.paymentservice.domains.dtos;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class VNPayIpnResponse {

    private String RspCode;
    private String Message;

}
