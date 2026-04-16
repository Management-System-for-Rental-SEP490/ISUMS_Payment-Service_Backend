package com.isums.paymentservice.domains.events;

import java.util.Map;

@lombok.Builder
public record SendEmailEvent(
        String to,
        String templateCode,
        Map<String, Object> params
) {}