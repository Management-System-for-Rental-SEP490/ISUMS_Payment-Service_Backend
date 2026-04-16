package com.isums.paymentservice.controllers;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class PaymentOpenApiForwardController {

    private static final String API_DOCS_PREFIX = "/api/payments/v3/api-docs";

    @GetMapping({API_DOCS_PREFIX, API_DOCS_PREFIX + "/{*path}"})
    public String forwardPrefixedApiDocs(HttpServletRequest request) {
        String requestUri = request.getRequestURI();
        String suffix = requestUri.length() > API_DOCS_PREFIX.length()
                ? requestUri.substring(API_DOCS_PREFIX.length())
                : "";
        String query = request.getQueryString();
        String target = "forward:/v3/api-docs" + suffix;
        return query == null ? target : target + "?" + query;
    }
}
