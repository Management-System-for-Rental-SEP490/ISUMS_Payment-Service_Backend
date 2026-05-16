package com.isums.paymentservice.configurations;

import common.paginations.configurations.IsumCacheConfigurer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;

@Component
public class RedisConfig implements IsumCacheConfigurer {

    public static final String FINANCE_DASHBOARD_CACHE = "finance-dashboard";
    public static final String TARIFF_CACHE = "tariff-config";

    @Override
    public Map<String, Duration> cacheTtls() {
        return Map.of(
                FINANCE_DASHBOARD_CACHE, Duration.ofMinutes(5),
                TARIFF_CACHE, Duration.ofHours(1)
        );
    }
}
