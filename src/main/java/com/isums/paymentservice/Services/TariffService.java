package com.isums.paymentservice.Services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.isums.paymentservice.domains.dtos.CreateTariffConfigRequest;
import com.isums.paymentservice.domains.dtos.TariffConfigDto;
import com.isums.paymentservice.domains.dtos.TariffConfigVersionDto;
import com.isums.paymentservice.domains.dtos.TariffTierDto;
import com.isums.paymentservice.domains.entities.TariffConfigVersion;
import com.isums.paymentservice.infrastructures.repositories.TariffConfigVersionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TariffService {

    private static final String TARIFF_CACHE = "tariff-config";

    private final TariffConfigVersionRepository repository;
    private final ObjectMapper objectMapper;

    @Cacheable(value = TARIFF_CACHE, key = "'electricity:residential:VN'")
    @Transactional(readOnly = true)
    public TariffConfigDto getElectricityResidentialTariff() {
        return getActiveTariff("electricity", "residential", "VN");
    }

    @Cacheable(value = TARIFF_CACHE, key = "'water:residential:' + #region")
    @Transactional(readOnly = true)
    public TariffConfigDto getWaterResidentialTariff(String region) {
        String r = (region == null || region.isBlank()) ? "HCM" : region.trim().toUpperCase();
        return repository.findActive("water", "residential", r)
                .map(this::parseConfig)
                .orElseGet(() -> getActiveTariff("water", "residential", "HCM"));
    }

    @Transactional(readOnly = true)
    public List<TariffConfigVersionDto> getHistory(String metric, String plan, String region) {
        return repository.findHistory(metric, plan, region).stream()
                .map(this::toDto)
                .toList();
    }

    @Transactional
    @CacheEvict(value = TARIFF_CACHE, allEntries = true)
    public TariffConfigVersionDto createVersion(UUID actorId, CreateTariffConfigRequest request) {
        validatePayload(request.configJson());
        Instant now = Instant.now();

        repository.findActive(request.metric(), request.plan(), request.region())
                .ifPresent(active -> {
                    active.setExpiredAt(now);
                    active.setExpiredBy(actorId);
                    repository.save(active);
                });

        String json;
        try {
            json = objectMapper.writeValueAsString(request.configJson());
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Invalid configJson: " + ex.getMessage(), ex);
        }

        TariffConfigVersion saved = repository.save(TariffConfigVersion.builder()
                .id(UUID.randomUUID())
                .metric(request.metric())
                .plan(request.plan())
                .region(request.region())
                .version(request.version())
                .configJson(json)
                .effectiveFrom(now)
                .notes(request.notes())
                .createdBy(actorId)
                .createdAt(now)
                .build());

        log.info("[Tariff] New version published id={} metric={} plan={} region={} version={} by={}",
                saved.getId(), saved.getMetric(), saved.getPlan(), saved.getRegion(),
                saved.getVersion(), actorId);
        return toDto(saved);
    }

    @Transactional
    @CacheEvict(value = TARIFF_CACHE, allEntries = true)
    public TariffConfigVersionDto expireVersion(UUID id, UUID actorId) {
        TariffConfigVersion v = repository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Version not found: " + id));
        if (v.getExpiredAt() != null) {
            throw new IllegalStateException("Version already expired: " + id);
        }
        v.setExpiredAt(Instant.now());
        v.setExpiredBy(actorId);
        TariffConfigVersion saved = repository.save(v);
        log.info("[Tariff] Expired version id={} by={}", id, actorId);
        return toDto(saved);
    }

    private TariffConfigDto getActiveTariff(String metric, String plan, String region) {
        return repository.findActive(metric, plan, region)
                .map(this::parseConfig)
                .orElseThrow(() -> new IllegalStateException(
                        "No active tariff version for " + metric + "/" + plan + "/" + region +
                                ". Run migration V20260507_1600."));
    }

    private TariffConfigDto parseConfig(TariffConfigVersion v) {
        try {
            JsonNode root = objectMapper.readTree(v.getConfigJson());
            List<TariffTierDto> tiers = objectMapper.convertValue(
                    root.path("tiers"),
                    objectMapper.getTypeFactory().constructCollectionType(List.class, TariffTierDto.class));
            String surchargeLabel = root.path("surchargeLabel").isNull()
                    ? null
                    : root.path("surchargeLabel").asText(null);
            String notes = root.path("notes").isNull() ? null : root.path("notes").asText(null);
            return new TariffConfigDto(
                    v.getMetric(),
                    v.getPlan(),
                    v.getRegion(),
                    root.path("currency").asText("VND"),
                    root.path("unit").asText(""),
                    tiers,
                    root.path("vatRate").asDouble(0),
                    root.path("surchargeRate").asDouble(0),
                    surchargeLabel,
                    root.path("source").asText(""),
                    root.path("effectiveFrom").asText(""),
                    v.getVersion(),
                    notes);
        } catch (Exception ex) {
            throw new IllegalStateException(
                    "Failed to parse tariff version " + v.getId() + ": " + ex.getMessage(), ex);
        }
    }

    private TariffConfigVersionDto toDto(TariffConfigVersion v) {
        return new TariffConfigVersionDto(
                v.getId(),
                v.getMetric(),
                v.getPlan(),
                v.getRegion(),
                v.getVersion(),
                parseConfig(v),
                v.getEffectiveFrom(),
                v.getExpiredAt(),
                v.getNotes(),
                v.getCreatedBy(),
                v.getCreatedAt(),
                v.getExpiredBy(),
                v.getExpiredAt() == null);
    }

    private void validatePayload(JsonNode root) {
        if (root == null || root.isNull() || root.isMissingNode()) {
            throw new IllegalArgumentException("configJson is required");
        }
        if (!root.path("tiers").isArray() || root.path("tiers").size() == 0) {
            throw new IllegalArgumentException("tiers[] is required and non-empty");
        }
        double vatRate = root.path("vatRate").asDouble(0);
        if (vatRate < 0 || vatRate > 1) {
            throw new IllegalArgumentException("vatRate must be in [0, 1]");
        }
    }
}
