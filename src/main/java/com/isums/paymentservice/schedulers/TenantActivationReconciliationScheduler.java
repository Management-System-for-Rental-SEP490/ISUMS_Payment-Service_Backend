package com.isums.paymentservice.schedulers;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.events.MapUserToHouseEvent;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class TenantActivationReconciliationScheduler {

    private final RentalInvoiceRepository invoiceRepository;
    private final KafkaTemplate<String, Object> kafka;

    @Value("${reconcile.tenant-activation.lookback-hours:24}")
    private int lookbackHours;

    @Scheduled(fixedDelayString = "${reconcile.tenant-activation.delay-ms:600000}",
               initialDelayString = "${reconcile.tenant-activation.initial-delay-ms:60000}")
    public void republishRecentTenantActivations() {
        Instant cutoff = Instant.now().minus(lookbackHours, ChronoUnit.HOURS);

        List<RentalInvoice> recentPaidDeposits = invoiceRepository
                .findByTypeAndStatusAndPaidAtAfter(InvoiceType.DEPOSIT, InvoiceStatus.PAID, cutoff);

        if (recentPaidDeposits.isEmpty()) {
            return;
        }

        int published = 0;
        int skipped = 0;
        for (RentalInvoice deposit : recentPaidDeposits) {
            if (deposit.getTenantId() == null || deposit.getHouseId() == null) {
                skipped++;
                continue;
            }
            try {
                kafka.send("map-user-to-house-topic", MapUserToHouseEvent.builder()
                        .userId(deposit.getTenantId())
                        .houseId(deposit.getHouseId())
                        .handoverDate(deposit.getContractStartAt())
                        .build());
                published++;
            } catch (Exception e) {
                log.error("[ActivationReconcile] republish failed invoiceId={} tenantId={} houseId={}: {}",
                        deposit.getId(), deposit.getTenantId(), deposit.getHouseId(), e.getMessage(), e);
            }
        }

        log.info("[ActivationReconcile] lookbackHours={} candidates={} published={} skipped={}",
                lookbackHours, recentPaidDeposits.size(), published, skipped);
    }
}
