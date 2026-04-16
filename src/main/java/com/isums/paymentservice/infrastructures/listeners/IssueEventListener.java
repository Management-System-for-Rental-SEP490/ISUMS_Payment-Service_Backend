package com.isums.paymentservice.infrastructures.listeners;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.events.QuoteInvoiceCreateEvent;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class IssueEventListener {

    private final RentalInvoiceRepository invoiceRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "quote-invoice-create", groupId = "payment-service")
    @Transactional
    public void handle(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            QuoteInvoiceCreateEvent event = objectMapper.readValue(record.value(), QuoteInvoiceCreateEvent.class);

            String periodKey = "QUOTE-" + event.getQuoteId();
            boolean exists = invoiceRepository.existsByContractIdAndPeriodKey(event.getQuoteId(), periodKey);
            if (exists) {
                log.warn("[Invoice] Already exists for quoteId={}, skipping", event.getQuoteId());
                ack.acknowledge();
                return;
            }

            long totalAmount = event.getTotalPrice().longValue();

            RentalInvoice invoice = RentalInvoice.builder()
                    .contractId(event.getQuoteId())
                    .tenantId(event.getTenantId())
                    .houseId(event.getHouseId())
                    .quoteId(event.getQuoteId())
                    .type(InvoiceType.ISSUE)
                    .periodKey(periodKey)
                    .baseAmount(totalAmount)
                    .serviceAmount(0L)
                    .penaltyAmount(0L)
                    .totalAmount(totalAmount)
                    .status(InvoiceStatus.UNPAID)
                    .dueDate(Instant.now().plus(7, ChronoUnit.DAYS))
                    .build();

            invoiceRepository.save(invoice);

            ack.acknowledge();
            log.info("[Invoice] Created MAINTENANCE invoice={} quoteId={} tenantId={} amount={}",
                    invoice.getId(), event.getQuoteId(), event.getTenantId(), totalAmount);

        } catch (com.fasterxml.jackson.core.JacksonException e) {
            log.error("[Invoice] Deserialize failed raw={}: {}", record.value(), e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Invoice] handle failed: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}