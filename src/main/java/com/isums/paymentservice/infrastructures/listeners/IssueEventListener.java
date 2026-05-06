package com.isums.paymentservice.infrastructures.listeners;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.enums.PaymentMethod;
import com.isums.paymentservice.domains.enums.PaymentStatus;
import com.isums.paymentservice.domains.enums.ReferenceType;
import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.domains.events.QuoteCashPaymentConfirmedEvent;
import com.isums.paymentservice.domains.events.QuoteInvoiceCreateEvent;
import com.isums.paymentservice.domains.events.QuotePaymentCompletedEvent;
import com.isums.paymentservice.infrastructures.repositories.PaymentRepository;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
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
    private final PaymentRepository paymentRepository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, Object> kafka;

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

    @KafkaListener(topics = "quote-cash-payment-confirmed", groupId = "payment-service")
    @Transactional
    public void handleCashPaymentConfirmed(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            QuoteCashPaymentConfirmedEvent event = objectMapper.readValue(
                    record.value(),
                    QuoteCashPaymentConfirmedEvent.class
            );

            String periodKey = "QUOTE-" + event.getQuoteId();
            invoiceRepository.findByContractIdAndPeriodKey(event.getQuoteId(), periodKey)
                    .ifPresent(invoice -> {
                        if (invoice.getStatus() != InvoiceStatus.PAID) {
                            invoice.setStatus(InvoiceStatus.PAID);
                            invoice.setPaidAt(event.getPaidAt());
                            invoiceRepository.save(invoice);
                        }
                    });

            if (paymentRepository.findByGatewayTxnId(event.getTxnNo()).isEmpty()) {
                paymentRepository.save(Payment.builder()
                        .referenceId(event.getQuoteId())
                        .referenceType(ReferenceType.QUOTE)
                        .tenantId(event.getTenantId())
                        .payerUserId(event.getTenantId())
                        .amount(event.getAmount().longValue())
                        .method(PaymentMethod.CASH)
                        .status(PaymentStatus.SUCCESS)
                        .gatewayTxnId(event.getTxnNo())
                        .gatewayResponse("{\"method\":\"CASH\"}")
                        .note("Cash payment confirmed by staff")
                        .paidAt(event.getPaidAt())
                        .build());
            }

            kafka.send("quote-payment-completed", QuotePaymentCompletedEvent.builder()
                    .quoteId(event.getQuoteId())
                    .issueId(event.getIssueId())
                    .tenantId(event.getTenantId())
                    .amount(event.getAmount())
                    .txnNo(event.getTxnNo())
                    .paidAt(event.getPaidAt())
                    .build());

            ack.acknowledge();
            log.info("[Invoice] Cash payment confirmed quoteId={} issueId={} txnNo={}",
                    event.getQuoteId(), event.getIssueId(), event.getTxnNo());
        } catch (com.fasterxml.jackson.core.JacksonException e) {
            log.error("[Invoice] Cash payment deserialize failed raw={}: {}", record.value(), e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Invoice] Cash payment handling failed: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
