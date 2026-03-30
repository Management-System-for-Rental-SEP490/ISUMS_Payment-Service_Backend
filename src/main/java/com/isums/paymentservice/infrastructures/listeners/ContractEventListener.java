package com.isums.paymentservice.infrastructures.listeners;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.events.ContractCompletedEvent;
import com.isums.paymentservice.domains.events.SendEmailEvent;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.isums.paymentservice.services.PaymentTokenService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContractEventListener {

    private final RentalInvoiceRepository invoiceRepository;
    private final PaymentTokenService paymentTokenService;
    private final KafkaTemplate<String, Object> kafka;
    private final ObjectMapper objectMapper;

    @Value("${app.payment.outsystem-url:https://outsystem.isums.pro/payments}")
    private String outsystemPaymentUrl;

    private static final ZoneId VN = ZoneId.of("Asia/Ho_Chi_Minh");
    private static final DateTimeFormatter DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(VN);

    @KafkaListener(topics = "contract-completed-topic", groupId = "payment-group")
    public void handleContractCompleted(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            ContractCompletedEvent event = objectMapper.readValue(record.value(), ContractCompletedEvent.class);

            log.info("[Payment] ContractCompleted contractId={} tenantId={} deposit={} rent={}",
                    event.getContractId(), event.getTenantId(), event.getDepositAmount(), event.getRentAmount());

            if (invoiceRepository.existsByContractIdAndPeriodKey(event.getContractId(), "DEPOSIT")) {
                log.warn("[Payment] Invoices already created contractId={}, skip", event.getContractId());
                ack.acknowledge();
                return;
            }

            List<RentalInvoice> invoices = new ArrayList<>();

            if (event.getDepositAmount() != null && event.getDepositAmount() > 0) {
                invoices.add(RentalInvoice.builder()
                        .contractId(event.getContractId())
                        .tenantId(event.getTenantId())
                        .houseId(event.getHouseId())
                        .type(InvoiceType.DEPOSIT)
                        .periodKey("DEPOSIT")
                        .baseAmount(event.getDepositAmount())
                        .serviceAmount(0L)
                        .penaltyAmount(0L)
                        .totalAmount(event.getDepositAmount())
                        .status(InvoiceStatus.UNPAID)
                        .dueDate(Instant.now().plusSeconds(3 * 24 * 3600))
                        .build());
            }

            if (event.getRentAmount() != null && event.getRentAmount() > 0) {
                Instant firstDue = calcFirstRentDue(event.getStartAt(), event.getPayDate());
                String periodKey = "RENT_" + firstDue.atZone(VN).format(DateTimeFormatter.ofPattern("yyyyMM"));

                invoices.add(RentalInvoice.builder()
                        .contractId(event.getContractId())
                        .tenantId(event.getContractId())
                        .houseId(event.getContractId())
                        .type(InvoiceType.MONTHLY_RENT)
                        .periodKey(periodKey)
                        .baseAmount(event.getRentAmount())
                        .serviceAmount(0L)
                        .penaltyAmount(0L)
                        .totalAmount(event.getRentAmount())
                        .status(InvoiceStatus.UNPAID)
                        .dueDate(firstDue)
                        .build());
            }

            invoiceRepository.saveAll(invoices);
            log.info("[Payment] Created {} invoices contractId={}", invoices.size(), event.getContractId());

            if (event.getTenantEmail() != null && !event.getTenantEmail().isBlank()) {
                sendPaymentEmails(invoices, event);
            } else {
                log.warn("[Payment] tenantEmail null — skip email notification contractId={}", event.getContractId());
            }

            ack.acknowledge();

        } catch (JacksonException e) {
            log.error("[Payment] Deserialize failed raw={}: {}", record.value(), e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Payment] Processing failed, will retry: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void sendPaymentEmails(List<RentalInvoice> invoices, ContractCompletedEvent event) {
        for (RentalInvoice invoice : invoices) {
            try {
                String token = paymentTokenService.generateToken(invoice.getId(), event.getTenantId());
                String paymentUrl = outsystemPaymentUrl + "?invoiceId=" + invoice.getId();

                Map<String, Object> params = new HashMap<>();
                params.put("invoiceType", invoice.getType() == InvoiceType.DEPOSIT ? "Tiền cọc" : "Tiền thuê tháng đầu");
                params.put("amount", formatVnd(invoice.getTotalAmount()));
                params.put("dueDate", DMY.format(invoice.getDueDate()));
                params.put("paymentUrl", paymentUrl);
                params.put("expiresIn", "7 ngày");

                kafka.send("notification-email", SendEmailEvent.builder()
                        .to(event.getTenantEmail())
                        .templateCode("PAYMENT_INVOICE")
                        .params(params)
                        .build());

                log.info("[Payment] Email queued invoiceId={} type={} to={}",
                        invoice.getId(), invoice.getType(), event.getTenantEmail());

            } catch (Exception e) {
                log.error("[Payment] Send email failed invoiceId={}: {}", invoice.getId(), e.getMessage(), e);
            }
        }
    }

    private Instant calcFirstRentDue(Instant startAt, Integer payDate) {
        if (payDate == null || startAt == null) return startAt;

        ZonedDateTime start = startAt.atZone(VN);
        int maxDay = start.toLocalDate().lengthOfMonth();
        ZonedDateTime due = start.withDayOfMonth(Math.min(payDate, maxDay));

        if (!due.isAfter(start)) {
            due = due.plusMonths(1);
            maxDay = due.toLocalDate().lengthOfMonth();
            due = due.withDayOfMonth(Math.min(payDate, maxDay));
        }

        return due.toInstant();
    }

    private String formatVnd(Long amount) {
        if (amount == null) return "0 ₫";
        return NumberFormat.getNumberInstance(new Locale("vi", "VN"))
                .format(amount) + " ₫";
    }
}