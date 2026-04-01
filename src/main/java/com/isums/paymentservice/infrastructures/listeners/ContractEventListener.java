package com.isums.paymentservice.infrastructures.listeners;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.events.ContractCompletedEvent;
import com.isums.paymentservice.domains.events.DepositPaidEvent;
import com.isums.paymentservice.domains.events.MapUserToHouseEvent;
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
                        .rentAmount(event.getRentAmount())
                        .payDate(event.getPayDate())
                        .contractStartAt(event.getStartAt())
                        .tenantEmail(event.getTenantEmail())
                        .isNewAccount(event.getIsNewAccount())
                        .build());
            }

            invoiceRepository.saveAll(invoices);
            log.info("[Payment] Created {} invoices contractId={}", invoices.size(), event.getContractId());

            if (event.getTenantEmail() != null && !event.getTenantEmail().isBlank()) {
                sendPaymentEmail(invoices, event);
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

    @KafkaListener(topics = "deposit-paid-topic", groupId = "payment-group")
    public void handleDepositPaid(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            DepositPaidEvent event = objectMapper.readValue(record.value(), DepositPaidEvent.class);

            // Lấy context từ DEPOSIT invoice đã lưu
            RentalInvoice depositInvoice = invoiceRepository
                    .findByContractIdAndType(event.contractId(), InvoiceType.DEPOSIT)
                    .orElse(null);

            if (depositInvoice == null || depositInvoice.getRentAmount() == null) {
                log.warn("[Payment] No deposit invoice context contractId={}, skip", event.contractId());
                ack.acknowledge();
                return;
            }

            Instant firstRentDue = calcFirstRentDue(depositInvoice.getContractStartAt(), depositInvoice.getPayDate());
            String periodKey = "RENT_" + firstRentDue.atZone(VN).format(DateTimeFormatter.ofPattern("yyyyMM"));

            if (invoiceRepository.existsByContractIdAndPeriodKey(event.contractId(), periodKey)) {
                log.warn("[Payment] MONTHLY_RENT already exists contractId={}, skip", event.contractId());
                ack.acknowledge();
                return;
            }

            RentalInvoice monthlyInvoice = RentalInvoice.builder()
                    .contractId(event.contractId())
                    .tenantId(event.tenantId())
                    .houseId(event.houseId())
                    .type(InvoiceType.MONTHLY_RENT)
                    .periodKey(periodKey)
                    .baseAmount(depositInvoice.getRentAmount())
                    .serviceAmount(0L)
                    .penaltyAmount(0L)
                    .totalAmount(depositInvoice.getRentAmount())
                    .status(InvoiceStatus.UNPAID)
                    .dueDate(firstRentDue)
                    .build();
            invoiceRepository.save(monthlyInvoice);

            String token = paymentTokenService.generateToken(monthlyInvoice.getId(), event.tenantId());
            String paymentUrl = outsystemPaymentUrl + "?invoiceId=" + monthlyInvoice.getId() + "&token=" + token;

            kafka.send("deposit-paid-topic-v2", DepositPaidEvent.builder()
                    .contractId(event.contractId())
                    .tenantId(event.tenantId())
                    .isNewAccount(depositInvoice.getIsNewAccount())
                    .firstRentPaymentUrl(paymentUrl)
                    .firstRentAmount(depositInvoice.getRentAmount())
                    .firstRentDueDate(firstRentDue)
                    .build());

            kafka.send("map-user-to-house-topic", MapUserToHouseEvent.builder()
                    .userId(event.tenantId())
                    .houseId(event.houseId())
                    .build());

            log.info("[Payment] MONTHLY_RENT created contractId={}", event.contractId());
            ack.acknowledge();

        } catch (JacksonException e) {
            log.error("[Payment] Deserialize deposit-paid failed: {}", e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Payment] handleDepositPaid failed, will retry: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void sendPaymentEmail(List<RentalInvoice> invoices, ContractCompletedEvent event) {
        for (RentalInvoice invoice : invoices) {
            try {
                String token = paymentTokenService.generateToken(invoice.getId(), event.getTenantId());
                String paymentUrl = outsystemPaymentUrl + "?invoiceId=" + invoice.getId() + "&token=" + token;

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
        return NumberFormat.getNumberInstance(Locale.of("vi", "VN"))
                .format(amount) + " ₫";
    }
}