package com.isums.paymentservice.infrastructures.listeners;

import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.enums.PaymentMethod;
import com.isums.paymentservice.domains.enums.PaymentStatus;
import com.isums.paymentservice.domains.enums.ReferenceType;
import com.isums.paymentservice.domains.events.*;
import com.isums.paymentservice.infrastructures.repositories.PaymentRepository;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.isums.paymentservice.services.PaymentTokenService;
import common.kafkas.IdempotencyService;
import common.kafkas.KafkaListenerHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

@Slf4j
@Component
@RequiredArgsConstructor
public class ContractEventListener {

    private final RentalInvoiceRepository invoiceRepository;
    private final PaymentRepository paymentRepository;
    private final PaymentTokenService paymentTokenService;
    private final KafkaTemplate<String, Object> kafka;
    private final ObjectMapper objectMapper;
    private final IdempotencyService idempotencyService;
    private final KafkaListenerHelper kafkaHelper;

    @Value("${app.payment.outsystem-url:https://outsystem.isums.pro/payments}")
    private String outsystemPaymentUrl;

    private static final ZoneId VN = ZoneId.of("Asia/Ho_Chi_Minh");
    private static final DateTimeFormatter DMY = DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(VN);
    private static final DateTimeFormatter DT_FMT = DateTimeFormatter.ofPattern("HH:mm 'ngày' dd/MM/yyyy").withZone(VN);

    @KafkaListener(topics = "contract-completed-topic", groupId = "payment-group")
    public void handleContractCompleted(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            ContractCompletedEvent event = objectMapper.readValue(record.value(), ContractCompletedEvent.class);

            log.info("[Payment] ContractCompleted contractId={} tenantId={} deposit={} rent={}",
                    event.getContractId(), event.getTenantId(), event.getDepositAmount(), event.getRentAmount());

            if (invoiceRepository.existsByContractIdAndPeriodKey(event.getContractId(), "DEPOSIT")) {
                log.warn("[Payment] Already processed contractId={}, skip", event.getContractId());
                ack.acknowledge();
                return;
            }

            if (event.getSignedPdfUrl() != null
                    && event.getTenantEmail() != null
                    && !event.getTenantEmail().isBlank()) {
                Map<String, Object> params = new HashMap<>();
                params.put("contractId",
                        event.getContractId().toString().substring(0, 8).toUpperCase());
                params.put("signedPdfUrl", event.getSignedPdfUrl());
                if (event.getDepositDueAt() != null) {
                    params.put("depositDeadline", DT_FMT.format(event.getDepositDueAt()));
                }
                if (event.getDepositAmount() != null && event.getDepositAmount() > 0) {
                    params.put("depositAmount", formatVnd(event.getDepositAmount()));
                }
                kafka.send("notification-email", SendEmailEvent.builder()
                        .to(event.getTenantEmail())
                        .templateCode("CONTRACT_COMPLETED")
                        .params(params)
                        .build());
                log.info("[Payment] CONTRACT_COMPLETED email queued to={}", event.getTenantEmail());
            }

            long billable = event.getDepositAmount() != null ? event.getDepositAmount() : 0L;
            long transferred = event.getTransferredDepositAmount() != null
                    ? event.getTransferredDepositAmount() : 0L;
            long originalDeposit = event.getOriginalDepositAmount() != null
                    ? event.getOriginalDepositAmount() : (billable + transferred);
            boolean isRelocation = event.getRelocationSourceContractId() != null
                    || transferred > 0;

            if (event.getRelocationSourceContractId() != null) {
                int orphanCancelled = cancelOpenRentInvoices(event.getRelocationSourceContractId());
                if (orphanCancelled > 0) {
                    log.warn("[Payment] ContractCompleted: cancelled {} orphan rent invoices on oldContractId={} "
                                    + "(safety net — contract.replaced consumer may have skipped)",
                            orphanCancelled, event.getRelocationSourceContractId());
                }
            }

            if (originalDeposit <= 0 && !isRelocation) {
                RentalInvoice monthlyInvoice = createFirstRentInvoice(
                        event.getContractId(),
                        event.getTenantId(),
                        event.getHouseId(),
                        event.getRentAmount(),
                        event.getPayDate(),
                        event.getStartAt(),
                        event.getTenantEmail(),
                        event.getIsNewAccount());
                if (monthlyInvoice != null) {
                    if (event.getTenantEmail() != null && !event.getTenantEmail().isBlank()) {
                        sendPaymentEmail(List.of(monthlyInvoice), event);
                    }
                    publishTenantActivation(
                            event.getContractId(),
                            event.getTenantId(),
                            event.getHouseId(),
                            event.getStartAt(),
                            monthlyInvoice,
                            event.getTenantEmail(),
                            event.getIsNewAccount());
                }
                ack.acknowledge();
                return;
            }

            List<RentalInvoice> invoices = new ArrayList<>();

            Instant depositDueDate = event.getDepositDueAt() != null
                    ? event.getDepositDueAt()
                    : Instant.now().plus(3, ChronoUnit.DAYS);

            boolean fullCoverage = isRelocation && billable == 0 && originalDeposit > 0;
            InvoiceStatus depositStatus = fullCoverage ? InvoiceStatus.PAID : InvoiceStatus.UNPAID;
            Long depositInvoiceTotal = fullCoverage
                    ? originalDeposit
                    : (billable > 0 ? billable : originalDeposit);
            Long depositInvoiceBase = fullCoverage
                    ? originalDeposit
                    : (billable > 0 ? billable : originalDeposit);

            RentalInvoice depositInvoice = RentalInvoice.builder()
                    .contractId(event.getContractId())
                    .tenantId(event.getTenantId())
                    .houseId(event.getHouseId())
                    .type(InvoiceType.DEPOSIT)
                    .periodKey("DEPOSIT")
                    .relocationSourceContractId(event.getRelocationSourceContractId())
                    .baseAmount(depositInvoiceBase)
                    .serviceAmount(0L)
                    .penaltyAmount(0L)
                    .totalAmount(depositInvoiceTotal)
                    .status(depositStatus)
                    .dueDate(depositDueDate)
                    .paidAt(fullCoverage ? Instant.now() : null)
                    .rentAmount(event.getRentAmount())
                    .payDate(event.getPayDate())
                    .contractStartAt(event.getStartAt())
                    .tenantEmail(event.getTenantEmail())
                    .isNewAccount(event.getIsNewAccount())
                    .build();
            invoices.add(depositInvoice);

            invoiceRepository.saveAll(invoices);
            log.info("[Payment] Created {} invoices contractId={} relocation={} status={} billable={} transferred={}",
                    invoices.size(), event.getContractId(), isRelocation, depositStatus, billable, transferred);

            if (fullCoverage) {
                kafka.send("deposit-paid-topic", DepositPaidEvent.builder()
                        .invoiceId(depositInvoice.getId())
                        .contractId(event.getContractId())
                        .tenantId(event.getTenantId())
                        .houseId(event.getHouseId())
                        .amount(originalDeposit)
                        .invoiceType(InvoiceType.DEPOSIT)
                        .txnNo("TRANSFER-" + event.getContractId().toString().substring(0, 8))
                        .paidAt(Instant.now())
                        .relocationSourceContractId(event.getRelocationSourceContractId())
                        .build());
                log.info("[Payment] Full-coverage deposit auto-paid contractId={} amount={}",
                        event.getContractId(), originalDeposit);
                if (event.getTenantEmail() != null && !event.getTenantEmail().isBlank()) {
                    kafka.send("notification-email", SendEmailEvent.builder()
                            .to(event.getTenantEmail())
                            .templateCode("CONTRACT_DEPOSIT_TRANSFERRED")
                            .params(Map.of(
                                    "contractId", event.getContractId().toString().substring(0, 8).toUpperCase(),
                                    "depositAmount", formatVnd(originalDeposit),
                                    "transferredAmount", formatVnd(transferred)
                            ))
                            .build());
                }
                ack.acknowledge();
                return;
            }

            if (isRelocation && billable > 0
                    && event.getTenantEmail() != null && !event.getTenantEmail().isBlank()) {
                String topupToken = paymentTokenService.generateToken(depositInvoice.getId(), event.getTenantId());
                String topupPaymentUrl = outsystemPaymentUrl
                        + "?invoiceId=" + depositInvoice.getId()
                        + "&token=" + topupToken;
                String appDeepLink = "isumstenant://contracts/" + event.getContractId();

                Map<String, Object> params = new HashMap<>();
                params.put("contractId", event.getContractId().toString().substring(0, 8).toUpperCase());
                params.put("originalAmount", formatVnd(originalDeposit));
                params.put("transferredAmount", formatVnd(transferred));
                params.put("additionalAmount", formatVnd(billable));
                params.put("dueDate", DMY.format(depositDueDate));
                params.put("paymentUrl", topupPaymentUrl);
                params.put("appDeepLink", appDeepLink);

                kafka.send("notification-email", SendEmailEvent.builder()
                        .to(event.getTenantEmail())
                        .templateCode("CONTRACT_DEPOSIT_INCREASE")
                        .params(params)
                        .build());
                log.info("[Payment] Deposit increase email sent contractId={} additional={} paymentUrl={}",
                        event.getContractId(), billable, topupPaymentUrl);
                ack.acknowledge();
                return;
            }

            if (!invoices.isEmpty()
                    && event.getTenantEmail() != null
                    && !event.getTenantEmail().isBlank()) {
                sendPaymentEmail(invoices, event);
            } else if (invoices.isEmpty()) {
                log.warn("[Payment] No invoices created contractId={}", event.getContractId());
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

    @KafkaListener(topics = "contract.cash-deposit-confirmed", groupId = "payment-group")
    @Transactional
    public void handleContractCashDepositConfirmed(
            ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            ContractCashDepositConfirmedEvent event = objectMapper.readValue(
                    record.value(), ContractCashDepositConfirmedEvent.class);

            log.info("[Payment] CashDepositConfirmed contractId={} receipt={} amount={}",
                    event.contractId(), event.receiptNumber(), event.amount());

            if (paymentRepository.findByGatewayTxnId(event.receiptNumber()).isPresent()) {
                log.warn("[Payment] CashDeposit already processed receipt={}, skip",
                        event.receiptNumber());
                ack.acknowledge();
                return;
            }

            RentalInvoice depositInvoice = invoiceRepository
                    .findByContractIdAndType(event.contractId(), InvoiceType.DEPOSIT)
                    .orElse(null);

            if (depositInvoice == null) {
                log.warn("[Payment] No DEPOSIT invoice for contractId={}, skip cash confirm",
                        event.contractId());
                ack.acknowledge();
                return;
            }

            if (depositInvoice.getStatus() != InvoiceStatus.PAID) {
                depositInvoice.setStatus(InvoiceStatus.PAID);
                depositInvoice.setPaidAt(event.paidAt());
                invoiceRepository.save(depositInvoice);
            }

            paymentRepository.save(Payment.builder()
                    .referenceId(depositInvoice.getId())
                    .referenceType(ReferenceType.INVOICE)
                    .tenantId(event.tenantId())
                    .payerUserId(event.tenantId())
                    .amount(event.amount())
                    .method(PaymentMethod.CASH)
                    .status(PaymentStatus.SUCCESS)
                    .gatewayTxnId(event.receiptNumber())
                    .gatewayResponse("{\"method\":\"CASH\",\"confirmedBy\":\""
                            + event.confirmedByUserId() + "\"}")
                    .note(event.note() != null && !event.note().isBlank()
                            ? event.note()
                            : "Cash deposit confirmed by manager")
                    .paidAt(event.paidAt())
                    .build());

            kafka.send("deposit-paid-topic", DepositPaidEvent.builder()
                    .invoiceId(depositInvoice.getId())
                    .contractId(event.contractId())
                    .tenantId(event.tenantId())
                    .houseId(event.houseId())
                    .amount(event.amount())
                    .invoiceType(InvoiceType.DEPOSIT)
                    .txnNo(event.receiptNumber())
                    .paidAt(event.paidAt())
                    .build());

            log.info("[Payment] CashDeposit recorded receipt={} invoiceId={}",
                    event.receiptNumber(), depositInvoice.getId());
            ack.acknowledge();

        } catch (JacksonException e) {
            log.error("[Payment] Deserialize cash-deposit-confirmed failed: {}", e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Payment] handleContractCashDepositConfirmed failed: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = "contract.deposit-expired", groupId = "payment-group")
    @Transactional
    public void handleDepositExpired(
            ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            ContractDepositExpiredEvent event = objectMapper.readValue(
                    record.value(), ContractDepositExpiredEvent.class);

            log.info("[Payment] DepositExpired contractId={} contractNo={}",
                    event.contractId(), event.contractNo());

            RentalInvoice depositInvoice = invoiceRepository
                    .findByContractIdAndType(event.contractId(), InvoiceType.DEPOSIT)
                    .orElse(null);

            if (depositInvoice != null
                    && (depositInvoice.getStatus() == InvoiceStatus.UNPAID
                        || depositInvoice.getStatus() == InvoiceStatus.OVERDUE)) {
                depositInvoice.setStatus(InvoiceStatus.CANCELLED);
                invoiceRepository.save(depositInvoice);
                log.info("[Payment] DEPOSIT invoice cancelled invoiceId={}",
                        depositInvoice.getId());
            }

            if (event.tenantEmail() != null && !event.tenantEmail().isBlank()) {
                kafka.send("notification-email", SendEmailEvent.builder()
                        .to(event.tenantEmail())
                        .templateCode("CONTRACT_DEPOSIT_EXPIRED_TENANT_INVOICE")
                        .params(Map.of(
                                "tenantName", event.tenantName() != null ? event.tenantName() : "",
                                "contractNo", event.contractNo() != null ? event.contractNo() : "",
                                "depositAmount", formatVnd(event.depositAmount()),
                                "deadline", event.depositDueAt() != null
                                        ? DT_FMT.format(event.depositDueAt()) : ""
                        ))
                        .build());
            }

            ack.acknowledge();
        } catch (JacksonException e) {
            log.error("[Payment] Deserialize deposit-expired failed: {}", e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Payment] handleDepositExpired failed: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = "deposit-paid-topic", groupId = "payment-group")
    public void handleDepositPaid(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String messageId = kafkaHelper.extractMessageId(record);
        kafkaHelper.setupMDC(record, messageId);
        try {
            if (idempotencyService.isDuplicate(messageId)) {
                log.warn("[Payment] Duplicate deposit-paid skipped messageId={}", messageId);
                ack.acknowledge();
                return;
            }

            DepositPaidEvent event = objectMapper.readValue(record.value(), DepositPaidEvent.class);

            log.info("[Payment] DepositPaid contractId={} tenantId={}",
                    event.contractId(), event.tenantId());

            RentalInvoice depositInvoice = invoiceRepository
                    .findByContractIdAndType(event.contractId(), InvoiceType.DEPOSIT)
                    .orElse(null);

            if (depositInvoice == null || depositInvoice.getRentAmount() == null) {
                log.warn("[Payment] No deposit context contractId={}, skip", event.contractId());
                idempotencyService.markProcessed(messageId);
                ack.acknowledge();
                return;
            }

            UUID oldContractId = event.relocationSourceContractId();
            long firstMonthCredit = resolveFirstMonthCredit(oldContractId);
            RentalInvoice monthlyInvoice = createFirstRentInvoice(
                    event.contractId(),
                    event.tenantId(),
                    event.houseId(),
                    depositInvoice.getRentAmount(),
                    depositInvoice.getPayDate(),
                    depositInvoice.getContractStartAt(),
                    depositInvoice.getTenantEmail(),
                    depositInvoice.getIsNewAccount(),
                    firstMonthCredit,
                    oldContractId);

            if (monthlyInvoice == null) {
                Instant firstRentDue = calcFirstRentDue(
                        depositInvoice.getContractStartAt(), depositInvoice.getPayDate());
                String periodKey = "RENT_" + firstRentDue.atZone(VN)
                        .format(DateTimeFormatter.ofPattern("yyyyMM"));
                monthlyInvoice = invoiceRepository
                        .findByContractIdAndPeriodKey(event.contractId(), periodKey)
                        .orElse(null);
                if (monthlyInvoice == null) {
                    log.error("[Payment] handleDepositPaid: MONTHLY_RENT lookup failed contractId={} periodKey={} — activation chain broken",
                            event.contractId(), periodKey);
                    idempotencyService.markProcessed(messageId);
                    ack.acknowledge();
                    return;
                }
                log.info("[Payment] handleDepositPaid: re-emitting enriched event for existing MONTHLY_RENT contractId={} invoiceId={}",
                        event.contractId(), monthlyInvoice.getId());
            }

            publishTenantActivation(
                    event.contractId(),
                    event.tenantId(),
                    event.houseId(),
                    depositInvoice.getContractStartAt(),
                    monthlyInvoice,
                    depositInvoice.getTenantEmail(),
                    depositInvoice.getIsNewAccount());

            log.info("[Payment] MONTHLY_RENT enriched event sent contractId={}",
                    event.contractId());
            idempotencyService.markProcessed(messageId);
            ack.acknowledge();

        } catch (JacksonException e) {
            log.error("[Payment] Deserialize deposit-paid failed messageId={}: {}", messageId, e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Payment] handleDepositPaid failed messageId={}, will retry: {}", messageId, e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            kafkaHelper.clearMDC();
        }
    }

    @KafkaListener(topics = "contract.deposit-invoice-cancel-requested", groupId = "payment-group")
    @Transactional
    public void handleCancelDepositInvoiceRequested(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            CancelDepositInvoiceRequestedEvent event = objectMapper.readValue(
                    record.value(), CancelDepositInvoiceRequestedEvent.class);

            RentalInvoice depositInvoice = invoiceRepository
                    .findByContractIdAndType(event.getContractId(), InvoiceType.DEPOSIT)
                    .orElse(null);

            if (depositInvoice == null) {
                log.info("[Payment] No deposit invoice to cancel contractId={}", event.getContractId());
                ack.acknowledge();
                return;
            }

            if (depositInvoice.getStatus() != InvoiceStatus.UNPAID
                    && depositInvoice.getStatus() != InvoiceStatus.OVERDUE) {
                log.warn("[Payment] Deposit invoice not in cancellable state contractId={} invoiceId={} status={}",
                        event.getContractId(), depositInvoice.getId(), depositInvoice.getStatus());
                ack.acknowledge();
                return;
            }

            depositInvoice.setStatus(InvoiceStatus.CANCELLED);
            invoiceRepository.save(depositInvoice);

            log.info("[Payment] Deposit invoice cancelled contractId={} invoiceId={} reason={}",
                    event.getContractId(), depositInvoice.getId(), event.getReason());
            ack.acknowledge();
        } catch (JacksonException e) {
            log.error("[Payment] Deserialize cancel deposit failed: {}", e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Payment] handleCancelDepositInvoiceRequested failed: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private RentalInvoice createFirstRentInvoice(
            UUID contractId,
            UUID tenantId,
            UUID houseId,
            Long rentAmount,
            Integer payDate,
            Instant contractStartAt,
            String tenantEmail,
            Boolean isNewAccount) {
        return createFirstRentInvoice(contractId, tenantId, houseId, rentAmount,
                payDate, contractStartAt, tenantEmail, isNewAccount, 0L, null);
    }

    private RentalInvoice createFirstRentInvoice(
            UUID contractId,
            UUID tenantId,
            UUID houseId,
            Long rentAmount,
            Integer payDate,
            Instant contractStartAt,
            String tenantEmail,
            Boolean isNewAccount,
            long firstMonthCredit,
            UUID oldContractId) {
        Instant firstRentDue = calcFirstRentDue(contractStartAt, payDate);
        String periodKey = "RENT_" + firstRentDue.atZone(VN)
                .format(DateTimeFormatter.ofPattern("yyyyMM"));

        if (invoiceRepository.existsByContractIdAndPeriodKey(contractId, periodKey)) {
            log.warn("[Payment] MONTHLY_RENT already exists contractId={}, skip", contractId);
            return null;
        }

        long rent = rentAmount != null ? rentAmount : 0L;
        long credit = Math.max(0L, firstMonthCredit);
        long applied = Math.min(credit, rent);
        long billable = Math.max(0L, rent - applied);
        boolean fullyCredited = rent > 0 && billable == 0 && applied > 0;
        InvoiceStatus status = fullyCredited ? InvoiceStatus.PAID : InvoiceStatus.UNPAID;

        RentalInvoice monthlyInvoice = RentalInvoice.builder()
                .contractId(contractId)
                .tenantId(tenantId)
                .houseId(houseId)
                .type(InvoiceType.MONTHLY_RENT)
                .periodKey(periodKey)
                .baseAmount(billable > 0 ? billable : rent)
                .serviceAmount(0L)
                .penaltyAmount(0L)
                .totalAmount(rent)
                .status(status)
                .paidAt(fullyCredited ? Instant.now() : null)
                .dueDate(firstRentDue)
                .rentAmount(rent)
                .payDate(payDate)
                .contractStartAt(contractStartAt)
                .tenantEmail(tenantEmail)
                .isNewAccount(isNewAccount)
                .build();
        invoiceRepository.save(monthlyInvoice);

        if (credit > 0 && tenantEmail != null && !tenantEmail.isBlank()) {
            sendFirstMonthCreditEmail(tenantEmail, contractId, rent, credit, billable);
        }

        if (credit > rent && rent > 0 && oldContractId != null) {
            long excess = credit - rent;
            log.info("[Payment] First-month credit excess={} oldContract={} newContract={} → manual refund required",
                    excess, oldContractId, contractId);
            if (tenantEmail != null && !tenantEmail.isBlank()) {
                kafka.send("notification-email", SendEmailEvent.builder()
                        .to(tenantEmail)
                        .templateCode("CONTRACT_FIRST_MONTH_REFUND_DUE")
                        .params(Map.of(
                                "contractId", contractId.toString().substring(0, 8).toUpperCase(),
                                "oldContractId", oldContractId.toString().substring(0, 8).toUpperCase(),
                                "excessAmount", formatVnd(excess)
                        ))
                        .build());
            }
        }

        return monthlyInvoice;
    }

    private void sendFirstMonthCreditEmail(String tenantEmail, UUID contractId,
                                           long rent, long credit, long billable) {
        boolean fullCover = billable == 0 && credit >= rent;
        String templateCode = fullCover
                ? "CONTRACT_FIRST_MONTH_COVERED"
                : "CONTRACT_FIRST_MONTH_PARTIAL_CREDIT";
        Map<String, Object> params = new HashMap<>();
        params.put("contractId", contractId.toString().substring(0, 8).toUpperCase());
        params.put("rentAmount", formatVnd(rent));
        params.put("creditAmount", formatVnd(Math.min(credit, rent)));
        params.put("billableAmount", formatVnd(billable));
        kafka.send("notification-email", SendEmailEvent.builder()
                .to(tenantEmail)
                .templateCode(templateCode)
                .params(params)
                .build());
    }

    private long resolveFirstMonthCredit(UUID oldContractId) {
        if (oldContractId == null) return 0L;
        return invoiceRepository
                .findFirstByContractIdAndTypeOrderByDueDateAsc(oldContractId, InvoiceType.MONTHLY_RENT)
                .filter(inv -> inv.getStatus() == InvoiceStatus.PAID)
                .map(RentalInvoice::getTotalAmount)
                .map(amount -> amount != null ? amount : 0L)
                .orElse(0L);
    }

    private void publishTenantActivation(
            UUID contractId,
            UUID tenantId,
            UUID houseId,
            Instant handoverDate,
            RentalInvoice monthlyInvoice,
            String tenantEmail,
            Boolean isNewAccount) {
        String token = paymentTokenService.generateToken(monthlyInvoice.getId(), tenantId);
        String paymentUrl = outsystemPaymentUrl
                + "?invoiceId=" + monthlyInvoice.getId()
                + "&token=" + token;

        kafka.send("deposit-paid-enriched-topic", DepositPaidEvent.builder()
                .contractId(contractId)
                .tenantId(tenantId)
                .houseId(houseId)
                .isNewAccount(isNewAccount)
                .tenantEmail(tenantEmail)
                .firstRentPaymentUrl(paymentUrl)
                .firstRentAmount(monthlyInvoice.getTotalAmount())
                .firstRentDueDate(monthlyInvoice.getDueDate())
                .build());

        kafka.send("map-user-to-house-topic", MapUserToHouseEvent.builder()
                .userId(tenantId)
                .houseId(houseId)
                .handoverDate(handoverDate)
                .build());
    }

    private static final Map<String, Map<InvoiceType, String>> INVOICE_TYPE_LABELS = Map.of(
            "vi", Map.of(
                    InvoiceType.DEPOSIT, "Tiền cọc",
                    InvoiceType.MONTHLY_RENT, "Tiền thuê tháng đầu"),
            "en", Map.of(
                    InvoiceType.DEPOSIT, "Deposit",
                    InvoiceType.MONTHLY_RENT, "First-month rent"),
            "ja", Map.of(
                    InvoiceType.DEPOSIT, "敷金",
                    InvoiceType.MONTHLY_RENT, "初月家賃")
    );

    private static String invoiceTypeLabel(InvoiceType type, String locale) {
        Map<InvoiceType, String> map = INVOICE_TYPE_LABELS
                .getOrDefault(locale != null ? locale : "vi", INVOICE_TYPE_LABELS.get("vi"));
        String label = map.get(type);
        return label != null ? label : type.name();
    }

    private void sendPaymentEmail(List<RentalInvoice> invoices, ContractCompletedEvent event) {
        for (RentalInvoice invoice : invoices) {
            try {
                String token = paymentTokenService.generateToken(invoice.getId(), event.getTenantId());
                String paymentUrl = outsystemPaymentUrl + "?invoiceId=" + invoice.getId() + "&token=" + token;

                Map<String, Object> params = new HashMap<>();
                params.put("invoiceType", invoiceTypeLabel(invoice.getType(), "vi"));
                params.put("invoiceTypeVi", invoiceTypeLabel(invoice.getType(), "vi"));
                params.put("invoiceTypeEn", invoiceTypeLabel(invoice.getType(), "en"));
                params.put("invoiceTypeJa", invoiceTypeLabel(invoice.getType(), "ja"));
                params.put("invoiceTypeCode", invoice.getType().name());
                params.put("amount", formatVnd(invoice.getTotalAmount()));
                params.put("dueDate", DMY.format(invoice.getDueDate()));
                params.put("paymentUrl", paymentUrl);
                params.put("expiresIn", "7 days");

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

    @KafkaListener(topics = "contract.replaced", groupId = "payment-group")
    @Transactional
    public void handleContractReplaced(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            ContractReplacedEvent event = objectMapper.readValue(record.value(), ContractReplacedEvent.class);

            int cancelled = cancelOpenRentInvoices(event.getOldContractId());
            int depositClosed = closeOldDeposit(event);

            log.info("[Payment] ContractReplaced oldContractId={} newContractId={} rentCancelled={} depositClosed={} handling={}",
                    event.getOldContractId(), event.getNewContractId(),
                    cancelled, depositClosed, event.getDepositHandling());
            ack.acknowledge();
        } catch (JacksonException e) {
            log.error("[Payment] Deserialize contract.replaced failed: {}", e.getMessage());
            ack.acknowledge();
        } catch (Exception e) {
            log.error("[Payment] handleContractReplaced failed, will retry: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private int cancelOpenRentInvoices(UUID oldContractId) {
        int cancelled = 0;
        for (InvoiceStatus open : List.of(InvoiceStatus.UNPAID, InvoiceStatus.OVERDUE)) {
            for (RentalInvoice inv : invoiceRepository.findByContractIdAndStatus(oldContractId, open)) {
                if (inv.getType() == InvoiceType.DEPOSIT) {
                    continue;
                }
                inv.setStatus(InvoiceStatus.CANCELLED);
                invoiceRepository.save(inv);
                cancelled++;
            }
        }
        return cancelled;
    }

    private int closeOldDeposit(ContractReplacedEvent event) {
        InvoiceStatus terminal = resolveDepositTerminalStatus(event.getDepositHandling());
        if (terminal == null) return 0;

        RentalInvoice oldDeposit = invoiceRepository
                .findByContractIdAndType(event.getOldContractId(), InvoiceType.DEPOSIT)
                .orElse(null);
        if (oldDeposit == null) return 0;
        if (oldDeposit.getStatus() == terminal) return 0;
        if (oldDeposit.getStatus() != InvoiceStatus.PAID) return 0;

        oldDeposit.setStatus(terminal);
        invoiceRepository.save(oldDeposit);

        if (terminal == InvoiceStatus.TRANSFERRED) {
            long oldDepositAmount = oldDeposit.getTotalAmount() != null ? oldDeposit.getTotalAmount() : 0L;
            long transferred = event.getTransferredDepositAmount() != null
                    ? event.getTransferredDepositAmount() : 0L;
            long refund = Math.max(0L, oldDepositAmount - transferred);
            if (refund > 0
                    && oldDeposit.getTenantEmail() != null
                    && !oldDeposit.getTenantEmail().isBlank()
                    && event.getNewContractId() != null) {
                kafka.send("notification-email", SendEmailEvent.builder()
                        .to(oldDeposit.getTenantEmail())
                        .templateCode("CONTRACT_DEPOSIT_REFUND")
                        .params(Map.of(
                                "contractId", event.getNewContractId().toString().substring(0, 8).toUpperCase(),
                                "originalAmount", formatVnd(oldDepositAmount),
                                "transferredAmount", formatVnd(transferred),
                                "refundAmount", formatVnd(refund),
                                "refundMethod", "Tài khoản VNPay đã đăng ký"
                        ))
                        .build());
                log.info("[Payment] Deposit refund email sent oldContractId={} refund={}",
                        event.getOldContractId(), refund);
            }
        }
        return 1;
    }

    private InvoiceStatus resolveDepositTerminalStatus(String depositHandling) {
        if (depositHandling == null) return null;
        return switch (depositHandling) {
            case "TRANSFER_TO_REPLACEMENT", "PARTIAL_TRANSFER" -> InvoiceStatus.TRANSFERRED;
            case "FORFEIT" -> InvoiceStatus.FORFEITED;
            case "REFUND_TO_TENANT" -> InvoiceStatus.REFUNDED;
            default -> null;
        };
    }

    @KafkaListener(topics = "contract.deposit-refund.confirmed", groupId = "payment-group")
    @Transactional
    public void handleDepositRefundConfirmed(
            ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            DepositRefundConfirmedEvent event = objectMapper.readValue(record.value(), DepositRefundConfirmedEvent.class);

            String periodKey = "DEPOSIT_REFUND";
            if (invoiceRepository.existsByContractIdAndPeriodKey(event.getContractId(), periodKey)) {
                log.warn("[Payment] DEPOSIT_REFUND already exists contractId={}, skip",
                        event.getContractId());
                ack.acknowledge();
                return;
            }

            RentalInvoice refundInvoice = RentalInvoice.builder()
                    .contractId(event.getContractId())
                    .tenantId(event.getTenantId())
                    .houseId(event.getHouseId())
                    .type(InvoiceType.DEPOSIT_REFUND)
                    .periodKey(periodKey)
                    .baseAmount(event.getRefundAmount())
                    .serviceAmount(0L)
                    .penaltyAmount(0L)
                    .totalAmount(event.getRefundAmount())
                    .status(InvoiceStatus.UNPAID)
                    .tenantEmail(event.getTenantEmail())
                    .dueDate(Instant.now().plus(7, ChronoUnit.DAYS))
                    .build();

            invoiceRepository.save(refundInvoice);

            kafka.send("notification-email", SendEmailEvent.builder()
                    .to(event.getTenantEmail())
                    .templateCode("deposit_refund_notify")
                    .params(Map.of(
                            "refundAmount", formatVnd(event.getRefundAmount()),
                            "note", event.getNote() != null
                                    ? event.getNote() : "No note",
                            "dueDate", DMY.format(refundInvoice.getDueDate())
                    ))
                    .build());

            log.info("[Payment] DEPOSIT_REFUND invoice created contractId={} amount={}",
                    event.getContractId(), event.getRefundAmount());
            ack.acknowledge();

        } catch (Exception e) {
            log.error("[Payment] handleDepositRefundConfirmed failed: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private String formatVnd(Long amount) {
        if (amount == null) return "0 ₫";
        return NumberFormat.getNumberInstance(Locale.of("vi", "VN"))
                .format(amount) + " ₫";
    }
}
