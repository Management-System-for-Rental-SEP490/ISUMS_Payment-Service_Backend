package com.isums.paymentservice.schedulers;

import com.isums.paymentservice.domains.entities.LatePaymentActionLog;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.LatePaymentAction;
import com.isums.paymentservice.domains.events.AppAccessChangedEvent;
import com.isums.paymentservice.domains.events.PowerCutRequestEvent;
import com.isums.paymentservice.domains.events.SendEmailEvent;
import com.isums.paymentservice.domains.events.TerminationRequestedEvent;
import com.isums.paymentservice.infrastructures.grpcs.UserGrpcService;
import com.isums.paymentservice.infrastructures.repositories.LatePaymentActionLogRepository;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.isums.userservice.grpc.UserResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class LatePaymentScheduler {

    private final RentalInvoiceRepository invoiceRepo;
    private final LatePaymentActionLogRepository actionLogRepo;
    private final KafkaTemplate<String, Object> kafka;
    private final UserGrpcService userGrpcService;

    private static final int GRACE_DAYS = 3;
    private static final ZoneId VN = ZoneId.of("Asia/Ho_Chi_Minh");

    @Scheduled(cron = "30 4 15 * * *", zone = "Asia/Ho_Chi_Minh")
    @Transactional
    public void processLatePayments() {
        LocalDate today = LocalDate.now(VN);
        Instant now = Instant.now();

        List<RentalInvoice> overdueInvoices = invoiceRepo.findOverdueMonthlyRentInvoices(now);

        log.info("[LatePayment] Processing {} overdue invoices", overdueInvoices.size());

        for (RentalInvoice invoice : overdueInvoices) {
            try {
                processInvoice(invoice, today);
            } catch (Exception e) {
                log.error("[LatePayment] Failed invoiceId={}: {}",
                        invoice.getId(), e.getMessage(), e);
            }
        }
    }

    private void processInvoice(RentalInvoice invoice, LocalDate today) {
        LocalDate dueDate = invoice.getDueDate().atZone(VN).toLocalDate();
        long daysLate = ChronoUnit.DAYS.between(dueDate, today);

        // Cập nhật overdueDays
        invoice.setOverdueDays((int) daysLate);
        invoiceRepo.save(invoice);

        // Ngày +0: nhắc lần 1
        triggerOnce(invoice, LatePaymentAction.NOTIFY_DAY_0, daysLate >= 0, () ->
                publishNotification(invoice, "late_payment_reminder_day0", daysLate));

        // Ngày +1: nhắc lần 2
        triggerOnce(invoice, LatePaymentAction.NOTIFY_DAY_1, daysLate >= 1, () ->
                publishNotification(invoice, "late_payment_reminder_day1", daysLate));

        // Ngày +2: nhắc lần 3
        triggerOnce(invoice, LatePaymentAction.NOTIFY_DAY_2, daysLate >= 2, () ->
                publishNotification(invoice, "late_payment_reminder_day2", daysLate));

        // Ngày +3: phạt tier 1 (5%)
        triggerOnce(invoice, LatePaymentAction.PENALTY_TIER_1, daysLate >= GRACE_DAYS, () ->
                applyPenalty(invoice, 1));

        // Ngày +7: email cảnh báo chính thức + app read-only
        triggerOnce(invoice, LatePaymentAction.FORMAL_WARNING, daysLate >= 7, () ->
                publishFormalWarning(invoice));

        triggerOnce(invoice, LatePaymentAction.APP_RESTRICTED, daysLate >= 7, () ->
                publishAppRestriction(invoice, true));

        // Ngày +8: phạt tier 2 (10%)
        triggerOnce(invoice, LatePaymentAction.PENALTY_TIER_2, daysLate >= 8, () ->
                applyPenalty(invoice, 2));

        // Ngày +14: phạt tier 3 (15%) + gửi yêu cầu cắt điện lên chủ nhà
        triggerOnce(invoice, LatePaymentAction.PENALTY_TIER_3, daysLate >= 14, () ->
                applyPenalty(invoice, 3));

        triggerOnce(invoice, LatePaymentAction.POWER_CUT_REQUEST, daysLate >= 14, () ->
                publishPowerCutRequest(invoice));

        // Ngày +30: bắt đầu quy trình chấm dứt hợp đồng
        triggerOnce(invoice, LatePaymentAction.TERMINATION_INITIATED, daysLate >= 30, () ->
                publishTerminationRequest(invoice));
    }

    private void triggerOnce(RentalInvoice invoice,
                             LatePaymentAction action,
                             boolean condition,
                             Runnable task) {
        if (!condition) return;
        if (actionLogRepo.existsByInvoiceIdAndActionType(invoice.getId(), action)) return;

        task.run();

        actionLogRepo.save(LatePaymentActionLog.builder()
                .invoiceId(invoice.getId())
                .contractId(invoice.getContractId())
                .tenantId(invoice.getTenantId())
                .actionType(action)
                .build());

        log.info("[LatePayment] Action={} invoiceId={} daysLate={}",
                action, invoice.getId(), invoice.getOverdueDays());
    }

    private void applyPenalty(RentalInvoice invoice, int tier) {
        long baseAmount = invoice.getBaseAmount();
        long penaltyAmount = baseAmount * (tier * 5L) / 100;
        String tenantEmail = userGrpcService.getTenantEmail(invoice.getTenantId());

        invoice.setPenaltyAmount(penaltyAmount);
        invoice.setTotalAmount(baseAmount + penaltyAmount);
        invoiceRepo.save(invoice);

        kafka.send("notification-email",
                SendEmailEvent.builder()
                        .to(tenantEmail)
                        .templateCode("late_payment_penalty_applied")
                        .params(Map.of(
                                "penaltyPercent", String.valueOf(tier * 5),
                                "penaltyAmount",  formatVnd(penaltyAmount),
                                "totalAmount",    formatVnd(invoice.getTotalAmount()),
                                "daysLate",       String.valueOf(invoice.getOverdueDays())
                        ))
                        .build());
    }

    private void publishNotification(RentalInvoice invoice, String templateCode, long daysLate) {
        String tenantEmail = userGrpcService.getTenantEmail(invoice.getTenantId());
        kafka.send("notification-email",
                SendEmailEvent.builder()
                        .to(tenantEmail)
                        .templateCode(templateCode)
                        .params(Map.of(
                                "totalAmount", formatVnd(invoice.getTotalAmount()),
                                "dueDate",     DMY.format(invoice.getDueDate()),
                                "daysLate",    String.valueOf(daysLate)
                        ))
                        .build());
    }

    private void publishFormalWarning(RentalInvoice invoice) {
        String tenantEmail = userGrpcService.getTenantEmail(invoice.getTenantId());
        kafka.send("notification-email",
                SendEmailEvent.builder()
                        .to(tenantEmail)
                        .templateCode("late_payment_formal_warning")
                        .params(Map.of(
                                "totalAmount", formatVnd(invoice.getTotalAmount()),
                                "dueDate",     DMY.format(invoice.getDueDate()),
                                "daysLate",    String.valueOf(invoice.getOverdueDays())
                        ))
                        .build());
    }

    private void publishAppRestriction(RentalInvoice invoice, boolean restrict) {
        kafka.send("payment.app-access-changed",
                invoice.getHouseId().toString(),
                AppAccessChangedEvent.builder()
                        .tenantId(invoice.getTenantId())
                        .houseId(invoice.getHouseId())
                        .contractId(invoice.getContractId())
                        .restricted(restrict)
                        .reason("LATE_PAYMENT")
                        .messageId(UUID.randomUUID().toString())
                        .build());
    }

    private void publishPowerCutRequest(RentalInvoice invoice) {
        kafka.send("payment.power-cut-requested",
                invoice.getContractId().toString(),
                PowerCutRequestEvent.builder()
                        .invoiceId(invoice.getId())
                        .contractId(invoice.getContractId())
                        .houseId(invoice.getHouseId())
                        .tenantId(invoice.getTenantId())
                        .daysLate(invoice.getOverdueDays())
                        .totalAmount(invoice.getTotalAmount())
                        .messageId(UUID.randomUUID().toString())
                        .build());
    }

    private void publishTerminationRequest(RentalInvoice invoice) {
        kafka.send("payment.termination-requested",
                invoice.getContractId().toString(),
                TerminationRequestedEvent.builder()
                        .contractId(invoice.getContractId())
                        .houseId(invoice.getHouseId())
                        .tenantId(invoice.getTenantId())
                        .invoiceId(invoice.getId())
                        .reason("OVERDUE_PAYMENT_30_DAYS")
                        .messageId(UUID.randomUUID().toString())
                        .build());
    }

    private String formatVnd(Long amount) {
        return NumberFormat.getNumberInstance(Locale.of("vi", "VN")).format(amount) + " ₫";
    }

    private static final DateTimeFormatter DMY =
            DateTimeFormatter.ofPattern("dd/MM/yyyy").withZone(ZoneId.of("Asia/Ho_Chi_Minh"));
}
