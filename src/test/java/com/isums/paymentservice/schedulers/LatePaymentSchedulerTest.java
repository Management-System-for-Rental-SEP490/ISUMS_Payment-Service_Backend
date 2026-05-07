package com.isums.paymentservice.schedulers;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.enums.LatePaymentAction;
import com.isums.paymentservice.domains.events.AppAccessChangedEvent;
import com.isums.paymentservice.domains.events.PowerCutRequestEvent;
import com.isums.paymentservice.domains.events.SendEmailEvent;
import com.isums.paymentservice.domains.events.TerminationRequestedEvent;
import com.isums.paymentservice.infrastructures.grpcs.UserGrpcService;
import com.isums.paymentservice.infrastructures.repositories.LatePaymentActionLogRepository;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("LatePaymentScheduler")
class LatePaymentSchedulerTest {

    @Mock private RentalInvoiceRepository invoiceRepo;
    @Mock private LatePaymentActionLogRepository actionLogRepo;
    @Mock private KafkaTemplate<String, Object> kafka;
    @Mock private UserGrpcService userGrpcService;

    @InjectMocks private LatePaymentScheduler scheduler;

    private RentalInvoice overdue(int daysOverdue, long baseAmount) {
        Instant dueDate = Instant.now().atZone(ZoneId.of("Asia/Ho_Chi_Minh"))
                .minusDays(daysOverdue).toInstant();
        return RentalInvoice.builder()
                .id(UUID.randomUUID()).contractId(UUID.randomUUID())
                .tenantId(UUID.randomUUID()).houseId(UUID.randomUUID())
                .type(InvoiceType.MONTHLY_RENT).periodKey("2026-04")
                .baseAmount(baseAmount).serviceAmount(0L).penaltyAmount(0L)
                .totalAmount(baseAmount).status(InvoiceStatus.UNPAID)
                .dueDate(dueDate).build();
    }

    @BeforeEach
    void setUp() {
        lenient().when(userGrpcService.getTenantEmail(any())).thenReturn("alice@example.com");
    }

    @Nested
    @DisplayName("processLatePayments — tiered actions")
    class TieredActions {

        @Test
        @DisplayName("day 0: only NOTIFY_DAY_0 fires")
        void day0() {
            RentalInvoice inv = overdue(0, 1_000_000L);
            when(invoiceRepo.findOverdueMonthlyRentInvoices(any())).thenReturn(List.of(inv));
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), any())).thenReturn(false);

            scheduler.processLatePayments();

            verify(kafka).send(eq("notification-email"), any(SendEmailEvent.class));
            verifyActionLog(LatePaymentAction.NOTIFY_DAY_0);
            verify(actionLogRepo, never()).save(argThatAction(LatePaymentAction.PENALTY_TIER_1));
            verify(actionLogRepo, never()).save(argThatAction(LatePaymentAction.APP_RESTRICTED));
        }

        @Test
        @DisplayName("day 3: day0/1/2 notifications + PENALTY_TIER_1 (5%)")
        void day3() {
            RentalInvoice inv = overdue(3, 1_000_000L);
            when(invoiceRepo.findOverdueMonthlyRentInvoices(any())).thenReturn(List.of(inv));
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), any())).thenReturn(false);

            scheduler.processLatePayments();

            // 3 notifications + 1 penalty-email + invoice save (penalty applied)
            verify(kafka, atLeast(4)).send(eq("notification-email"), any(SendEmailEvent.class));
            assertThat(inv.getPenaltyAmount()).isEqualTo(50_000L); // 5%
            assertThat(inv.getTotalAmount()).isEqualTo(1_050_000L);
            verifyActionLog(LatePaymentAction.PENALTY_TIER_1);
            verify(actionLogRepo, never()).save(argThatAction(LatePaymentAction.APP_RESTRICTED));
        }

        @Test
        @DisplayName("day 7: FORMAL_WARNING + APP_RESTRICTED fires")
        void day7() {
            RentalInvoice inv = overdue(7, 1_000_000L);
            when(invoiceRepo.findOverdueMonthlyRentInvoices(any())).thenReturn(List.of(inv));
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), any())).thenReturn(false);

            scheduler.processLatePayments();

            verify(kafka).send(eq("payment.app-access-changed"), anyString(), any(AppAccessChangedEvent.class));
            verifyActionLog(LatePaymentAction.FORMAL_WARNING);
            verifyActionLog(LatePaymentAction.APP_RESTRICTED);
            verify(actionLogRepo, never()).save(argThatAction(LatePaymentAction.PENALTY_TIER_2));
        }

        @Test
        @DisplayName("day 8: PENALTY_TIER_2 (10%)")
        void day8() {
            RentalInvoice inv = overdue(8, 1_000_000L);
            when(invoiceRepo.findOverdueMonthlyRentInvoices(any())).thenReturn(List.of(inv));
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), any())).thenReturn(false);

            scheduler.processLatePayments();

            assertThat(inv.getPenaltyAmount()).isEqualTo(100_000L);
            verifyActionLog(LatePaymentAction.PENALTY_TIER_2);
            verify(actionLogRepo, never()).save(argThatAction(LatePaymentAction.PENALTY_TIER_3));
        }

        @Test
        @DisplayName("day 14: PENALTY_TIER_3 (15%) + POWER_CUT_REQUEST")
        void day14() {
            RentalInvoice inv = overdue(14, 1_000_000L);
            when(invoiceRepo.findOverdueMonthlyRentInvoices(any())).thenReturn(List.of(inv));
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), any())).thenReturn(false);

            scheduler.processLatePayments();

            assertThat(inv.getPenaltyAmount()).isEqualTo(150_000L);
            verify(kafka).send(eq("payment.power-cut-requested"), anyString(), any(PowerCutRequestEvent.class));
            verifyActionLog(LatePaymentAction.POWER_CUT_REQUEST);
            verify(actionLogRepo, never()).save(argThatAction(LatePaymentAction.TERMINATION_INITIATED));
        }

        @Test
        @DisplayName("day 30: TERMINATION_INITIATED fires")
        void day30() {
            RentalInvoice inv = overdue(30, 1_000_000L);
            when(invoiceRepo.findOverdueMonthlyRentInvoices(any())).thenReturn(List.of(inv));
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), any())).thenReturn(false);

            scheduler.processLatePayments();

            verify(kafka).send(eq("payment.termination-requested"), anyString(), any(TerminationRequestedEvent.class));
            verifyActionLog(LatePaymentAction.TERMINATION_INITIATED);
        }
    }

    @Nested
    @DisplayName("idempotency")
    class Idempotency {

        @Test
        @DisplayName("skips actions already logged (idempotent across runs)")
        void skipsLoggedActions() {
            RentalInvoice inv = overdue(3, 1_000_000L);
            when(invoiceRepo.findOverdueMonthlyRentInvoices(any())).thenReturn(List.of(inv));
            // Simulate: NOTIFY_DAY_0 already done, PENALTY_TIER_1 not yet
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), eq(LatePaymentAction.NOTIFY_DAY_0)))
                    .thenReturn(true);
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), eq(LatePaymentAction.NOTIFY_DAY_1)))
                    .thenReturn(false);
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), eq(LatePaymentAction.NOTIFY_DAY_2)))
                    .thenReturn(false);
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), eq(LatePaymentAction.PENALTY_TIER_1)))
                    .thenReturn(false);

            scheduler.processLatePayments();

            verify(actionLogRepo, never()).save(argThatAction(LatePaymentAction.NOTIFY_DAY_0));
            verifyActionLog(LatePaymentAction.PENALTY_TIER_1);
        }
    }

    @Nested
    @DisplayName("error isolation")
    class ErrorIsolation {

        @Test
        @DisplayName("continues processing next invoice when one fails")
        void isolatesFailure() {
            RentalInvoice bad = overdue(0, 1_000_000L);
            RentalInvoice ok = overdue(0, 500_000L);

            when(invoiceRepo.findOverdueMonthlyRentInvoices(any())).thenReturn(List.of(bad, ok));
            when(actionLogRepo.existsByInvoiceIdAndActionType(any(), any())).thenReturn(false);

            // Make processing bad invoice throw
            when(invoiceRepo.save(bad)).thenThrow(new RuntimeException("db down"));

            scheduler.processLatePayments();

            // ok invoice still processed
            verify(invoiceRepo).save(ok);
        }
    }

    @Test
    @DisplayName("no work when no overdue invoices")
    void noOverdue() {
        when(invoiceRepo.findOverdueMonthlyRentInvoices(any())).thenReturn(List.of());

        scheduler.processLatePayments();

        verify(invoiceRepo, never()).save(any());
        verify(actionLogRepo, never()).save(any());
    }

    private void verifyActionLog(LatePaymentAction action) {
        verify(actionLogRepo).save(argThatAction(action));
    }

    private static com.isums.paymentservice.domains.entities.LatePaymentActionLog argThatAction(LatePaymentAction action) {
        return org.mockito.ArgumentMatchers.argThat(log -> log != null && log.getActionType() == action);
    }
}
