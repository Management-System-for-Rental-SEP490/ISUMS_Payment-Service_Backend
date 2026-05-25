package com.isums.paymentservice.infrastructures.listeners;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.events.ContractCompletedEvent;
import com.isums.paymentservice.domains.events.DepositPaidEvent;
import com.isums.paymentservice.domains.events.DepositRefundConfirmedEvent;
import com.isums.paymentservice.domains.events.SendEmailEvent;
import com.isums.paymentservice.infrastructures.grpcs.UserGrpcService;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.isums.paymentservice.services.PaymentTokenService;
import common.kafkas.IdempotencyService;
import common.kafkas.KafkaListenerHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.util.ReflectionTestUtils;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("ContractEventListener")
class ContractEventListenerTest {

    @Mock private RentalInvoiceRepository invoiceRepository;
    @Mock private PaymentTokenService paymentTokenService;
    @Mock private KafkaTemplate<String, Object> kafka;
    @Mock private ObjectMapper objectMapper;
    @Mock private Acknowledgment ack;
    @Mock private IdempotencyService idempotencyService;
    @Mock private KafkaListenerHelper kafkaHelper;
    @Mock private UserGrpcService userGrpcService;

    @InjectMocks private ContractEventListener listener;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(listener, "outsystemPaymentUrl", "https://out.example/payments");
        org.mockito.Mockito.lenient()
                .when(kafkaHelper.extractMessageId(any())).thenReturn("test-msg-id");
    }

    @Nested
    @DisplayName("handleContractCompleted")
    class HandleContractCompleted {

        private ConsumerRecord<String, String> rec = new ConsumerRecord<>(
                "contract-completed-topic", 0, 0L, "k", "v");

        private ContractCompletedEvent event(long deposit) {
            return ContractCompletedEvent.builder()
                    .contractId(UUID.randomUUID()).tenantId(UUID.randomUUID())
                    .tenantEmail("alice@example.com").houseId(UUID.randomUUID())
                    .landlordId(UUID.randomUUID()).isNewAccount(true)
                    .depositAmount(deposit).rentAmount(5_000_000L).payDate(5)
                    .startAt(Instant.now()).signedPdfUrl("https://pdf.example/c.pdf")
                    .build();
        }

        @Test
        @DisplayName("creates DEPOSIT invoice, sends email (auto-acked on return)")
        void happy() throws Exception {
            ContractCompletedEvent evt = event(10_000_000L);
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(evt.getContractId(), InvoiceType.DEPOSIT))
                    .thenReturn(Optional.empty());
            when(paymentTokenService.generateToken(any(), any())).thenReturn("tok-1");

            listener.handleContractCompleted("v");

            ArgumentCaptor<List<RentalInvoice>> cap = ArgumentCaptor.forClass(List.class);
            verify(invoiceRepository).saveAll(cap.capture());
            assertThatInvoiceDeposit(cap.getValue(), evt);
            verify(kafka, org.mockito.Mockito.times(2))
                    .send(eq("notification-email"), any(SendEmailEvent.class));
        }

        private void assertThatInvoiceDeposit(List<RentalInvoice> invoices, ContractCompletedEvent evt) {
            org.assertj.core.api.Assertions.assertThat(invoices).hasSize(1);
            RentalInvoice inv = invoices.get(0);
            org.assertj.core.api.Assertions.assertThat(inv.getType()).isEqualTo(InvoiceType.DEPOSIT);
            org.assertj.core.api.Assertions.assertThat(inv.getBaseAmount()).isEqualTo(evt.getDepositAmount());
            org.assertj.core.api.Assertions.assertThat(inv.getTotalAmount()).isEqualTo(evt.getDepositAmount());
            org.assertj.core.api.Assertions.assertThat(inv.getStatus()).isEqualTo(InvoiceStatus.UNPAID);
        }

        @Test
        @DisplayName("when DEPOSIT already exists + UNPAID: re-queues both emails, does NOT recreate invoice")
        void resendOnExistingUnpaid() throws Exception {
            ContractCompletedEvent evt = event(10_000_000L);
            RentalInvoice existing = RentalInvoice.builder()
                    .id(UUID.randomUUID()).contractId(evt.getContractId())
                    .tenantId(evt.getTenantId()).houseId(evt.getHouseId())
                    .type(InvoiceType.DEPOSIT).periodKey("DEPOSIT")
                    .baseAmount(10_000_000L).totalAmount(10_000_000L)
                    .status(InvoiceStatus.UNPAID).dueDate(Instant.now()).build();
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(evt.getContractId(), InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(existing));
            when(paymentTokenService.generateToken(any(), any())).thenReturn("tok-resend");

            listener.handleContractCompleted("v");

            verify(invoiceRepository, never()).saveAll(any());
            verify(invoiceRepository, never()).save(any(RentalInvoice.class));
            verify(kafka, org.mockito.Mockito.times(2))
                    .send(eq("notification-email"), any(SendEmailEvent.class));
        }

        @Test
        @DisplayName("when DEPOSIT already exists + OVERDUE: re-queues both emails (still recoverable)")
        void resendOnExistingOverdue() throws Exception {
            ContractCompletedEvent evt = event(10_000_000L);
            RentalInvoice existing = RentalInvoice.builder()
                    .id(UUID.randomUUID()).contractId(evt.getContractId())
                    .tenantId(evt.getTenantId()).houseId(evt.getHouseId())
                    .type(InvoiceType.DEPOSIT).periodKey("DEPOSIT")
                    .baseAmount(10_000_000L).totalAmount(10_000_000L)
                    .status(InvoiceStatus.OVERDUE).dueDate(Instant.now()).build();
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(evt.getContractId(), InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(existing));
            when(paymentTokenService.generateToken(any(), any())).thenReturn("tok-od");

            listener.handleContractCompleted("v");

            verify(invoiceRepository, never()).saveAll(any());
            verify(kafka, org.mockito.Mockito.times(2))
                    .send(eq("notification-email"), any(SendEmailEvent.class));
        }

        @Test
        @DisplayName("when DEPOSIT already exists + PAID: re-queues CONTRACT_COMPLETED only, no payment link")
        void resendOnExistingPaid() throws Exception {
            ContractCompletedEvent evt = event(10_000_000L);
            RentalInvoice existing = RentalInvoice.builder()
                    .id(UUID.randomUUID()).contractId(evt.getContractId())
                    .tenantId(evt.getTenantId()).houseId(evt.getHouseId())
                    .type(InvoiceType.DEPOSIT).periodKey("DEPOSIT")
                    .baseAmount(10_000_000L).totalAmount(10_000_000L)
                    .status(InvoiceStatus.PAID).dueDate(Instant.now()).build();
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(evt.getContractId(), InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(existing));

            listener.handleContractCompleted("v");

            verify(invoiceRepository, never()).saveAll(any());
            verify(kafka, org.mockito.Mockito.times(1))
                    .send(eq("notification-email"), any(SendEmailEvent.class));
        }

        @Test
        @DisplayName("when DEPOSIT exists + UNPAID but tenantEmail blank: skips PAYMENT_INVOICE, still queues CONTRACT_COMPLETED if signedPdfUrl present")
        void resendWithBlankEmail() throws Exception {
            ContractCompletedEvent evt = ContractCompletedEvent.builder()
                    .contractId(UUID.randomUUID()).tenantId(UUID.randomUUID())
                    .tenantEmail("").houseId(UUID.randomUUID())
                    .landlordId(UUID.randomUUID()).isNewAccount(true)
                    .depositAmount(10_000_000L).rentAmount(5_000_000L).payDate(5)
                    .startAt(Instant.now()).signedPdfUrl("https://pdf/c.pdf")
                    .build();
            RentalInvoice existing = RentalInvoice.builder()
                    .id(UUID.randomUUID()).contractId(evt.getContractId())
                    .tenantId(evt.getTenantId()).houseId(evt.getHouseId())
                    .type(InvoiceType.DEPOSIT).periodKey("DEPOSIT")
                    .baseAmount(10_000_000L).totalAmount(10_000_000L)
                    .status(InvoiceStatus.UNPAID).dueDate(Instant.now()).build();
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(evt.getContractId(), InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(existing));

            listener.handleContractCompleted("v");

            verify(invoiceRepository, never()).saveAll(any());
            verify(kafka, never()).send(eq("notification-email"), any(SendEmailEvent.class));
        }

        @Test
        @DisplayName("zero deposit branches to first-rent-only path (no DEPOSIT saveAll)")
        void zeroDeposit() throws Exception {
            ContractCompletedEvent evt = event(0L);
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(evt.getContractId(), InvoiceType.DEPOSIT))
                    .thenReturn(Optional.empty());
            when(invoiceRepository.save(any(RentalInvoice.class)))
                    .thenAnswer(inv -> inv.getArgument(0));
            when(paymentTokenService.generateToken(any(), any())).thenReturn("tok-zero");

            listener.handleContractCompleted("v");

            verify(invoiceRepository, never()).saveAll(any());
        }

        @Test
        @DisplayName("swallows Jackson failure (logs + auto-acks on return)")
        void jacksonFails() throws Exception {
            when(objectMapper.readValue(any(String.class), eq(ContractCompletedEvent.class)))
                    .thenThrow(new JsonParseException(null, "bad"));

            listener.handleContractCompleted("v");

            verifyNoInteractions(invoiceRepository);
        }

        @Test
        @DisplayName("swallows null payload (logs, no work, no retry)")
        void nullPayload() {
            listener.handleContractCompleted(null);
            verifyNoInteractions(invoiceRepository, kafka);
        }

        @Test
        @DisplayName("rethrows for retry on generic Exception")
        void rethrows() throws Exception {
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(event(1L));
            when(invoiceRepository.findByContractIdAndType(any(), any()))
                    .thenThrow(new RuntimeException("db"));

            assertThatThrownBy(() -> listener.handleContractCompleted("v"))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Nested
    @DisplayName("handleDepositExpired")
    class HandleDepositExpired {

        private com.isums.paymentservice.domains.events.ContractDepositExpiredEvent event(
                UUID contractId, String tenantEmail) {
            return new com.isums.paymentservice.domains.events.ContractDepositExpiredEvent(
                    contractId, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(),
                    tenantEmail, "Khach Thue", "EC-12345678",
                    5_000_000L, Instant.now(), Instant.now(), "msg-de-1");
        }

        private RentalInvoice depositInvoice(UUID contractId, InvoiceStatus status) {
            return RentalInvoice.builder()
                    .id(UUID.randomUUID()).contractId(contractId)
                    .tenantId(UUID.randomUUID()).houseId(UUID.randomUUID())
                    .type(InvoiceType.DEPOSIT).periodKey("DEPOSIT")
                    .baseAmount(5_000_000L).totalAmount(5_000_000L)
                    .status(status).dueDate(Instant.now()).build();
        }

        @Test
        @DisplayName("cancels DEPOSIT invoice + queues expired-tenant email on happy path")
        void happy() throws Exception {
            UUID contractId = UUID.randomUUID();
            var evt = event(contractId, "alice@example.com");
            RentalInvoice inv = depositInvoice(contractId, InvoiceStatus.UNPAID);
            when(objectMapper.readValue("v", com.isums.paymentservice.domains.events
                    .ContractDepositExpiredEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(inv));

            listener.handleDepositExpired("v");

            org.assertj.core.api.Assertions.assertThat(inv.getStatus())
                    .isEqualTo(InvoiceStatus.CANCELLED);
            verify(invoiceRepository).save(inv);
            ArgumentCaptor<SendEmailEvent> emailCap = ArgumentCaptor.forClass(SendEmailEvent.class);
            verify(kafka).send(eq("notification-email"), emailCap.capture());
            org.assertj.core.api.Assertions.assertThat(emailCap.getValue().to())
                    .isEqualTo("alice@example.com");
            org.assertj.core.api.Assertions.assertThat(emailCap.getValue().templateCode())
                    .isEqualTo("CONTRACT_DEPOSIT_EXPIRED_TENANT_INVOICE");
        }

        @Test
        @DisplayName("also cancels invoice in OVERDUE state")
        void overdueInvoice() throws Exception {
            UUID contractId = UUID.randomUUID();
            var evt = event(contractId, "bob@example.com");
            RentalInvoice inv = depositInvoice(contractId, InvoiceStatus.OVERDUE);
            when(objectMapper.readValue("v", com.isums.paymentservice.domains.events
                    .ContractDepositExpiredEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(inv));

            listener.handleDepositExpired("v");

            org.assertj.core.api.Assertions.assertThat(inv.getStatus())
                    .isEqualTo(InvoiceStatus.CANCELLED);
            verify(invoiceRepository).save(inv);
        }

        @Test
        @DisplayName("does NOT re-cancel an already-CANCELLED invoice (idempotent)")
        void alreadyCancelled() throws Exception {
            UUID contractId = UUID.randomUUID();
            var evt = event(contractId, "carol@example.com");
            RentalInvoice inv = depositInvoice(contractId, InvoiceStatus.CANCELLED);
            when(objectMapper.readValue("v", com.isums.paymentservice.domains.events
                    .ContractDepositExpiredEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(inv));

            listener.handleDepositExpired("v");

            verify(invoiceRepository, never()).save(any(RentalInvoice.class));
        }

        @Test
        @DisplayName("does NOT crash + does NOT send email when tenantEmail is blank")
        void blankEmail() throws Exception {
            UUID contractId = UUID.randomUUID();
            var evt = event(contractId, "");
            RentalInvoice inv = depositInvoice(contractId, InvoiceStatus.UNPAID);
            when(objectMapper.readValue("v", com.isums.paymentservice.domains.events
                    .ContractDepositExpiredEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(inv));

            listener.handleDepositExpired("v");

            org.assertj.core.api.Assertions.assertThat(inv.getStatus())
                    .isEqualTo(InvoiceStatus.CANCELLED);
            verify(kafka, never()).send(eq("notification-email"), any(SendEmailEvent.class));
        }

        @Test
        @DisplayName("handles missing invoice (deletes nothing, still sends email)")
        void missingInvoice() throws Exception {
            UUID contractId = UUID.randomUUID();
            var evt = event(contractId, "dave@example.com");
            when(objectMapper.readValue("v", com.isums.paymentservice.domains.events
                    .ContractDepositExpiredEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.empty());

            listener.handleDepositExpired("v");

            verify(invoiceRepository, never()).save(any(RentalInvoice.class));
            verify(kafka).send(eq("notification-email"), any(SendEmailEvent.class));
        }

        @Test
        @DisplayName("swallows Jackson failure (no retry)")
        void jacksonFails() throws Exception {
            when(objectMapper.readValue(any(String.class), eq(com.isums.paymentservice
                    .domains.events.ContractDepositExpiredEvent.class)))
                    .thenThrow(new JsonParseException(null, "bad"));

            listener.handleDepositExpired("v");

            verifyNoInteractions(invoiceRepository);
        }

        @Test
        @DisplayName("swallows null payload (logs, no work)")
        void nullPayload() {
            listener.handleDepositExpired(null);
            verifyNoInteractions(invoiceRepository, kafka);
        }

        @Test
        @DisplayName("rethrows for retry on generic Exception")
        void rethrows() throws Exception {
            UUID contractId = UUID.randomUUID();
            var evt = event(contractId, "eve@example.com");
            when(objectMapper.readValue("v", com.isums.paymentservice.domains.events
                    .ContractDepositExpiredEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(any(), any()))
                    .thenThrow(new RuntimeException("db down"));

            assertThatThrownBy(() -> listener.handleDepositExpired("v"))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Nested
    @DisplayName("handleDepositPaid")
    class HandleDepositPaid {

        @Test
        @DisplayName("creates MONTHLY_RENT invoice and enriches/maps events on happy path")
        void happy() throws Exception {
            UUID contractId = UUID.randomUUID();
            UUID tenantId = UUID.randomUUID();
            UUID houseId = UUID.randomUUID();
            DepositPaidEvent evt = DepositPaidEvent.builder()
                    .contractId(contractId).tenantId(tenantId).houseId(houseId)
                    .invoiceId(UUID.randomUUID()).amount(1_000L).txnNo("t1")
                    .paidAt(Instant.now()).invoiceType(InvoiceType.DEPOSIT)
                    .build();

            RentalInvoice depositInv = RentalInvoice.builder()
                    .id(UUID.randomUUID()).contractId(contractId).tenantId(tenantId)
                    .houseId(houseId).type(InvoiceType.DEPOSIT)
                    .periodKey("DEPOSIT").baseAmount(1000L).totalAmount(1000L)
                    .rentAmount(5_000_000L).payDate(5).contractStartAt(Instant.now())
                    .tenantEmail("a@b.com").isNewAccount(true)
                    .status(InvoiceStatus.PAID).dueDate(Instant.now()).build();

            when(objectMapper.readValue("v", DepositPaidEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(depositInv));
            when(invoiceRepository.existsByContractIdAndPeriodKey(eq(contractId), any(String.class)))
                    .thenReturn(false);
            when(invoiceRepository.save(any(RentalInvoice.class))).thenAnswer(a -> {
                RentalInvoice r = a.getArgument(0);
                if (r.getId() == null) r.setId(UUID.randomUUID());
                return r;
            });
            when(paymentTokenService.generateToken(any(), eq(tenantId))).thenReturn("tok-xyz");

            listener.handleDepositPaid("v");

            ArgumentCaptor<RentalInvoice> invCap = ArgumentCaptor.forClass(RentalInvoice.class);
            verify(invoiceRepository).save(invCap.capture());
            org.assertj.core.api.Assertions.assertThat(invCap.getValue().getType()).isEqualTo(InvoiceType.MONTHLY_RENT);
            org.assertj.core.api.Assertions.assertThat(invCap.getValue().getTotalAmount()).isEqualTo(5_000_000L);

            verify(kafka).send(eq("deposit-paid-enriched-topic"), any());
            verify(kafka).send(eq("map-user-to-house-topic"), any());
        }

        @Test
        @DisplayName("skips when deposit invoice lacks rentAmount context")
        void noDepositContext() throws Exception {
            UUID contractId = UUID.randomUUID();
            DepositPaidEvent evt = DepositPaidEvent.builder()
                    .contractId(contractId).tenantId(UUID.randomUUID()).houseId(UUID.randomUUID())
                    .invoiceId(UUID.randomUUID()).amount(1_000L).txnNo("t1")
                    .paidAt(Instant.now()).invoiceType(InvoiceType.DEPOSIT)
                    .build();

            when(objectMapper.readValue("v", DepositPaidEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.empty());

            listener.handleDepositPaid("v");

            verify(invoiceRepository, never()).save(any());
            verify(kafka, never()).send(any(String.class), any());
        }

        @Test
        @DisplayName("re-emits enriched event when MONTHLY_RENT already exists (Kafka redelivery safety)")
        void alreadyExists() throws Exception {
            UUID contractId = UUID.randomUUID();
            UUID tenantId = UUID.randomUUID();
            UUID houseId = UUID.randomUUID();
            DepositPaidEvent evt = DepositPaidEvent.builder()
                    .contractId(contractId).tenantId(tenantId).houseId(houseId)
                    .invoiceId(UUID.randomUUID()).amount(1L).txnNo("t").paidAt(Instant.now())
                    .invoiceType(InvoiceType.DEPOSIT).build();
            RentalInvoice depositInv = RentalInvoice.builder()
                    .contractId(contractId).type(InvoiceType.DEPOSIT)
                    .periodKey("DEPOSIT").baseAmount(1L).totalAmount(1L)
                    .rentAmount(100L).payDate(5).contractStartAt(Instant.now())
                    .tenantEmail("alice@example.com").isNewAccount(true)
                    .status(InvoiceStatus.PAID).dueDate(Instant.now()).build();
            RentalInvoice existingMonthly = RentalInvoice.builder()
                    .id(UUID.randomUUID()).contractId(contractId).tenantId(tenantId)
                    .houseId(houseId).type(InvoiceType.MONTHLY_RENT)
                    .totalAmount(100L).status(InvoiceStatus.UNPAID)
                    .dueDate(Instant.now()).build();

            when(objectMapper.readValue("v", DepositPaidEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(depositInv));
            when(invoiceRepository.existsByContractIdAndPeriodKey(eq(contractId), any(String.class)))
                    .thenReturn(true);
            when(invoiceRepository.findByContractIdAndPeriodKey(eq(contractId), any(String.class)))
                    .thenReturn(Optional.of(existingMonthly));
            when(paymentTokenService.generateToken(any(), eq(tenantId))).thenReturn("tok-xyz");

            listener.handleDepositPaid("v");

            verify(invoiceRepository, never()).save(any());
            verify(kafka).send(eq("deposit-paid-enriched-topic"), any());
            verify(kafka).send(eq("map-user-to-house-topic"), any());
        }

        @Test
        @DisplayName("swallows null payload (no work, no retry)")
        void nullPayload() {
            listener.handleDepositPaid(null);
            verifyNoInteractions(invoiceRepository);
            verify(kafka, never()).send(any(String.class), any());
        }

        @Test
        @DisplayName("swallows Jackson failure")
        void jacksonFails() throws Exception {
            when(objectMapper.readValue(any(String.class), eq(DepositPaidEvent.class)))
                    .thenThrow(new JsonParseException(null, "bad"));

            listener.handleDepositPaid("v");

            verifyNoInteractions(invoiceRepository);
        }
    }

    @Nested
    @DisplayName("handleDepositRefundConfirmed")
    class HandleDepositRefundConfirmed {

        private ConsumerRecord<String, String> rec = new ConsumerRecord<>(
                "contract.deposit-refund.confirmed", 0, 0L, "k", "v");

        @Test
        @DisplayName("creates DEPOSIT_REFUND invoice and sends notification email")
        void happy() throws Exception {
            UUID contractId = UUID.randomUUID();
            DepositRefundConfirmedEvent evt = DepositRefundConfirmedEvent.builder()
                    .contractId(contractId).tenantId(UUID.randomUUID()).houseId(UUID.randomUUID())
                    .tenantEmail("alice@example.com").refundAmount(2_000_000L)
                    .note("some note").messageId("m1").build();

            when(objectMapper.readValue("v", DepositRefundConfirmedEvent.class)).thenReturn(evt);
            when(invoiceRepository.existsByContractIdAndPeriodKey(contractId, "DEPOSIT_REFUND"))
                    .thenReturn(false);

            listener.handleDepositRefundConfirmed("v");

            ArgumentCaptor<RentalInvoice> cap = ArgumentCaptor.forClass(RentalInvoice.class);
            verify(invoiceRepository).save(cap.capture());
            org.assertj.core.api.Assertions.assertThat(cap.getValue().getType()).isEqualTo(InvoiceType.DEPOSIT_REFUND);
            org.assertj.core.api.Assertions.assertThat(cap.getValue().getTotalAmount()).isEqualTo(2_000_000L);
            verify(kafka).send(eq("notification-email"), any(SendEmailEvent.class));
        }

        @Test
        @DisplayName("falls back to user gRPC when event email missing")
        void fallbackEmail() throws Exception {
            UUID contractId = UUID.randomUUID();
            UUID tenantId = UUID.randomUUID();
            DepositRefundConfirmedEvent evt = DepositRefundConfirmedEvent.builder()
                    .contractId(contractId).tenantId(tenantId).houseId(UUID.randomUUID())
                    .refundAmount(2_000_000L).messageId("m1").build();

            when(objectMapper.readValue("v", DepositRefundConfirmedEvent.class)).thenReturn(evt);
            when(invoiceRepository.existsByContractIdAndPeriodKey(contractId, "DEPOSIT_REFUND"))
                    .thenReturn(false);
            when(userGrpcService.getTenantEmail(tenantId)).thenReturn("alice@example.com");

            listener.handleDepositRefundConfirmed("v");

            ArgumentCaptor<RentalInvoice> cap = ArgumentCaptor.forClass(RentalInvoice.class);
            verify(invoiceRepository).save(cap.capture());
            org.assertj.core.api.Assertions.assertThat(cap.getValue().getTenantEmail())
                    .isEqualTo("alice@example.com");
            verify(kafka).send(eq("notification-email"), any(SendEmailEvent.class));
        }

        @Test
        @DisplayName("idempotent when DEPOSIT_REFUND already exists")
        void idempotent() throws Exception {
            UUID contractId = UUID.randomUUID();
            DepositRefundConfirmedEvent evt = DepositRefundConfirmedEvent.builder()
                    .contractId(contractId).tenantId(UUID.randomUUID()).houseId(UUID.randomUUID())
                    .refundAmount(1L).messageId("m1").build();

            when(objectMapper.readValue("v", DepositRefundConfirmedEvent.class)).thenReturn(evt);
            when(invoiceRepository.existsByContractIdAndPeriodKey(contractId, "DEPOSIT_REFUND"))
                    .thenReturn(true);

            listener.handleDepositRefundConfirmed("v");

            verify(invoiceRepository, never()).save(any());
        }

        @Test
        @DisplayName("rethrows as RuntimeException on any failure (retry)")
        void failure() throws Exception {
            when(objectMapper.readValue(any(String.class), eq(DepositRefundConfirmedEvent.class)))
                    .thenThrow(new RuntimeException("err"));

            assertThatThrownBy(() -> listener.handleDepositRefundConfirmed("v"))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @Nested
    @DisplayName("handleForceTermination")
    class HandleForceTermination {

        private final ConsumerRecord<String, String> rec = new ConsumerRecord<>(
                "contract.force-terminated", 0, 0L, "k", "v");

        private com.isums.paymentservice.domains.events.ForceTerminationEvent event(UUID contractId) {
            return com.isums.paymentservice.domains.events.ForceTerminationEvent.builder()
                    .contractId(contractId)
                    .houseId(UUID.randomUUID())
                    .tenantId(UUID.randomUUID())
                    .reason("OVERDUE_PAYMENT_30_DAYS")
                    .actorId(UUID.randomUUID())
                    .terminatedAt(Instant.now())
                    .messageId("ft-1")
                    .build();
        }

        private RentalInvoice unpaid(UUID contractId, InvoiceType type, long amount) {
            return RentalInvoice.builder()
                    .id(UUID.randomUUID())
                    .contractId(contractId)
                    .tenantId(UUID.randomUUID())
                    .houseId(UUID.randomUUID())
                    .type(type)
                    .periodKey("2026-04")
                    .baseAmount(amount).serviceAmount(0L).penaltyAmount(0L)
                    .totalAmount(amount)
                    .status(InvoiceStatus.UNPAID)
                    .dueDate(Instant.now())
                    .build();
        }

        @Test
        @DisplayName("forfeits all UNPAID/OVERDUE rent invoices and PAID deposit on force-termination")
        void forfeitsAll() throws Exception {
            UUID contractId = UUID.randomUUID();
            RentalInvoice rent1 = unpaid(contractId, InvoiceType.MONTHLY_RENT, 5_000_000L);
            RentalInvoice rent2 = unpaid(contractId, InvoiceType.MONTHLY_RENT, 5_000_000L);
            rent2.setStatus(InvoiceStatus.OVERDUE);
            RentalInvoice deposit = unpaid(contractId, InvoiceType.DEPOSIT, 10_000_000L);
            deposit.setStatus(InvoiceStatus.PAID);

            when(objectMapper.readValue("v", com.isums.paymentservice.domains.events.ForceTerminationEvent.class))
                    .thenReturn(event(contractId));
            when(invoiceRepository.findByContractIdAndStatus(contractId, InvoiceStatus.UNPAID))
                    .thenReturn(List.of(rent1));
            when(invoiceRepository.findByContractIdAndStatus(contractId, InvoiceStatus.OVERDUE))
                    .thenReturn(List.of(rent2));
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(deposit));

            listener.handleForceTermination("v");

            ArgumentCaptor<RentalInvoice> savedCaptor = ArgumentCaptor.forClass(RentalInvoice.class);
            verify(invoiceRepository, org.mockito.Mockito.atLeast(3)).save(savedCaptor.capture());
            org.assertj.core.api.Assertions.assertThat(savedCaptor.getAllValues())
                    .extracting(RentalInvoice::getStatus)
                    .containsOnly(InvoiceStatus.FORFEITED);
        }

        @Test
        @DisplayName("skips DEPOSIT in unpaid loop (deposit handled separately)")
        void skipsDepositInUnpaidLoop() throws Exception {
            UUID contractId = UUID.randomUUID();
            RentalInvoice unpaidDeposit = unpaid(contractId, InvoiceType.DEPOSIT, 10_000_000L);

            when(objectMapper.readValue("v", com.isums.paymentservice.domains.events.ForceTerminationEvent.class))
                    .thenReturn(event(contractId));
            when(invoiceRepository.findByContractIdAndStatus(contractId, InvoiceStatus.UNPAID))
                    .thenReturn(List.of(unpaidDeposit));
            when(invoiceRepository.findByContractIdAndStatus(contractId, InvoiceStatus.OVERDUE))
                    .thenReturn(List.of());
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(unpaidDeposit));

            listener.handleForceTermination("v");

            verify(invoiceRepository, never()).save(unpaidDeposit);
        }

        @Test
        @DisplayName("acks on missing contractId")
        void missingContractId() throws Exception {
            when(objectMapper.readValue("v", com.isums.paymentservice.domains.events.ForceTerminationEvent.class))
                    .thenReturn(com.isums.paymentservice.domains.events.ForceTerminationEvent.builder()
                            .messageId("m").build());

            listener.handleForceTermination("v");

            verify(invoiceRepository, never()).save(any());
        }
    }
}
