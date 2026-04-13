package com.isums.paymentservice.infrastructures.listeners;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.events.ContractCompletedEvent;
import com.isums.paymentservice.domains.events.DepositPaidEvent;
import com.isums.paymentservice.domains.events.DepositRefundConfirmedEvent;
import com.isums.paymentservice.domains.events.SendEmailEvent;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import com.isums.paymentservice.services.PaymentTokenService;
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
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

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

    @InjectMocks private ContractEventListener listener;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(listener, "outsystemPaymentUrl", "https://out.example/payments");
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
        @DisplayName("creates DEPOSIT invoice, sends email, acknowledges")
        void happy() throws Exception {
            ContractCompletedEvent evt = event(10_000_000L);
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(evt);
            when(invoiceRepository.existsByContractIdAndPeriodKey(evt.getContractId(), "DEPOSIT"))
                    .thenReturn(false);
            when(paymentTokenService.generateToken(any(), any())).thenReturn("tok-1");

            listener.handleContractCompleted(rec, ack);

            ArgumentCaptor<List<RentalInvoice>> cap = ArgumentCaptor.forClass(List.class);
            verify(invoiceRepository).saveAll(cap.capture());
            assertThatInvoiceDeposit(cap.getValue(), evt);
            // 1 CONTRACT_COMPLETED email + 1 PAYMENT_INVOICE email per invoice
            verify(kafka, org.mockito.Mockito.times(2))
                    .send(eq("notification-email"), any(SendEmailEvent.class));
            verify(ack).acknowledge();
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
        @DisplayName("skips when deposit invoice already exists (idempotent)")
        void idempotent() throws Exception {
            ContractCompletedEvent evt = event(10_000_000L);
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(evt);
            when(invoiceRepository.existsByContractIdAndPeriodKey(evt.getContractId(), "DEPOSIT"))
                    .thenReturn(true);

            listener.handleContractCompleted(rec, ack);

            verify(invoiceRepository, never()).saveAll(any());
            verify(kafka, never()).send(any(String.class), any(SendEmailEvent.class));
            verify(ack).acknowledge();
        }

        @Test
        @DisplayName("skips saveAll when depositAmount is null or zero")
        void zeroDeposit() throws Exception {
            ContractCompletedEvent evt = event(0L);
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(evt);
            when(invoiceRepository.existsByContractIdAndPeriodKey(evt.getContractId(), "DEPOSIT"))
                    .thenReturn(false);

            listener.handleContractCompleted(rec, ack);

            ArgumentCaptor<List<RentalInvoice>> cap = ArgumentCaptor.forClass(List.class);
            verify(invoiceRepository).saveAll(cap.capture());
            org.assertj.core.api.Assertions.assertThat(cap.getValue()).isEmpty();
            verify(ack).acknowledge();
        }

        @Test
        @DisplayName("acks on Jackson failure")
        void jacksonFails() throws Exception {
            when(objectMapper.readValue(any(String.class), eq(ContractCompletedEvent.class)))
                    .thenThrow(new JacksonException("bad") {});

            listener.handleContractCompleted(rec, ack);

            verify(ack).acknowledge();
            verifyNoInteractions(invoiceRepository);
        }

        @Test
        @DisplayName("rethrows for retry on generic Exception")
        void rethrows() throws Exception {
            when(objectMapper.readValue("v", ContractCompletedEvent.class)).thenReturn(event(1L));
            when(invoiceRepository.existsByContractIdAndPeriodKey(any(), any()))
                    .thenThrow(new RuntimeException("db"));

            assertThatThrownBy(() -> listener.handleContractCompleted(rec, ack))
                    .isInstanceOf(RuntimeException.class);
            verify(ack, never()).acknowledge();
        }
    }

    @Nested
    @DisplayName("handleDepositPaid")
    class HandleDepositPaid {

        private ConsumerRecord<String, String> rec = new ConsumerRecord<>(
                "deposit-paid-topic", 0, 0L, "k", "v");

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

            listener.handleDepositPaid(rec, ack);

            ArgumentCaptor<RentalInvoice> invCap = ArgumentCaptor.forClass(RentalInvoice.class);
            verify(invoiceRepository).save(invCap.capture());
            org.assertj.core.api.Assertions.assertThat(invCap.getValue().getType()).isEqualTo(InvoiceType.MONTHLY_RENT);
            org.assertj.core.api.Assertions.assertThat(invCap.getValue().getTotalAmount()).isEqualTo(5_000_000L);

            verify(kafka).send(eq("deposit-paid-enriched-topic"), any());
            verify(kafka).send(eq("map-user-to-house-topic"), any());
            verify(ack).acknowledge();
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

            listener.handleDepositPaid(rec, ack);

            verify(invoiceRepository, never()).save(any());
            verify(kafka, never()).send(any(String.class), any());
            verify(ack).acknowledge();
        }

        @Test
        @DisplayName("idempotent when MONTHLY_RENT already exists")
        void alreadyExists() throws Exception {
            UUID contractId = UUID.randomUUID();
            DepositPaidEvent evt = DepositPaidEvent.builder()
                    .contractId(contractId).tenantId(UUID.randomUUID()).houseId(UUID.randomUUID())
                    .invoiceId(UUID.randomUUID()).amount(1L).txnNo("t").paidAt(Instant.now())
                    .invoiceType(InvoiceType.DEPOSIT).build();
            RentalInvoice depositInv = RentalInvoice.builder()
                    .contractId(contractId).type(InvoiceType.DEPOSIT)
                    .periodKey("DEPOSIT").baseAmount(1L).totalAmount(1L)
                    .rentAmount(100L).payDate(5).contractStartAt(Instant.now())
                    .status(InvoiceStatus.PAID).dueDate(Instant.now()).build();

            when(objectMapper.readValue("v", DepositPaidEvent.class)).thenReturn(evt);
            when(invoiceRepository.findByContractIdAndType(contractId, InvoiceType.DEPOSIT))
                    .thenReturn(Optional.of(depositInv));
            when(invoiceRepository.existsByContractIdAndPeriodKey(eq(contractId), any(String.class)))
                    .thenReturn(true);

            listener.handleDepositPaid(rec, ack);

            verify(invoiceRepository, never()).save(any());
            verify(ack).acknowledge();
        }

        @Test
        @DisplayName("acks on Jackson failure")
        void jacksonFails() throws Exception {
            when(objectMapper.readValue(any(String.class), eq(DepositPaidEvent.class)))
                    .thenThrow(new JacksonException("bad") {});

            listener.handleDepositPaid(rec, ack);

            verify(ack).acknowledge();
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

            listener.handleDepositRefundConfirmed(rec, ack);

            ArgumentCaptor<RentalInvoice> cap = ArgumentCaptor.forClass(RentalInvoice.class);
            verify(invoiceRepository).save(cap.capture());
            org.assertj.core.api.Assertions.assertThat(cap.getValue().getType()).isEqualTo(InvoiceType.DEPOSIT_REFUND);
            org.assertj.core.api.Assertions.assertThat(cap.getValue().getTotalAmount()).isEqualTo(2_000_000L);
            verify(kafka).send(eq("notification-email"), any(SendEmailEvent.class));
            verify(ack).acknowledge();
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

            listener.handleDepositRefundConfirmed(rec, ack);

            verify(invoiceRepository, never()).save(any());
            verify(ack).acknowledge();
        }

        @Test
        @DisplayName("rethrows as RuntimeException on any failure (retry)")
        void failure() throws Exception {
            when(objectMapper.readValue(any(String.class), eq(DepositRefundConfirmedEvent.class)))
                    .thenThrow(new RuntimeException("err"));

            assertThatThrownBy(() -> listener.handleDepositRefundConfirmed(rec, ack))
                    .isInstanceOf(RuntimeException.class);
            verify(ack, never()).acknowledge();
        }
    }
}
