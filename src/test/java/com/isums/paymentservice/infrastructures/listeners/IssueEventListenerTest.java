package com.isums.paymentservice.infrastructures.listeners;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import com.isums.paymentservice.domains.events.QuoteInvoiceCreateEvent;
import com.isums.paymentservice.infrastructures.repositories.RentalInvoiceRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("IssueEventListener")
class IssueEventListenerTest {

    @Mock private RentalInvoiceRepository invoiceRepository;
    @Mock private ObjectMapper objectMapper;
    @Mock private Acknowledgment ack;

    @InjectMocks private IssueEventListener listener;

    private final ConsumerRecord<String, String> rec =
            new ConsumerRecord<>("quote-invoice-create", 0, 0L, "k", "v");
    private QuoteInvoiceCreateEvent event() {
        return QuoteInvoiceCreateEvent.builder()
                .quoteId(UUID.randomUUID()).issueId(UUID.randomUUID())
                .tenantId(UUID.randomUUID()).houseId(UUID.randomUUID())
                .totalPrice(BigDecimal.valueOf(350_000L))
                .tenantEmail("alice@example.com").build();
    }

    @Test
    @DisplayName("creates ISSUE invoice with status UNPAID on happy path")
    void happy() throws Exception {
        QuoteInvoiceCreateEvent evt = event();
        when(objectMapper.readValue("v", QuoteInvoiceCreateEvent.class)).thenReturn(evt);
        when(invoiceRepository.existsByContractIdAndPeriodKey(eq(evt.getQuoteId()), any(String.class)))
                .thenReturn(false);

        listener.handle(rec, ack);

        ArgumentCaptor<RentalInvoice> cap = ArgumentCaptor.forClass(RentalInvoice.class);
        verify(invoiceRepository).save(cap.capture());
        RentalInvoice inv = cap.getValue();
        assertThat(inv.getType()).isEqualTo(InvoiceType.ISSUE);
        assertThat(inv.getTotalAmount()).isEqualTo(350_000L);
        assertThat(inv.getStatus()).isEqualTo(InvoiceStatus.UNPAID);
        assertThat(inv.getPeriodKey()).isEqualTo("QUOTE-" + evt.getQuoteId());
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("idempotent when already exists")
    void idempotent() throws Exception {
        QuoteInvoiceCreateEvent evt = event();
        when(objectMapper.readValue("v", QuoteInvoiceCreateEvent.class)).thenReturn(evt);
        when(invoiceRepository.existsByContractIdAndPeriodKey(eq(evt.getQuoteId()), any(String.class)))
                .thenReturn(true);

        listener.handle(rec, ack);

        verify(invoiceRepository, never()).save(any());
        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("acks on Jackson failure")
    void jacksonFails() throws Exception {
        when(objectMapper.readValue(any(String.class), eq(QuoteInvoiceCreateEvent.class)))
                .thenThrow(new JsonParseException(null, "bad"));

        listener.handle(rec, ack);

        verify(ack).acknowledge();
    }

    @Test
    @DisplayName("rethrows as RuntimeException on generic failure (retry)")
    void rethrows() throws Exception {
        when(objectMapper.readValue("v", QuoteInvoiceCreateEvent.class)).thenReturn(event());
        when(invoiceRepository.existsByContractIdAndPeriodKey(any(), any()))
                .thenThrow(new RuntimeException("db"));

        assertThatThrownBy(() -> listener.handle(rec, ack))
                .isInstanceOf(RuntimeException.class);
        verify(ack, never()).acknowledge();
    }

}
