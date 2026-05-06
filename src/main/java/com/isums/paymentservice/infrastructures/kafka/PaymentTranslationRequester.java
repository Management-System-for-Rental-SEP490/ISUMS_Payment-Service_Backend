package com.isums.paymentservice.infrastructures.kafka;

import com.isums.common.i18n.SupportedLocales;
import com.isums.common.i18n.TranslationMap;
import com.isums.common.i18n.events.TextTranslationRequestedEvent;
import com.isums.common.i18n.events.TranslationIntent;
import com.isums.paymentservice.domains.entities.InvoicePenaltyItem;
import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.domains.entities.PaymentEscalation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentTranslationRequester {

    static final String CALLBACK_TOPIC = "text.translation.result.payment";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${isums.i18n.payment.auto-translate:true}")
    private boolean autoTranslate;

    @Value("${isums.i18n.payment.required-locales:vi,en,ja}")
    private String requiredLocalesCsv;

    @Value("${isums.i18n.payment.default-source:vi}")
    private String defaultSourceLanguage;

    public void requestForPayment(Payment p) {
        if (!autoTranslate || p == null || p.getId() == null) return;
        if (p.getNote() != null && !p.getNote().isBlank()) {
            publish(p.getId(), "payment.note", "note", p.getNote(), p.getNoteTranslations());
        }
    }

    public void requestForPenalty(InvoicePenaltyItem item) {
        if (!autoTranslate || item == null || item.getId() == null) return;
        if (item.getDescription() != null && !item.getDescription().isBlank()) {
            publish(item.getId(), "invoice-penalty.description", "description",
                    item.getDescription(), item.getDescriptionTranslations());
        }
    }

    public void requestForEscalation(PaymentEscalation esc) {
        if (!autoTranslate || esc == null || esc.getId() == null) return;
        if (esc.getNote() != null && !esc.getNote().isBlank()) {
            publish(esc.getId(), "payment-escalation.note", "note",
                    esc.getNote(), esc.getNoteTranslations());
        }
    }

    private void publish(UUID id, String resourceType, String fieldName, String text, TranslationMap existing) {
        Set<String> required = parseLocales();
        Set<String> have = existing == null ? Set.of() : existing.languagesPresent();
        List<String> missing = new ArrayList<>();
        for (String loc : required) {
            if (loc.equals(defaultSourceLanguage)) continue;
            if (!have.contains(loc)) missing.add(loc);
        }
        if (missing.isEmpty()) return;

        TextTranslationRequestedEvent ev = new TextTranslationRequestedEvent(
                UUID.randomUUID(),
                resourceType,
                id,
                fieldName,
                text,
                defaultSourceLanguage,
                missing,
                resourceType.startsWith("invoice-penalty") ? TranslationIntent.CUSTOMER_FACING_UI : TranslationIntent.STAFF_INTERNAL,
                resourceType.startsWith("invoice-penalty") ? Boolean.TRUE : Boolean.FALSE,
                Instant.now(),
                CALLBACK_TOPIC);
        try {
            kafkaTemplate.send(TextTranslationRequestedEvent.TOPIC, id.toString(), ev);
        } catch (Exception ex) {
            log.warn("publish payment translation request failed: {}", ex.toString());
        }
    }

    private Set<String> parseLocales() {
        Set<String> out = new LinkedHashSet<>();
        for (String raw : requiredLocalesCsv.split(",")) {
            String c = TranslationMap.normalizeLanguage(raw);
            if (c != null && SupportedLocales.isSupported(c)) out.add(c);
        }
        if (out.isEmpty()) out.addAll(SupportedLocales.ALL);
        return out;
    }
}
