package com.isums.paymentservice.infrastructures.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.isums.common.i18n.TranslationMap;
import com.isums.common.i18n.events.TextTranslationResultEvent;
import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.infrastructures.repositories.PaymentRepository;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Consumes translation results for any payment-domain resource. Routes by
 * {@code resourceType}; uses {@link PaymentRepository} for the {@code Payment}
 * entity and falls back to {@link EntityManager} for {@code InvoicePenaltyItem}
 * and {@code PaymentEscalation} (which currently lack dedicated repositories).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentTranslationResultListener {

    private final ObjectMapper objectMapper;
    private final PaymentRepository paymentRepo;
    private final EntityManager entityManager;

    @KafkaListener(topics = PaymentTranslationRequester.CALLBACK_TOPIC,
            groupId = "payment-translation-result")
    @Transactional
    public void onResult(String payload, Acknowledgment ack) {
        try {
            TextTranslationResultEvent ev = objectMapper.readValue(payload, TextTranslationResultEvent.class);
            if (!TextTranslationResultEvent.STATUS_DONE.equals(ev.status())
                    || ev.translatedText() == null || ev.translatedText().isBlank()) {
                ack.acknowledge();
                return;
            }
            apply(ev);
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to apply payment translation result", e);
            ack.acknowledge();
        }
    }

    private void apply(TextTranslationResultEvent ev) {
        Map<String, String> patch = new LinkedHashMap<>();
        patch.put(ev.targetLanguage(), ev.translatedText());

        switch (ev.resourceType()) {
            case "payment.note" -> paymentRepo.findById(ev.resourceId()).ifPresent(p -> {
                TranslationMap before = p.getNoteTranslations() == null ? TranslationMap.empty() : p.getNoteTranslations();
                p.setNoteTranslations(before.mergeAutoFilled(patch));
                paymentRepo.save(p);
            });
            case "invoice-penalty.description" -> applyByEm(
                    com.isums.paymentservice.domains.entities.InvoicePenaltyItem.class,
                    ev.resourceId(), patch,
                    item -> item.getDescriptionTranslations(),
                    (item, map) -> item.setDescriptionTranslations(map));
            case "payment-escalation.note" -> applyByEm(
                    com.isums.paymentservice.domains.entities.PaymentEscalation.class,
                    ev.resourceId(), patch,
                    esc -> esc.getNoteTranslations(),
                    (esc, map) -> esc.setNoteTranslations(map));
            default -> log.warn("Unknown resourceType for payment translation: {}", ev.resourceType());
        }
    }

    private <T> void applyByEm(Class<T> type, java.util.UUID id, Map<String, String> patch,
                               java.util.function.Function<T, TranslationMap> getter,
                               java.util.function.BiConsumer<T, TranslationMap> setter) {
        T entity = entityManager.find(type, id);
        if (entity == null) return;
        TranslationMap before = getter.apply(entity);
        if (before == null) before = TranslationMap.empty();
        setter.accept(entity, before.mergeAutoFilled(patch));
        entityManager.merge(entity);
    }
}
