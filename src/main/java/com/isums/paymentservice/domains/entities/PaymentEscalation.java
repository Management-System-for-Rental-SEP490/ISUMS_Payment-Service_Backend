package com.isums.paymentservice.domains.entities;

import com.isums.common.i18n.TranslationMap;
import com.isums.common.i18n.TranslationMapConverter;
import com.isums.paymentservice.domains.enums.ActionType;
import com.isums.paymentservice.domains.enums.EscalationType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UuidGenerator;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment_escalations", indexes = {
        @Index(name = "idx_escalation_invoice", columnList = "invoice_id"),
        @Index(name = "idx_escalation_tenant", columnList = "tenant_id, house_id")
})
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class PaymentEscalation {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(name = "invoice_id", nullable = false)
    private UUID invoiceId;

    @Column(name = "tenant_id")
    private UUID tenantId;

    @Column(name = "house_id")
    private UUID houseId;

    @Column(name = "days_overdue")
    private int daysOverDue;

    @Column(name = "escalation_level")
    private int escalationLevel;

    @Enumerated(EnumType.STRING)
    @Column(name = "action_type", nullable = false)
    private ActionType actionType;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private EscalationType type;

    @Column(name = "executed_at")
    private Instant executedAt;

    private String note;

    @Column(name = "note_translations", columnDefinition = "text")
    @Convert(converter = TranslationMapConverter.class)
    private TranslationMap noteTranslations;

    @Column(name = "created_at")
    @CreationTimestamp
    private Instant createdAt;
}
