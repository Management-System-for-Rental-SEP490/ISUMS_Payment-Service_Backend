package com.isums.paymentservice.domains.entities;

import com.isums.common.i18n.TranslationMap;
import com.isums.common.i18n.TranslationMapConverter;
import com.isums.paymentservice.domains.enums.PaymentMethod;
import com.isums.paymentservice.domains.enums.PaymentStatus;
import com.isums.paymentservice.domains.enums.ReferenceType;
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
@Table(name = "payments", indexes = {
        @Index(name = "idx_payments_reference", columnList = "reference_id, reference_type"),
        @Index(name = "idx_payments_tenant", columnList = "tenant_id"),
        @Index(name = "idx_payments_payer", columnList = "payer_user_id"),
        @Index(name = "idx_payments_status", columnList = "status"),
        @Index(name = "idx_payments_created_at", columnList = "created_at")
})
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class Payment {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(name = "reference_id")
    private UUID referenceId;

    @Enumerated(EnumType.STRING)
    @Column(name = "reference_type", nullable = false)
    private ReferenceType referenceType;

    @Column(name = "tenant_id")
    private UUID tenantId;

    @Column(name = "payer_user_id", nullable = false)
    private UUID payerUserId;

    @Column(name = "amount", nullable = false)
    private long amount;

    @Enumerated(EnumType.STRING)
    @Column(name = "method", nullable = false)
    private PaymentMethod method;

    @Column(name = "invoice_ids", columnDefinition = "TEXT")
    private String invoiceIds;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private PaymentStatus status;

    @Column(name = "gateway_txn_id")
    private String gatewayTxnId;

    @Column(name = "gateway_response", columnDefinition = "text")
    private String gatewayResponse;

    private String note;

    @Column(name = "note_translations", columnDefinition = "text")
    @Convert(converter = TranslationMapConverter.class)
    private TranslationMap noteTranslations;

    @Column(name = "paid_at")
    private Instant paidAt;

    @Column(name = "created_at")
    @CreationTimestamp
    private Instant createdAt;
}
