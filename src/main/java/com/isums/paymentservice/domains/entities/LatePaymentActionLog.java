package com.isums.paymentservice.domains.entities;

import com.isums.paymentservice.domains.enums.LatePaymentAction;
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
@Table(name = "late_payment_action_logs",
        indexes = @Index(columnList = "invoice_id, action_type", unique = true))
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LatePaymentActionLog {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(name = "invoice_id", nullable = false)
    private UUID invoiceId;

    @Column(name = "contract_id", nullable = false)
    private UUID contractId;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Enumerated(EnumType.STRING)
    @Column(name = "action_type", nullable = false)
    private LatePaymentAction actionType;

    @CreationTimestamp
    private Instant executedAt;
}
