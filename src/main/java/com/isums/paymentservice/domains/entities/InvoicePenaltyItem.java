package com.isums.paymentservice.domains.entities;

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
@Table(name = "invoice_penalty_items", indexes = {
        @Index(name = "idx_penalty_item_invoice", columnList = "invoice_id"),
        @Index(name = "idx_penalty_item_policy", columnList = "policy_id")
})
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class InvoicePenaltyItem {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(name = "invoice_id", nullable = false)
    private UUID invoiceId;

    @Column(name = "policy_id", nullable = false)
    private UUID policyId;

    @Column(name = "days_overdue")
    private int daysOverdue;

    @Column(name = "calc_base_amount")
    private long calcBaseAmount;

    @Column(name = "penalty_amount")
    private long penaltyAmount;

    private String description;

    @CreationTimestamp
    @Column(name = "created_at")
    private Instant createdAt;
}
