package com.isums.paymentservice.domains.entities;

import com.isums.paymentservice.domains.enums.PolicyType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.UuidGenerator;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "penalty_policies", indexes = {
        @Index(name = "idx_policy_tenant_active", columnList = "tenant_id, is_active"),
        @Index(name = "idx_policy_contract", columnList = "contract_id")
})
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class PenaltyPolicy {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(name = "tenant_id")
    private UUID tenantId;

    @Column(name = "contract_id")
    private UUID contractId;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private PolicyType type;

    @Column(precision = 5, scale = 4)
    private BigDecimal rate;

    @Column(name = "calc_method")
    private String calcMethod;

    @Column(name = "grace_period_day")
    private int gracePeriodDay;

    @Column(name = "max_penalty_rate")
    private int maxPenaltyRate;

    @Column(name = "is_active")
    private boolean active;

    @Column(name = "created_at")
    @CreationTimestamp
    private Instant createdAt;

    @Column(name = "updated_at")
    @UpdateTimestamp
    private Instant updatedAt;
}
