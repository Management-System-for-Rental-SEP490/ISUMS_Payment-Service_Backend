package com.isums.paymentservice.domains.entities;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "tariff_config_version")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TariffConfigVersion {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "metric", nullable = false, length = 40)
    private String metric;

    @Column(name = "plan", nullable = false, length = 40)
    private String plan;

    @Column(name = "region", nullable = false, length = 40)
    private String region;

    @Column(name = "version", nullable = false, length = 80)
    private String version;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "config_json", nullable = false, columnDefinition = "jsonb")
    private String configJson;

    @Column(name = "effective_from", nullable = false)
    private Instant effectiveFrom;

    @Column(name = "expired_at")
    private Instant expiredAt;

    @Column(name = "notes", length = 1000)
    private String notes;

    @Column(name = "created_by", nullable = false)
    private UUID createdBy;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "expired_by")
    private UUID expiredBy;
}
