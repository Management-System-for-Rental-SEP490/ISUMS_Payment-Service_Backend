package com.isums.paymentservice.infrastructures.repositories;

import com.isums.paymentservice.domains.entities.TariffConfigVersion;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TariffConfigVersionRepository
        extends JpaRepository<TariffConfigVersion, UUID> {

    @Query("""
        SELECT t FROM TariffConfigVersion t
        WHERE t.metric = :metric
          AND t.plan = :plan
          AND t.region = :region
          AND t.expiredAt IS NULL
        ORDER BY t.effectiveFrom DESC
    """)
    Optional<TariffConfigVersion> findActive(
            @Param("metric") String metric,
            @Param("plan") String plan,
            @Param("region") String region);

    @Query("""
        SELECT t FROM TariffConfigVersion t
        WHERE t.metric = :metric
          AND t.plan = :plan
          AND t.region = :region
        ORDER BY t.effectiveFrom DESC
    """)
    List<TariffConfigVersion> findHistory(
            @Param("metric") String metric,
            @Param("plan") String plan,
            @Param("region") String region);
}
