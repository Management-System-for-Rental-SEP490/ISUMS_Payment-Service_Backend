package com.isums.paymentservice.infrastructures.repositories;

import com.isums.paymentservice.domains.dtos.finance.projections.HouseAggregateProjection;
import com.isums.paymentservice.domains.dtos.finance.projections.MonthlyTotalProjection;
import com.isums.paymentservice.domains.dtos.finance.projections.TypeAmountProjection;
import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface RentalInvoiceRepository extends JpaRepository<RentalInvoice, UUID> {

    List<RentalInvoice> findByTenantIdOrderByDueDateAsc(UUID tenantId);

    boolean existsByContractIdAndPeriodKey(UUID contractId, String periodKey);

    boolean existsByHouseIdAndTenantIdAndTypeAndStatus(UUID houseId, UUID tenantId, InvoiceType type, InvoiceStatus status);

    Optional<RentalInvoice> findFirstByHouseIdAndTenantIdAndStatus(UUID houseId, UUID tenantId, InvoiceStatus status);

    List<RentalInvoice> findByTenantIdAndHouseIdOrderByDueDateAsc(UUID tenantId, UUID houseId);

    Optional<RentalInvoice> findByContractIdAndType(UUID contractId, InvoiceType type);

    Optional<RentalInvoice> findFirstByContractIdAndTypeOrderByDueDateAsc(UUID contractId, InvoiceType type);

    Optional<RentalInvoice> findByContractIdAndPeriodKey(UUID contractId, String periodKey);

    List<RentalInvoice> findByContractIdAndStatus(UUID contractId, InvoiceStatus status);

    List<RentalInvoice> findByTypeAndStatusAndPaidAtAfter(InvoiceType type, InvoiceStatus status, Instant paidAtAfter);

    @Query("""
            SELECT r FROM RentalInvoice r
            WHERE r.type = 'MONTHLY_RENT'
            AND r.status = 'UNPAID'
            AND r.dueDate < :now
            """)
    List<RentalInvoice> findOverdueMonthlyRentInvoices(@Param("now") Instant now);

    @Query(value = """
            SELECT CAST(type AS TEXT) AS type, COALESCE(SUM(total_amount), 0) AS amount
            FROM rental_invoices
            WHERE status = 'PAID'
              AND type IN (:types)
              AND paid_at >= :from
              AND paid_at < :to
              AND (CAST(:scoped AS BOOLEAN) = FALSE OR house_id IN (:houseIds))
            GROUP BY type
            """, nativeQuery = true)
    List<TypeAmountProjection> aggregatePaidByType(
            @Param("types") Collection<String> types,
            @Param("from") Instant from,
            @Param("to") Instant to,
            @Param("scoped") boolean scoped,
            @Param("houseIds") Collection<UUID> houseIds);

    @Query(value = """
            SELECT TO_CHAR(date_trunc('month', paid_at AT TIME ZONE 'Asia/Ho_Chi_Minh'), 'YYYY-MM') AS month,
                   CAST(type AS TEXT) AS type,
                   COALESCE(SUM(total_amount), 0) AS amount
            FROM rental_invoices
            WHERE status = 'PAID'
              AND type IN (:types)
              AND paid_at >= :from
              AND paid_at < :to
              AND (CAST(:scoped AS BOOLEAN) = FALSE OR house_id IN (:houseIds))
            GROUP BY month, type
            ORDER BY month
            """, nativeQuery = true)
    List<MonthlyTotalProjection> aggregateMonthlyByType(
            @Param("types") Collection<String> types,
            @Param("from") Instant from,
            @Param("to") Instant to,
            @Param("scoped") boolean scoped,
            @Param("houseIds") Collection<UUID> houseIds);

    @Query(value = """
            SELECT house_id AS houseId,
                   COALESCE(SUM(CASE WHEN type IN ('MONTHLY_RENT','UTILITY','PENALTY')
                                     THEN total_amount ELSE 0 END), 0) AS revenue,
                   COALESCE(SUM(CASE WHEN type IN ('MAINTENANCE','ISSUE','DEPOSIT_REFUND')
                                     THEN total_amount ELSE 0 END), 0) AS expense
            FROM rental_invoices
            WHERE status = 'PAID'
              AND paid_at >= :from
              AND paid_at < :to
              AND (CAST(:scoped AS BOOLEAN) = FALSE OR house_id IN (:houseIds))
            GROUP BY house_id
            ORDER BY (COALESCE(SUM(CASE WHEN type IN ('MONTHLY_RENT','UTILITY','PENALTY')
                                       THEN total_amount ELSE 0 END), 0)
                    - COALESCE(SUM(CASE WHEN type IN ('MAINTENANCE','ISSUE','DEPOSIT_REFUND')
                                       THEN total_amount ELSE 0 END), 0)) DESC
            """, nativeQuery = true)
    List<HouseAggregateProjection> aggregateByHouse(
            @Param("from") Instant from,
            @Param("to") Instant to,
            @Param("scoped") boolean scoped,
            @Param("houseIds") Collection<UUID> houseIds,
            Pageable pageable);

    @Query("""
            SELECT r FROM RentalInvoice r
            WHERE r.status = 'PAID'
              AND r.paidAt IS NOT NULL
              AND r.paidAt >= :from
              AND r.paidAt < :to
              AND (:scoped = FALSE OR r.houseId IN :houseIds)
            ORDER BY r.paidAt DESC
            """)
    List<RentalInvoice> findRecentPaid(
            @Param("from") Instant from,
            @Param("to") Instant to,
            @Param("scoped") boolean scoped,
            @Param("houseIds") Collection<UUID> houseIds,
            Pageable pageable);

    @Query("""
            SELECT r FROM RentalInvoice r
            WHERE r.status IN ('UNPAID', 'OVERDUE')
              AND r.dueDate < :now
              AND (:scoped = FALSE OR r.houseId IN :houseIds)
            ORDER BY r.dueDate ASC
            """)
    List<RentalInvoice> findOutstanding(
            @Param("now") Instant now,
            @Param("scoped") boolean scoped,
            @Param("houseIds") Collection<UUID> houseIds,
            Pageable pageable);

    @Query("""
            SELECT COALESCE(SUM(r.totalAmount), 0)
            FROM RentalInvoice r
            WHERE r.status IN ('UNPAID', 'OVERDUE')
              AND r.dueDate < :now
              AND (:scoped = FALSE OR r.houseId IN :houseIds)
            """)
    Long sumOutstandingAmount(
            @Param("now") Instant now,
            @Param("scoped") boolean scoped,
            @Param("houseIds") Collection<UUID> houseIds);

    @Query("""
            SELECT COUNT(r)
            FROM RentalInvoice r
            WHERE r.status IN ('UNPAID', 'OVERDUE')
              AND r.dueDate < :now
              AND (:scoped = FALSE OR r.houseId IN :houseIds)
            """)
    long countOutstanding(
            @Param("now") Instant now,
            @Param("scoped") boolean scoped,
            @Param("houseIds") Collection<UUID> houseIds);
}
