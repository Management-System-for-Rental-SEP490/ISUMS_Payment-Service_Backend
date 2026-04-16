package com.isums.paymentservice.infrastructures.repositories;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
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

    Optional<RentalInvoice> findByContractIdAndPeriodKey(UUID contractId, String periodKey);

    @Query("""
            SELECT r FROM RentalInvoice r
            WHERE r.type = 'MONTHLY_RENT'
            AND r.status = 'UNPAID'
            AND r.dueDate < :now
            """)
    List<RentalInvoice> findOverdueMonthlyRentInvoices(@Param("now") Instant now);
}
