package com.isums.paymentservice.infrastructures.repositories;

import com.isums.paymentservice.domains.entities.RentalInvoice;
import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import org.springframework.data.jpa.repository.JpaRepository;

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

}
