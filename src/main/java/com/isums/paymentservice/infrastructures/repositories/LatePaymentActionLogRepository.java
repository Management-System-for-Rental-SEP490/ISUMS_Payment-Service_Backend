package com.isums.paymentservice.infrastructures.repositories;

import com.isums.paymentservice.domains.entities.LatePaymentActionLog;
import com.isums.paymentservice.domains.enums.LatePaymentAction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface LatePaymentActionLogRepository extends JpaRepository<LatePaymentActionLog, UUID> {

    boolean existsByInvoiceIdAndActionType(UUID invoiceId, LatePaymentAction actionType);

    boolean existsByContractIdAndActionType(UUID contractId, LatePaymentAction actionType);
}