package com.isums.paymentservice.infrastructures.repositories;

import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.domains.enums.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findByReferenceId(UUID referenceId);

    Optional<Payment> findByGatewayTxnId(String gatewayTxnId);

    boolean existsByReferenceIdAndStatus(UUID referenceId, PaymentStatus status);

    Optional<Payment> findByReferenceIdAndStatus(UUID referenceId, PaymentStatus status);
}
