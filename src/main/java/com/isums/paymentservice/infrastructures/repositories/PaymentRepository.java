package com.isums.paymentservice.infrastructures.repositories;

import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.domains.enums.PaymentStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findByReferenceId(UUID referenceId);

    Optional<Payment> findByGatewayTxnId(String gatewayTxnId);

    boolean existsByReferenceIdAndStatus(UUID referenceId, PaymentStatus status);

    Optional<Payment> findByReferenceIdAndStatus(UUID referenceId, PaymentStatus status);

    List<Payment> findByReferenceIdOrderByCreatedAtDesc(UUID referenceId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Payment p WHERE p.id = :id")
    Optional<Payment> findByIdForUpdate(@Param("id") UUID id);
}
