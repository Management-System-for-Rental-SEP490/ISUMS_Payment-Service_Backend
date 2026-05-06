package com.isums.paymentservice.infrastructures.repositories;

import com.isums.paymentservice.domains.entities.Payment;
import com.isums.paymentservice.domains.enums.PaymentStatus;
import com.isums.paymentservice.domains.enums.ReferenceType;
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

    /**
     * PENDING payments of a specific reference type for a given owner — used
     * by the Notification PREMIUM upgrade flow to detect a stale checkout
     * before issuing a new one. Sorted newest-first so callers can peek at
     * the most recent attempt and decide whether it's expired.
     */
    List<Payment> findByReferenceIdAndReferenceTypeAndStatusOrderByCreatedAtDesc(
            UUID referenceId, ReferenceType referenceType, PaymentStatus status);
}
