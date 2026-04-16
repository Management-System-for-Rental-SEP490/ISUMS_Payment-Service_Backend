package com.isums.paymentservice.domains.entities;

import com.isums.paymentservice.domains.enums.InvoiceStatus;
import com.isums.paymentservice.domains.enums.InvoiceType;
import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.UuidGenerator;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "rental_invoices", indexes = {
        @Index(name = "idx_invoice_tenant", columnList = "tenant_id,status"),
        @Index(name = "idx_invoice_house", columnList = "house_id"),
        @Index(name = "idx_invoice_contract", columnList = "contract_id,type"),
        @Index(name = "idx_invoice_due", columnList = "due_date,status"),
        @Index(name = "idx_invoice_period", columnList = "contract_id,period_key", unique = true)
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RentalInvoice {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(name = "contract_id")
    private UUID contractId;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "house_id", nullable = false)
    private UUID houseId;

    @Column(name = "quote_id")
    private UUID quoteId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private InvoiceType type;

    @Column(name = "period_key", nullable = false)
    private String periodKey;

    @Column(name = "base_amount")
    private Long baseAmount;

    @Column(name = "service_amount")
    private Long serviceAmount;

    @Column(name = "penalty_amount")
    private Long penaltyAmount;

    @Column(name = "total_amount", nullable = false)
    private Long totalAmount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private InvoiceStatus status;

    @Column(name = "due_date", nullable = false)
    private Instant dueDate;

    @Column(name = "rent_amount")
    private Long rentAmount;

    @Column(name = "pay_date")
    private Integer payDate;

    @Column(name = "contract_start_at")
    private Instant contractStartAt;

    @Column(name = "tenant_email", length = 320)
    private String tenantEmail;

    @Column(name = "is_new_account")
    private Boolean isNewAccount;

    @Column(name = "paid_at")
    private Instant paidAt;

    @Column(name = "refund_payment_method", length = 20)
    private String refundPaymentMethod;

    @Column(name = "refund_note", columnDefinition = "text")
    private String refundNote;

    @Column(name = "overdue_days")
    private Integer overdueDays;

    @Column(name = "power_cut_warned_at")
    private Instant powerCutWarnedAt;

    @Column(name = "power_cut_confirmed_at")
    private Instant powerCutConfirmedAt;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private Instant updatedAt;
}