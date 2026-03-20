package domains.entities;

import domains.enums.InvoiceStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.annotations.UuidGenerator;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "rental_invoices", indexes = {
        @Index(name = "idx_invoice_tenant", columnList = "tenant_id"),
        @Index(name = "idx_invoice_contract_status", columnList = "contract_id, status"),
        @Index(name = "idx_invoice_due_date", columnList = "due_date"),
        @Index(name = "idx_invoice_period", columnList = "contract_id, period_key", unique = true)
})
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class RentalInvoice {

    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(name = "tenant_id")
    private UUID tenantId;

    @Column(name = "house_id", nullable = false)
    private UUID houseId;

    @Column(name = "contract_id", nullable = false)
    private UUID contractId;

    @Column(name = "period_key", nullable = false)
    private String periodKey;

    @Column(name = "base_amount")
    private Long baseAmount;

    @Column(name = "service_amount")
    private Long serviceAmount;

    @Column(name = "penalty_amount")
    private Long penaltyAmount;

    @Column(name = "total_amount")
    private Long totalAmount;

    @Column(name = "due_date")
    private Instant dueDate;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private InvoiceStatus status;

    @Column(name = "paid_at")
    private Instant paidAt;

    @Column(name = "created_at")
    @CreationTimestamp
    private Instant createdAt;

    @Column(name = "updated_at")
    @UpdateTimestamp
    private Instant updatedAt;
}
