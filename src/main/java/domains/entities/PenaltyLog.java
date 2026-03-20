package domains.entities;

import domains.enums.ActorType;
import domains.enums.EventType;
import domains.enums.LogStatus;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UuidGenerator;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "penalty_logs", indexes = {
        @Index(name = "idx_penalty_log_payment", columnList = "payment_id"),
        @Index(name = "idx_penalty_log_invoice", columnList = "invoice_id")
})
@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
public class PenaltyLog {
    @Id
    @GeneratedValue
    @UuidGenerator
    private UUID id;

    @Column(name = "payment_id")
    private UUID paymentId;

    @Column(name = "invoice_id")
    private UUID invoiceId;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private EventType type;

    @Column(name = "new_status", nullable = false)
    @Enumerated(EnumType.STRING)
    private LogStatus newStatus;

    @Column(name = "old_status", nullable = false)
    @Enumerated(EnumType.STRING)
    private LogStatus oldStatus;

    @Column(name = "actor_id")
    private UUID actorId;

    @Column(name = "actor_type", nullable = false)
    @Enumerated(EnumType.STRING)
    private ActorType actorType;

    @Column(name = "meta_data", columnDefinition = "text")
    private String metaData;

    @Column(name = "created_at")
    @CreationTimestamp
    private Instant createdAt;
}
