-- Phase 6: Notification-Service PREMIUM subscription support.
--
-- The existing payments_reference_type_check CHECK constraint was
-- emitted by Hibernate at first boot from the original ReferenceType
-- enum (HOUSE/ISSUE/QUOTE/INVOICE/MAINTENANCE/UTILITY/MULTI_INVOICE).
-- ddl-auto=update never updates a CHECK in place — adding SUBSCRIPTION
-- to the Java enum throws DataIntegrityViolationException at insert
-- time. This migration rebuilds the constraint with the full set.
--
-- Defensive: drop only if present (re-running this on a fresh DB where
-- Hibernate emitted the constraint already with SUBSCRIPTION is a no-op
-- after the IF EXISTS branch).
DO $$
DECLARE
    cname text;
BEGIN
    -- Hibernate names this `payments_reference_type_check` by default,
    -- but a dialect upgrade or rebuild could rename it. Match by
    -- conkey (single column = reference_type) on the payments table to
    -- catch every variant.
    FOR cname IN
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        JOIN pg_attribute att ON att.attrelid = rel.oid
                             AND att.attnum = ANY(con.conkey)
        WHERE rel.relname = 'payments'
          AND nsp.nspname = current_schema()
          AND con.contype = 'c'
          AND att.attname = 'reference_type'
    LOOP
        EXECUTE format('ALTER TABLE payments DROP CONSTRAINT %I', cname);
    END LOOP;
END$$;

ALTER TABLE payments
    ADD CONSTRAINT payments_reference_type_check
    CHECK (reference_type::text = ANY (ARRAY[
        'HOUSE',
        'ISSUE',
        'QUOTE',
        'INVOICE',
        'MAINTENANCE',
        'UTILITY',
        'MULTI_INVOICE',
        'SUBSCRIPTION'
    ]::text[]));

COMMENT ON CONSTRAINT payments_reference_type_check ON payments IS
    'Mirrors com.isums.paymentservice.domains.enums.ReferenceType — bump together when adding new payment purposes.';
