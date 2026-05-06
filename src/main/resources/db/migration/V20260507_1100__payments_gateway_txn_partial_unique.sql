DO $$
DECLARE
    legacy_index text;
BEGIN
    FOR legacy_index IN
        SELECT i.indexname
        FROM pg_indexes i
        WHERE i.schemaname = current_schema()
          AND i.tablename = 'payments'
          AND i.indexdef ILIKE '%UNIQUE%gateway_txn_id%'
          AND i.indexname <> 'uk_payments_gateway_txn_id'
    LOOP
        EXECUTE format('DROP INDEX IF EXISTS %I', legacy_index);
    END LOOP;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uk_payments_gateway_txn_id
    ON payments(gateway_txn_id)
    WHERE gateway_txn_id IS NOT NULL AND gateway_txn_id <> '0';
