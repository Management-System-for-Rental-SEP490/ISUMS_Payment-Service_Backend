ALTER TABLE rental_invoices
    ADD COLUMN IF NOT EXISTS relocation_source_contract_id uuid;

CREATE INDEX IF NOT EXISTS idx_rental_invoices_relocation_source
    ON rental_invoices(relocation_source_contract_id)
    WHERE relocation_source_contract_id IS NOT NULL;
