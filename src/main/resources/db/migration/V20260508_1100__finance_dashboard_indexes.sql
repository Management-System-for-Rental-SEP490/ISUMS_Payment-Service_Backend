CREATE INDEX IF NOT EXISTS idx_invoice_paid_status_type
    ON rental_invoices(status, type, paid_at DESC)
    WHERE status = 'PAID';

CREATE INDEX IF NOT EXISTS idx_invoice_house_paidat
    ON rental_invoices(house_id, paid_at DESC)
    WHERE status = 'PAID';

CREATE INDEX IF NOT EXISTS idx_invoice_unpaid_due
    ON rental_invoices(due_date, status)
    WHERE status IN ('UNPAID', 'OVERDUE');

CREATE INDEX IF NOT EXISTS idx_invoice_house_status_type
    ON rental_invoices(house_id, status, type);
