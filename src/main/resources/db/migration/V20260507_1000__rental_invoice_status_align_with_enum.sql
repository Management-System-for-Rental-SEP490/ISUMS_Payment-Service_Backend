ALTER TABLE rental_invoices DROP CONSTRAINT IF EXISTS rental_invoices_status_check;

ALTER TABLE rental_invoices ADD CONSTRAINT rental_invoices_status_check
    CHECK (status IN ('UNPAID','PAID','OVERDUE','CANCELLED','TRANSFERRED','FORFEITED','REFUNDED'));
