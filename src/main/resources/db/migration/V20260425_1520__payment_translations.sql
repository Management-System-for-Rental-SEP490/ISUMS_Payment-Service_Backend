-- Phase 5a i18n: per-locale translation maps for payment text fields.
ALTER TABLE payments
    ADD COLUMN IF NOT EXISTS note_translations TEXT;

ALTER TABLE invoice_penalty_items
    ADD COLUMN IF NOT EXISTS description_translations TEXT;

ALTER TABLE payment_escalations
    ADD COLUMN IF NOT EXISTS note_translations TEXT;

COMMENT ON COLUMN payments.note_translations IS
    'JSON map of locale -> translated payment note. Reserved keys: _source, _auto.';
COMMENT ON COLUMN invoice_penalty_items.description_translations IS
    'JSON map of locale -> translated penalty description.';
COMMENT ON COLUMN payment_escalations.note_translations IS
    'JSON map of locale -> translated escalation note.';
