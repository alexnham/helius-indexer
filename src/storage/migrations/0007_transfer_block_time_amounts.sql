-- Block time and exact amounts become SQL columns so queries can apply a time cutoff and
-- aggregate in the database. record_json stays the serialized source of truth; the columns
-- come from the same record. NULL means unknown, never zero. Existing rows are backfilled
-- from record_json; the nullable ADD COLUMN itself is metadata-only.
ALTER TABLE transfers
    ADD COLUMN IF NOT EXISTS block_time BIGINT,
    ADD COLUMN IF NOT EXISTS amount_raw NUMERIC(20,0),
    ADD COLUMN IF NOT EXISTS fee_raw NUMERIC(20,0);
UPDATE transfers SET
    block_time=(record_json::jsonb->>'block_time')::bigint,
    amount_raw=(record_json::jsonb->>'amount_raw')::numeric,
    fee_raw=(record_json::jsonb->>'fee_raw')::numeric;
-- Serves mint-scoped time cutoffs; partitions created later inherit this index.
CREATE INDEX IF NOT EXISTS transfers_mint_block_time ON transfers(mint, block_time);
