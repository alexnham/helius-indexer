-- A pruned response keeps the block header, every transaction's signatures and the full
-- transactions that touch the row's mint; unrelated transactions are fetched again from
-- the source if a mint-filter correction needs them. Existing rows stay full (false).
-- Rollback: refetch pruned slots that need full bytes, then drop the column.
ALTER TABLE historical_inputs ADD COLUMN pruned BOOLEAN NOT NULL DEFAULT false;
