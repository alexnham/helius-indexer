-- Preserve version 2 jobs and charged budgets; provenance absent in the
-- prototype remains unknown until the input is processed again.
ALTER TABLE live_raw ADD COLUMN IF NOT EXISTS ingestion_mode TEXT NOT NULL DEFAULT 'live' CHECK (ingestion_mode IN ('live','replay'));
CREATE INDEX IF NOT EXISTS live_raw_mode_received ON live_raw(ingestion_mode,received_at);
UPDATE live_raw SET ingestion_mode='replay' WHERE EXISTS (SELECT 1 FROM replay_receipts r WHERE r.raw_id=live_raw.id);
ALTER TABLE replay_receipts
    ADD COLUMN IF NOT EXISTS normalized_hash BYTEA CHECK (octet_length(normalized_hash)=32),
    ADD COLUMN IF NOT EXISTS input_attempt BIGINT REFERENCES replay_attempts(id),
    ADD COLUMN IF NOT EXISTS processing_attempts BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS processed_at BIGINT;
