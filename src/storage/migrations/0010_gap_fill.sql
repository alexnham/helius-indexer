-- Bounded RPC fill of the measured interval between the archive tip and the replay
-- window. Its allowance is separate from replay's and archive's; batches share the
-- archive attempt log, distinguished by lane.
ALTER TABLE replay_jobs
    ADD COLUMN IF NOT EXISTS gap_grants BIGINT NOT NULL DEFAULT 1 CHECK (gap_grants BETWEEN 1 AND 64),
    ADD COLUMN IF NOT EXISTS gap_batches_charged BIGINT NOT NULL DEFAULT 0 CHECK (gap_batches_charged >= 0);
ALTER TABLE archive_attempts
    ADD COLUMN IF NOT EXISTS lane TEXT NOT NULL DEFAULT 'archive' CHECK (lane IN ('archive','gap'));
