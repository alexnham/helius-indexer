-- Archive work for a routed job has its own allowance, separate from replay's.
-- A batch is charged before any cache or network work; crashes keep the charge.
ALTER TABLE replay_jobs
    ADD COLUMN IF NOT EXISTS archive_grants BIGINT NOT NULL DEFAULT 1 CHECK (archive_grants BETWEEN 1 AND 64),
    ADD COLUMN IF NOT EXISTS archive_batches_charged BIGINT NOT NULL DEFAULT 0 CHECK (archive_batches_charged >= 0);
-- The limits each batch ran under are stored with it, so later default changes
-- do not rewrite history. Observed counts are diagnostics, not provider billing.
CREATE TABLE IF NOT EXISTS archive_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job TEXT NOT NULL,
    route_revision BIGINT NOT NULL,
    start_slot BIGINT NOT NULL CHECK (start_slot >= 0),
    end_slot BIGINT NOT NULL CHECK (end_slot >= start_slot),
    started_at BIGINT NOT NULL,
    status TEXT NOT NULL,
    request_limit BIGINT NOT NULL,
    byte_limit BIGINT NOT NULL,
    second_limit BIGINT NOT NULL,
    requests BIGINT,
    response_bytes BIGINT,
    cached_chunks BIGINT,
    FOREIGN KEY (job, route_revision) REFERENCES replay_routes(job, revision)
);
CREATE INDEX IF NOT EXISTS archive_attempts_job ON archive_attempts(job, id);
