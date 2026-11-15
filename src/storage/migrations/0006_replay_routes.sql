-- Append-only source measurements per job. NULL availability means unknown or not
-- probed, never zero. Routed segments say which source may serve a slot; they are
-- not processing progress and not coverage.
CREATE TABLE IF NOT EXISTS replay_routes (
    job TEXT NOT NULL REFERENCES replay_jobs(id),
    revision BIGINT NOT NULL CHECK (revision >= 1),
    observed_at BIGINT NOT NULL,
    replay_first BIGINT CHECK (replay_first >= 0),
    reference_slot BIGINT NOT NULL CHECK (reference_slot >= 0),
    archive_probed BOOLEAN NOT NULL,
    archive_last BIGINT CHECK (archive_last >= 0),
    archive_error TEXT,
    PRIMARY KEY (job, revision)
);
CREATE TABLE IF NOT EXISTS replay_route_segments (
    job TEXT NOT NULL,
    revision BIGINT NOT NULL,
    start_slot BIGINT NOT NULL CHECK (start_slot >= 0),
    end_slot BIGINT NOT NULL CHECK (end_slot >= start_slot),
    kind TEXT NOT NULL CHECK (kind IN ('archive','replay','overlap','uncovered')),
    PRIMARY KEY (job, revision, start_slot),
    FOREIGN KEY (job, revision) REFERENCES replay_routes(job, revision)
);
