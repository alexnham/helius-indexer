-- Append-only archive/replay reconciliation receipts per routed overlap slot. A receipt
-- is evidence for its route and decoder revision only, and it survives pruning;
-- verified coverage additionally needs the archive slot evidence still retained.
CREATE TABLE IF NOT EXISTS replay_overlap_outcomes (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job TEXT NOT NULL,
    route_revision BIGINT NOT NULL,
    slot BIGINT NOT NULL CHECK (slot >= 0),
    decoder TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('consistent','replay_omission','conflict','pending')),
    reason TEXT,
    replay_inputs BIGINT NOT NULL CHECK (replay_inputs >= 0),
    archive_mint_inputs BIGINT NOT NULL CHECK (archive_mint_inputs >= 0),
    observed_at BIGINT NOT NULL,
    FOREIGN KEY (job, route_revision) REFERENCES replay_routes(job, revision)
);
CREATE INDEX IF NOT EXISTS replay_overlap_outcomes_slot ON replay_overlap_outcomes(job, route_revision, slot, id);
