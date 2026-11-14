-- PostgreSQL equivalent of the previous SQLite schema. Slots, times and counters are
-- BIGINT; raw inputs stay as exact bytes (BYTEA) or exact text, never JSONB, so equality
-- and replay use the bytes that arrived. Amounts and fees live only inside record_json
-- text, decoded in Rust as u64 and aggregated as u128.

CREATE TABLE captures (
    signature TEXT PRIMARY KEY,
    slot BIGINT NOT NULL,
    result_json TEXT NOT NULL,
    capture_json BYTEA NOT NULL,
    status TEXT NOT NULL,
    decoder_version TEXT NOT NULL
);
CREATE INDEX captures_slot ON captures(slot);

-- Every distinct observation of a transaction. The digest identifies the exact
-- result text; large JSON cannot be a B-tree key itself.
CREATE TABLE capture_inputs (
    signature TEXT NOT NULL,
    source TEXT NOT NULL,
    result_digest BYTEA NOT NULL,
    slot BIGINT NOT NULL,
    result_json TEXT NOT NULL,
    capture_json BYTEA NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (signature, source, result_digest)
);
CREATE INDEX capture_inputs_slot ON capture_inputs(slot);

-- Range partitions of PARTITION_SLOTS slots are created on demand before writes.
-- transfer_id embeds the signature, which belongs to exactly one slot, so
-- (slot, transfer_id) is as unique as transfer_id alone.
CREATE TABLE transfers (
    transfer_id TEXT NOT NULL,
    signature TEXT NOT NULL,
    slot BIGINT NOT NULL,
    outer_index INTEGER NOT NULL,
    inner_index INTEGER NOT NULL,
    mint TEXT NOT NULL,
    record_json TEXT NOT NULL,
    PRIMARY KEY (slot, transfer_id)
) PARTITION BY RANGE (slot);
CREATE INDEX transfers_mint_order ON transfers(mint, slot, signature, outer_index, inner_index);
CREATE INDEX transfers_order ON transfers(slot, signature, outer_index, inner_index);
CREATE INDEX transfers_signature ON transfers(signature);

CREATE TABLE decoder_repairs (
    signature TEXT NOT NULL,
    target_decoder TEXT NOT NULL,
    slot BIGINT NOT NULL,
    previous_decoder TEXT NOT NULL,
    status TEXT NOT NULL,
    error_class TEXT,
    attempts BIGINT NOT NULL,
    first_pending BIGINT NOT NULL,
    PRIMARY KEY (signature, target_decoder)
);
CREATE INDEX decoder_repairs_slot ON decoder_repairs(target_decoder, slot);

CREATE TABLE live_raw (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slot BIGINT NOT NULL,
    protobuf BYTEA NOT NULL,
    proto_schema TEXT NOT NULL DEFAULT 'laserstream-core-proto/14.0.0',
    status TEXT NOT NULL,
    received_at BIGINT NOT NULL
);
CREATE INDEX live_raw_received ON live_raw(received_at);
CREATE INDEX live_raw_unresolved ON live_raw(id) WHERE status <> 'decoded';
CREATE INDEX live_raw_slot ON live_raw(slot);

CREATE TABLE live_retry_cursor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    last_id BIGINT NOT NULL
);

CREATE TABLE live_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    from_slot BIGINT NOT NULL,
    mint TEXT NOT NULL,
    decoder TEXT NOT NULL,
    status TEXT NOT NULL,
    reference_slot BIGINT,
    reference_at BIGINT,
    replay_first BIGINT,
    replay_at BIGINT
);

CREATE TABLE historical_slots (
    mint TEXT NOT NULL,
    decoder TEXT NOT NULL,
    slot BIGINT NOT NULL,
    status TEXT NOT NULL,
    error_class TEXT,
    first_pending BIGINT NOT NULL,
    attempts BIGINT NOT NULL DEFAULT 0,
    block_time BIGINT,
    PRIMARY KEY (mint, decoder, slot)
);
CREATE INDEX historical_slots_slot ON historical_slots(slot);

-- produced_block records, at write time, whether the response contained a block, so
-- skip evidence never needs to parse stored bytes.
CREATE TABLE historical_inputs (
    mint TEXT NOT NULL,
    decoder TEXT NOT NULL,
    slot BIGINT NOT NULL,
    attempt BIGINT NOT NULL,
    source_kind TEXT NOT NULL,
    received_at BIGINT NOT NULL,
    produced_block BOOLEAN NOT NULL,
    response BYTEA NOT NULL,
    PRIMARY KEY (mint, decoder, slot, attempt)
);
CREATE INDEX historical_inputs_lookup ON historical_inputs(mint, slot, source_kind, received_at, attempt);
CREATE INDEX historical_inputs_slot ON historical_inputs(slot) WHERE produced_block;

CREATE TABLE historical_source_slots (
    mint TEXT NOT NULL,
    decoder TEXT NOT NULL,
    slot BIGINT NOT NULL,
    source_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    PRIMARY KEY (mint, decoder, slot, source_kind)
);
CREATE INDEX historical_source_slots_slot ON historical_source_slots(slot);

CREATE TABLE historical_block_identity (
    slot BIGINT PRIMARY KEY,
    blockhash TEXT NOT NULL,
    parent_slot BIGINT NOT NULL,
    previous_blockhash TEXT NOT NULL,
    transaction_ids_hash BYTEA NOT NULL,
    block_time BIGINT
);
CREATE INDEX historical_block_identity_parent ON historical_block_identity(parent_slot);

CREATE TABLE bridge_overlap_proofs (
    mint TEXT NOT NULL,
    decoder TEXT NOT NULL,
    slot BIGINT NOT NULL,
    verified_attempt BIGINT NOT NULL,
    PRIMARY KEY (mint, decoder, slot)
);

CREATE TABLE bridge_plans (
    mint TEXT NOT NULL,
    decoder TEXT NOT NULL,
    start BIGINT NOT NULL,
    archive_tip BIGINT NOT NULL,
    reference_slot BIGINT NOT NULL,
    snapshot TEXT NOT NULL,
    next_slot BIGINT NOT NULL,
    PRIMARY KEY (mint, decoder, start)
);
