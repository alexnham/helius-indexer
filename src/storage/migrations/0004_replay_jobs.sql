-- Bridge unpublished replay prototypes whose versions 2/3 predate the merged Kafka migrations.
-- Replay position for live input produced to Kafka: the slot of the newest message
-- that, with every earlier one, the broker confirmed. Updates only move it forward.
CREATE TABLE IF NOT EXISTS kafka_producer_cursor (
    stream TEXT PRIMARY KEY,
    slot BIGINT NOT NULL
);
-- Consumer positions: the next Kafka offset to read per group and partition. They are
-- written in the same transaction as the output they cover and only move forward.
CREATE TABLE IF NOT EXISTS kafka_offsets (
    consumer_group TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    next_offset BIGINT NOT NULL,
    PRIMARY KEY (consumer_group, topic, partition)
);
-- Job progress survives transfer/raw pruning. No range-completion flag: filtered
-- replay observations are not evidence that every transaction in a slot arrived.
CREATE TABLE IF NOT EXISTS replay_jobs (
    id TEXT PRIMARY KEY,
    mint TEXT NOT NULL,
    start_slot BIGINT NOT NULL CHECK (start_slot >= 0),
    end_slot BIGINT NOT NULL CHECK (end_slot >= start_slot),
    decoder TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    grants BIGINT NOT NULL DEFAULT 1 CHECK (grants BETWEEN 1 AND 64),
    seconds_charged BIGINT NOT NULL DEFAULT 0 CHECK (seconds_charged >= 0),
    connections_used BIGINT NOT NULL DEFAULT 0 CHECK (connections_used >= 0),
    messages_used BIGINT NOT NULL DEFAULT 0 CHECK (messages_used >= 0),
    bytes_used BIGINT NOT NULL DEFAULT 0 CHECK (bytes_used >= 0),
    retry_cursor BYTEA
);
CREATE TABLE IF NOT EXISTS replay_attempts (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job TEXT NOT NULL REFERENCES replay_jobs(id),
    started_at BIGINT NOT NULL,
    reserved_seconds BIGINT NOT NULL CHECK (reserved_seconds BETWEEN 1 AND 60),
    status TEXT NOT NULL,
    first_available BIGINT,
    reference_slot BIGINT,
    observed_at BIGINT,
    end_observed BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS replay_attempts_job ON replay_attempts(job,id);
-- raw_id deliberately has no cascading foreign key: pruning raw bytes must not
-- delete a processing receipt or turn it into proof of retained output.
CREATE TABLE IF NOT EXISTS replay_receipts (
    job TEXT NOT NULL REFERENCES replay_jobs(id),
    signature BYTEA NOT NULL CHECK (octet_length(signature)=64),
    input_hash BYTEA NOT NULL CHECK (octet_length(input_hash)=32),
    slot BIGINT NOT NULL CHECK (slot >= 0),
    raw_id BIGINT NOT NULL,
    status TEXT NOT NULL,
    first_received BIGINT NOT NULL,
    PRIMARY KEY(job,signature)
);
CREATE INDEX IF NOT EXISTS replay_receipts_pending ON replay_receipts(job,status);
