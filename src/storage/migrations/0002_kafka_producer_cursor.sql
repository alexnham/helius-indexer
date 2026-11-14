-- Replay position for live input produced to Kafka: the slot of the newest message
-- that, with every earlier one, the broker confirmed. Updates only move it forward.
CREATE TABLE kafka_producer_cursor (
    stream TEXT PRIMARY KEY,
    slot BIGINT NOT NULL
);
