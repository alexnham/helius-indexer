-- Consumer positions: the next Kafka offset to read per group and partition. They are
-- written in the same transaction as the output they cover and only move forward.
CREATE TABLE kafka_offsets (
    consumer_group TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    next_offset BIGINT NOT NULL,
    PRIMARY KEY (consumer_group, topic, partition)
);
