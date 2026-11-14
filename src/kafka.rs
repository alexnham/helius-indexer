//! Kafka as the durable live intake log (#36). Until compressed object storage holds
//! replay inputs (#40), the topic itself is the raw retention, so it keeps 37 days with
//! no byte cap; watch broker disk usage instead of letting Kafka delete early.
use crate::{Error, Result};
use rdkafka::{
    ClientConfig, ClientContext,
    admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication},
    client::DefaultClientContext,
    error::{KafkaError, RDKafkaErrorCode},
    message::{Header, OwnedHeaders},
    producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext},
};
use std::time::Duration;

pub const TOPIC: &str = "live-raw";
pub const PARTITIONS: i32 = 6;
/// The provisional 37-day raw retention from PROPOSAL.md.
pub const RETENTION_MS: i64 = 37 * 24 * 60 * 60 * 1000;
/// Matches the decoder's capture bound; larger inputs become retained errors, never drops.
pub const MAX_MESSAGE_BYTES: usize = crate::decoder::MAX_CAPTURE_BYTES;

/// Broker addresses from INDEXER_KAFKA_BROKERS (never logged).
pub fn brokers() -> Result<String> {
    std::env::var("INDEXER_KAFKA_BROKERS").map_err(|_| Error("missing_kafka_brokers"))
}

/// shortcut: no TLS or SASL client yet, so only loopback brokers are accepted; a hosted
/// broker also needs replication factor >= 3 and min.insync.replicas >= 2 (#36).
pub fn require_loopback(brokers: &str) -> Result<()> {
    let loopback = |address: &str| {
        let host = address.rsplit_once(':').map_or(address, |(host, _)| host);
        matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]")
    };
    if brokers.is_empty() || !brokers.split(',').map(str::trim).all(loopback) {
        return Err(Error("kafka_tls_required"));
    }
    Ok(())
}

#[derive(Default)]
struct Deliveries(std::sync::Mutex<Vec<(u64, bool)>>);

impl ClientContext for Deliveries {}

impl ProducerContext for Deliveries {
    type DeliveryOpaque = Box<u64>;

    fn delivery(&self, result: &DeliveryResult<'_>, sequence: Self::DeliveryOpaque) {
        if let Ok(mut deliveries) = self.0.lock() {
            deliveries.push((*sequence, result.is_ok()));
        }
    }
}

/// Idempotent producer of live wire payloads, keyed by slot. Waits for broker room
/// instead of dropping, and reports the contiguous replay cursor as deliveries confirm.
pub struct IntakeProducer {
    producer: BaseProducer<Deliveries>,
    topic: String,
    cursor: DeliveryCursor,
    highest_sent: Option<u64>,
    /// Messages older than the newest sent slot by more than the reorder margin: a resume
    /// from a cursor stored after them could skip them, so they are counted and reported.
    pub late_beyond_margin: u64,
}

impl IntakeProducer {
    pub fn new(brokers: &str, topic: &str) -> Result<Self> {
        require_loopback(brokers)?;
        let producer = config(brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("linger.ms", "20")
            // Bounded local queue: a full queue applies backpressure to the stream.
            .set("queue.buffering.max.messages", "10000")
            .create_with_context(Deliveries::default())
            .map_err(|_| Error("kafka_unavailable"))?;
        Ok(Self {
            producer,
            topic: topic.into(),
            cursor: DeliveryCursor::default(),
            highest_sent: None,
            late_beyond_margin: 0,
        })
    }

    pub fn send(&mut self, slot: u64, payload: &[u8], schema: &str) -> Result<()> {
        if payload.len() > MAX_MESSAGE_BYTES {
            return Err(Error("live_input_limit"));
        }
        if self
            .highest_sent
            .is_some_and(|highest| slot < highest.saturating_sub(REORDER_MARGIN_SLOTS))
        {
            self.late_beyond_margin += 1;
        }
        self.highest_sent = Some(self.highest_sent.map_or(slot, |h| h.max(slot)));
        let sequence = self.cursor.sent(slot);
        let key = slot.to_be_bytes();
        let headers = OwnedHeaders::new().insert(Header {
            key: "proto_schema",
            value: Some(schema),
        });
        let mut record = BaseRecord::with_opaque_to(&self.topic, Box::new(sequence))
            .key(&key)
            .payload(payload)
            .headers(headers);
        loop {
            match self.producer.send(record) {
                Ok(()) => return Ok(()),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), back)) => {
                    record = back;
                    self.producer.poll(Duration::from_millis(100));
                }
                Err(_) => return Err(Error("kafka_produce_failed")),
            }
        }
    }

    /// Serves delivery reports; returns the cursor if it advanced. A failed delivery stops
    /// intake: the cursor stays behind it, so a restart produces it again.
    pub fn poll(&mut self, timeout: Duration) -> Result<Option<u64>> {
        self.producer.poll(timeout);
        let reports = std::mem::take(
            &mut *self
                .producer
                .context()
                .0
                .lock()
                .map_err(|_| Error("kafka_delivery_failed"))?,
        );
        let mut advanced = None;
        for (sequence, ok) in reports {
            if !ok {
                return Err(Error("kafka_delivery_failed"));
            }
            advanced = self.cursor.delivered(sequence).or(advanced);
        }
        Ok(advanced.and(self.cursor.cursor()))
    }

    /// Waits for every queued message, then reports the final cursor.
    pub fn flush(&mut self, timeout: Duration) -> Result<Option<u64>> {
        self.producer
            .flush(timeout)
            .map_err(|_| Error("kafka_delivery_failed"))?;
        self.poll(Duration::ZERO)?;
        if self.cursor.in_flight() > 0 {
            return Err(Error("kafka_delivery_failed"));
        }
        Ok(self.cursor.cursor())
    }

    pub fn cursor(&self) -> Option<u64> {
        self.cursor.cursor()
    }
}

/// Settings every client shares; errors are reduced to a class.
pub(crate) fn config(brokers: &str) -> ClientConfig {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", brokers)
        .set(
            "message.max.bytes",
            (MAX_MESSAGE_BYTES + 64 * 1024).to_string(),
        )
        .set("socket.timeout.ms", "10000");
    config
}

fn block_on<F: std::future::Future>(future: F) -> Result<F::Output> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| Error("runtime_failed"))?
        .block_on(future))
}

/// Creates the topic with the retention and size policy, or verifies an existing one
/// carries it. A topic with a shorter retention is refused rather than used.
pub fn ensure_topic(brokers: &str, topic: &str, partitions: i32) -> Result<()> {
    let admin: AdminClient<DefaultClientContext> = config(brokers)
        .create()
        .map_err(|_| Error("kafka_unavailable"))?;
    let options = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let retention = RETENTION_MS.to_string();
    let max_bytes = (MAX_MESSAGE_BYTES + 64 * 1024).to_string();
    let new = NewTopic::new(topic, partitions, TopicReplication::Fixed(1))
        .set("retention.ms", &retention)
        .set("retention.bytes", "-1")
        // Compaction keyed by slot would keep one message per slot and silently drop the rest.
        .set("cleanup.policy", "delete")
        .set("max.message.bytes", &max_bytes);
    let created =
        block_on(admin.create_topics([&new], &options))?.map_err(|_| Error("kafka_unavailable"))?;
    for result in created {
        match result {
            Ok(_) => {}
            Err((_, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err(_) => return Err(Error("kafka_topic_create_failed")),
        }
    }
    // A just-created topic's configuration can take a moment to become visible.
    for _ in 0..20 {
        if retention_ok(&admin, topic, &options)? {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    // The class also covers compaction and byte caps: any policy that can drop raw input early.
    Err(Error("kafka_topic_retention_too_short"))
}

fn retention_ok(
    admin: &AdminClient<DefaultClientContext>,
    topic: &str,
    options: &AdminOptions,
) -> Result<bool> {
    let described = block_on(admin.describe_configs([&ResourceSpecifier::Topic(topic)], options))?
        .map_err(|_| Error("kafka_unavailable"))?;
    let Some(Ok(resource)) = described.into_iter().next() else {
        return Ok(false);
    };
    let value = |name: &str| -> Option<i64> {
        resource
            .get(name)
            .and_then(|e| e.value.as_deref()?.parse().ok())
    };
    let delete_only = resource
        .get("cleanup.policy")
        .and_then(|e| e.value.as_deref())
        == Some("delete");
    // -1 retention.ms means forever, which also satisfies the policy.
    Ok(
        value("retention.ms").is_some_and(|ms| ms == -1 || ms >= RETENTION_MS)
            && value("retention.bytes") == Some(-1)
            && delete_only,
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rdkafka::{
        Message,
        consumer::{BaseConsumer, Consumer},
        producer::{BaseProducer, BaseRecord, Producer},
    };

    pub(crate) fn test_brokers() -> String {
        std::env::var("INDEXER_TEST_KAFKA_BROKERS").unwrap_or_else(|_| "127.0.0.1:59092".into())
    }

    /// A fresh topic per test, deleted afterwards. Start the broker with `docker compose up -d kafka`.
    pub(crate) struct TestTopic(pub String);

    impl TestTopic {
        pub(crate) fn new(partitions: i32) -> Self {
            use std::sync::atomic::{AtomicU32, Ordering};
            static NEXT: AtomicU32 = AtomicU32::new(0);
            let name = format!(
                "test-{}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .subsec_nanos()
            );
            ensure_topic(&test_brokers(), &name, partitions)
                .expect("local Kafka unavailable: run `docker compose up -d kafka`");
            Self(name)
        }
    }

    impl Drop for TestTopic {
        fn drop(&mut self) {
            if let Ok(admin) = config(&test_brokers()).create::<AdminClient<DefaultClientContext>>()
            {
                let _ = block_on(admin.delete_topics(&[&self.0], &AdminOptions::new()));
            }
        }
    }

    #[test]
    fn topic_carries_the_raw_retention_policy_and_creation_is_idempotent() {
        let topic = TestTopic::new(PARTITIONS);
        ensure_topic(&test_brokers(), &topic.0, PARTITIONS).unwrap();
        let admin: AdminClient<DefaultClientContext> = config(&test_brokers()).create().unwrap();
        let described = block_on(
            admin.describe_configs([&ResourceSpecifier::Topic(&topic.0)], &AdminOptions::new()),
        )
        .unwrap()
        .unwrap();
        let resource = described.into_iter().next().unwrap().unwrap();
        let get = |name: &str| resource.get(name).and_then(|e| e.value.clone()).unwrap();
        assert_eq!(get("retention.ms"), RETENTION_MS.to_string());
        assert_eq!(get("retention.bytes"), "-1");
        assert_eq!(get("cleanup.policy"), "delete");
    }

    #[test]
    fn a_short_retention_topic_is_refused() {
        let topic = TestTopic::new(1);
        let admin: AdminClient<DefaultClientContext> = config(&test_brokers()).create().unwrap();
        let mut alter = rdkafka::admin::AlterConfig::new(ResourceSpecifier::Topic(&topic.0));
        alter = alter.set("retention.ms", "86400000");
        block_on(admin.alter_configs([&alter], &AdminOptions::new()))
            .unwrap()
            .unwrap();
        assert_eq!(
            ensure_topic(&test_brokers(), &topic.0, 1).unwrap_err().0,
            "kafka_topic_retention_too_short"
        );
    }

    #[test]
    fn a_compacted_topic_is_refused() {
        let topic = TestTopic::new(1);
        let admin: AdminClient<DefaultClientContext> = config(&test_brokers()).create().unwrap();
        let retention = RETENTION_MS.to_string();
        let alter = rdkafka::admin::AlterConfig::new(ResourceSpecifier::Topic(&topic.0))
            .set("retention.ms", &retention)
            .set("retention.bytes", "-1")
            .set("cleanup.policy", "compact");
        block_on(admin.alter_configs([&alter], &AdminOptions::new()))
            .unwrap()
            .unwrap();
        assert_eq!(
            ensure_topic(&test_brokers(), &topic.0, 1).unwrap_err().0,
            "kafka_topic_retention_too_short"
        );
    }

    #[test]
    fn messages_round_trip_and_oversize_input_is_rejected_not_dropped() {
        let topic = TestTopic::new(1);
        let producer: BaseProducer = config(&test_brokers())
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .unwrap();
        producer
            .send(
                BaseRecord::to(&topic.0)
                    .key(&42u64.to_be_bytes())
                    .payload(b"wire"),
            )
            .map_err(|(e, _)| e)
            .unwrap();
        producer.flush(Duration::from_secs(10)).unwrap();
        let oversize = vec![0u8; MAX_MESSAGE_BYTES + 128 * 1024];
        assert!(
            producer
                .send(
                    BaseRecord::to(&topic.0)
                        .key(&43u64.to_be_bytes())
                        .payload(&oversize)
                )
                .is_err()
        );
        let consumer: BaseConsumer = config(&test_brokers())
            .set("group.id", format!("{}-group", topic.0))
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[&topic.0]).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let message = loop {
            if let Some(result) = consumer.poll(Duration::from_millis(200)) {
                break result.unwrap().detach();
            }
            assert!(std::time::Instant::now() < deadline, "no message consumed");
        };
        assert_eq!(message.payload(), Some(&b"wire"[..]));
        assert_eq!(message.key(), Some(&42u64.to_be_bytes()[..]));
    }
}

/// Slots to replay before the stored cursor on resume. The stream delivers transactions
/// slightly out of slot order, so a transaction for an earlier slot may still be on its way.
/// Provisional and unmeasured: how far LaserStream reorders has not been recorded yet.
/// shortcut: a fixed margin; replace with per-slot completion tracking when it exists (#36).
pub const REORDER_MARGIN_SLOTS: u64 = 150;

/// The replay cursor for produced messages: the lowest slot still in flight, or the highest
/// delivered slot when nothing is in flight. Resuming from it (inclusive, minus
/// `REORDER_MARGIN_SLOTS`) can repeat input but never skips an undelivered message, even
/// when deliveries confirm out of order or slots arrive out of order.
#[derive(Debug, Default)]
pub struct DeliveryCursor {
    next_sequence: u64,
    in_flight: std::collections::BTreeMap<u64, u64>,
    highest_delivered: Option<u64>,
}

impl DeliveryCursor {
    /// Registers a message about to be produced and returns its sequence number.
    pub fn sent(&mut self, slot: u64) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence += 1;
        self.in_flight.insert(sequence, slot);
        sequence
    }

    /// Records a confirmed delivery; returns the cursor if it changed.
    pub fn delivered(&mut self, sequence: u64) -> Option<u64> {
        let before = self.cursor();
        if let Some(slot) = self.in_flight.remove(&sequence) {
            self.highest_delivered = Some(self.highest_delivered.map_or(slot, |h| h.max(slot)));
        }
        let after = self.cursor();
        (after != before).then_some(after).flatten()
    }

    pub fn cursor(&self) -> Option<u64> {
        self.in_flight
            .values()
            .min()
            .copied()
            .or(self.highest_delivered)
    }

    /// Messages sent but not yet confirmed.
    pub fn in_flight(&self) -> usize {
        self.in_flight.len()
    }
}

/// Where live intake resumes for a stored cursor.
pub fn resume_slot(cursor: u64) -> u64 {
    cursor.saturating_sub(REORDER_MARGIN_SLOTS)
}

#[cfg(test)]
mod cursor_tests {
    use super::{DeliveryCursor, resume_slot};

    #[test]
    fn the_cursor_never_passes_a_message_still_in_flight() {
        let mut cursor = DeliveryCursor::default();
        let a = cursor.sent(100);
        let b = cursor.sent(101);
        let c = cursor.sent(102);
        assert_eq!(cursor.cursor(), Some(100));
        assert_eq!(cursor.delivered(c), None, "100 and 101 are still in flight");
        assert_eq!(cursor.delivered(b), None);
        assert_eq!(cursor.delivered(a), Some(102));
        assert_eq!(cursor.in_flight(), 0);
    }

    #[test]
    fn an_older_slot_in_flight_holds_the_cursor_below_it() {
        let mut cursor = DeliveryCursor::default();
        let newer = cursor.sent(200);
        let older = cursor.sent(199);
        cursor.delivered(newer);
        assert!(cursor.cursor().unwrap() <= 199, "199 is undelivered");
        cursor.delivered(older);
        assert_eq!(cursor.cursor(), Some(200));
        assert_eq!(
            cursor.delivered(newer),
            None,
            "a duplicate confirmation changes nothing"
        );
    }

    #[test]
    fn resume_replays_a_bounded_reorder_margin() {
        assert_eq!(resume_slot(1_000), 1_000 - super::REORDER_MARGIN_SLOTS);
        assert_eq!(resume_slot(10), 0);
    }
}
