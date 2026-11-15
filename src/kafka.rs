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

/// A crashed consumer keeps its partitions until this expires. Heartbeats come from
/// librdkafka's own thread, so a blocked storage write does not trigger it.
const SESSION_TIMEOUT: Duration = Duration::from_secs(10);

/// Broker addresses from INDEXER_KAFKA_BROKERS (never logged).
pub fn brokers() -> Result<String> {
    std::env::var("INDEXER_KAFKA_BROKERS").map_err(|_| Error("missing_kafka_brokers"))
}

/// shortcut: no TLS or SASL client yet, so only loopback brokers are accepted; a hosted
/// broker also needs replication factor >= 3 and min.insync.replicas >= 2 (#36).
/// INDEXER_KAFKA_PRIVATE_NETWORK=1 also admits named brokers on a private container
/// network such as Compose's, which no other host can reach.
pub fn require_loopback(brokers: &str) -> Result<()> {
    plaintext_allowed(
        brokers,
        std::env::var("INDEXER_KAFKA_PRIVATE_NETWORK").as_deref() == Ok("1"),
    )
}

fn plaintext_allowed(brokers: &str, private_network: bool) -> Result<()> {
    let loopback = |address: &str| {
        let host = address.rsplit_once(':').map_or(address, |(host, _)| host);
        private_network || matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]")
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

/// Assigns partitions at the positions stored in PostgreSQL, never Kafka's committed
/// offsets, and records revocations so a half-filled batch is not written by a consumer
/// that no longer owns its partitions.
struct Positions {
    db: crate::storage::Database,
    group: String,
    topic: String,
    revoked: std::sync::atomic::AtomicBool,
    failed: std::sync::Mutex<Option<Error>>,
}

impl ClientContext for Positions {}

impl rdkafka::consumer::ConsumerContext for Positions {
    fn rebalance(
        &self,
        consumer: &rdkafka::consumer::BaseConsumer<Self>,
        err: rdkafka::types::RDKafkaRespErr,
        tpl: &mut rdkafka::TopicPartitionList,
    ) {
        use rdkafka::{Offset, consumer::Consumer, types::RDKafkaRespErr};
        match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => {
                match crate::storage::live::LiveLog::kafka_offsets(
                    &self.db,
                    &self.group,
                    &self.topic,
                ) {
                    Ok(stored) => {
                        let partitions: Vec<(String, i32)> = tpl
                            .elements()
                            .iter()
                            .map(|e| (e.topic().to_string(), e.partition()))
                            .collect();
                        for (topic, partition) in partitions {
                            let offset = stored
                                .iter()
                                .find(|s| s.0 == partition)
                                .map_or(Offset::Beginning, |s| Offset::Offset(s.1));
                            let _ = tpl.set_partition_offset(&topic, partition, offset);
                        }
                        let _ = consumer.assign(tpl);
                    }
                    // Without stored positions nothing is assigned; the loop reports the error.
                    Err(error) => {
                        if let Ok(mut failed) = self.failed.lock() {
                            *failed = Some(error);
                        }
                    }
                }
            }
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => {
                self.revoked
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                let _ = consumer.unassign();
            }
            _ => {}
        }
    }
}

/// Consumes live input from `topic` as one member of the session's consumer group and
/// writes it in batches; each batch commits its output and the group's positions together.
/// Input that fails before storage is retained in live_raw for offline retry. A storage
/// failure stops the consumer without advancing positions, so a restart replays the batch.
pub fn consume(
    database: &crate::storage::Database,
    brokers: &str,
    topic: &str,
    seconds: u64,
) -> Result<serde_json::Value> {
    use crate::live::{BATCH_INPUTS, BATCH_WINDOW};
    use rdkafka::consumer::{BaseConsumer, Consumer};
    require_loopback(brokers)?;
    ensure_topic(brokers, topic, PARTITIONS)?;
    let group = format!("indexer-{}", database.session());
    let consumer: BaseConsumer<Positions> = config(brokers)
        .set("group.id", &group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set(
            "session.timeout.ms",
            SESSION_TIMEOUT.as_millis().to_string(),
        )
        .create_with_context(Positions {
            db: database.clone(),
            group: group.clone(),
            topic: topic.into(),
            revoked: Default::default(),
            failed: Default::default(),
        })
        .map_err(|_| Error("kafka_unavailable"))?;
    consumer
        .subscribe(&[topic])
        .map_err(|_| Error("kafka_unavailable"))?;
    let mut store = crate::storage::Store::open(database)?;
    // One run identity for every event this consumer emits.
    let mut telemetry = crate::telemetry::Telemetry::default();
    telemetry.set_context("kafka_consumer", "kafka", "live");
    let deadline = std::time::Instant::now() + Duration::from_secs(seconds);
    let mut buffer: Vec<rdkafka::message::OwnedMessage> = Vec::new();
    let mut since: Option<std::time::Instant> = None;
    let mut totals = Totals::default();
    loop {
        if let Some(error) = consumer
            .context()
            .failed
            .lock()
            .ok()
            .and_then(|mut f| f.take())
        {
            return Err(error);
        }
        let now = std::time::Instant::now();
        let done = now >= deadline;
        let due =
            since.is_some_and(|s| s.elapsed() >= BATCH_WINDOW) || buffer.len() >= BATCH_INPUTS;
        if (done || due) && !buffer.is_empty() {
            if let Err(error) =
                write_consumed(&mut store, &group, topic, &buffer, &mut totals, &telemetry)
            {
                telemetry.emit(
                    "consumer_stopped_on_storage_failure",
                    "consume",
                    "failed",
                    Some(error.0),
                );
                return Err(error);
            }
            buffer.clear();
            since = None;
        }
        if done {
            break;
        }
        let wait = since
            .map_or(BATCH_WINDOW, |s| BATCH_WINDOW.saturating_sub(s.elapsed()))
            .min(deadline.saturating_duration_since(now))
            .max(Duration::from_millis(1));
        match consumer.poll(wait) {
            Some(Ok(message)) => {
                buffer.push(message.detach());
                since.get_or_insert_with(std::time::Instant::now);
            }
            Some(Err(_)) => return Err(Error("kafka_consume_failed")),
            None => {}
        }
        if consumer
            .context()
            .revoked
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            // Positions for these messages were never stored; the next owner replays them.
            totals.revoked_unwritten += buffer.len() as u64;
            buffer.clear();
            since = None;
        }
    }
    Ok(
        serde_json::json!({"consumer_group":group,"topic":topic,"consumed":totals.consumed,
        "inserted_transfers":totals.inserted,"retained_failures":totals.retained,
        "batches":totals.batches,"revoked_unwritten":totals.revoked_unwritten,"storage_failures_retained":totals.storage_retained,
        "storage":store.stats(),"coverage":"unknown"}),
    )
}

#[derive(Default)]
struct Totals {
    consumed: u64,
    inserted: u64,
    retained: u64,
    batches: u64,
    revoked_unwritten: u64,
    storage_retained: u64,
    consecutive_retained: u64,
}

/// Consecutive inputs retained for storage failure before the consumer treats the fault
/// as systemic (a broken table, a decoder regression) and stops instead of draining.
const MAX_CONSECUTIVE_RETAINED: u64 = 3;

/// Writes a batch; if it fails storage, retries its messages one by one, in order. A
/// message that still fails on its own is retained in live_raw with its error class and its
/// position advances, so one bad input cannot block its partition; `live-retry` stores it
/// once the cause is fixed. After `MAX_CONSECUTIVE_RETAINED` such failures in a row, or if
/// retention itself fails, the consumer stops without advancing.
fn write_consumed(
    store: &mut crate::storage::Store,
    group: &str,
    topic: &str,
    messages: &[rdkafka::message::OwnedMessage],
    totals: &mut Totals,
    telemetry: &crate::telemetry::Telemetry,
) -> Result<()> {
    use rdkafka::Message;
    if write_once(store, group, topic, messages, totals).is_ok() {
        totals.consecutive_retained = 0;
        return Ok(());
    }
    for message in messages {
        let error = match write_once(store, group, topic, std::slice::from_ref(message), totals) {
            Ok(()) => {
                totals.consecutive_retained = 0;
                continue;
            }
            Err(error) => error,
        };
        if totals.consecutive_retained >= MAX_CONSECUTIVE_RETAINED {
            telemetry.emit(
                "consumer_stopped_on_systemic_storage_failure",
                "consume",
                "failed",
                Some(error.0),
            );
            return Err(error);
        }
        let slot = message
            .key()
            .and_then(|k| <[u8; 8]>::try_from(k).ok())
            .map_or(0, |k| u64::from_be_bytes(k) as i64);
        store.acknowledge(&crate::storage::Acknowledge::Kafka {
            group,
            topic,
            offsets: &[(message.partition(), message.offset() + 1)],
            retained: &[(
                slot,
                message.payload().unwrap_or_default().to_vec(),
                error.0,
            )],
        })?;
        telemetry.emit(
            "consumer_retained_storage_failure",
            "consume",
            "failed",
            Some(error.0),
        );
        totals.consecutive_retained += 1;
        totals.consumed += 1;
        totals.retained += 1;
        totals.storage_retained += 1;
    }
    Ok(())
}

fn write_once(
    store: &mut crate::storage::Store,
    group: &str,
    topic: &str,
    messages: &[rdkafka::message::OwnedMessage],
    totals: &mut Totals,
) -> Result<()> {
    use crate::{
        decoder,
        storage::{Acknowledge, BatchItem},
    };
    use laserstream_core_proto::geyser::SubscribeUpdateTransaction;
    use prost::Message as _;
    use rdkafka::message::{Headers, Message};
    let mut offsets = std::collections::BTreeMap::new();
    let mut decoded = Vec::new();
    let mut retained = Vec::new();
    for message in messages {
        let next = offsets.entry(message.partition()).or_insert(0);
        *next = (*next).max(message.offset() + 1);
        let payload = message.payload().unwrap_or_default();
        let Some(slot) = message
            .key()
            .and_then(|k| <[u8; 8]>::try_from(k).ok())
            .map(|k| u64::from_be_bytes(k) as i64)
        else {
            // Every produced message is keyed by slot; anything else is retained, not guessed.
            retained.push((0, payload.to_vec(), "kafka_message_missing_key"));
            continue;
        };
        let schema = message.headers().and_then(|headers| {
            headers
                .iter()
                .find(|h| h.key == "proto_schema")
                .and_then(|h| h.value)
        });
        let prepared = (|| {
            if schema != Some(crate::live::PROTO_SCHEMA.as_bytes()) {
                return Err(Error("unsupported_stored_proto_schema"));
            }
            let update = SubscribeUpdateTransaction::decode(payload)
                .map_err(|_| Error("stored_record_invalid"))?;
            let raw = crate::live::normalize(&update)?;
            Ok((decoder::parse_capture(&raw)?, raw))
        })();
        match prepared {
            Ok((capture, raw)) => {
                let transfers = decoder::decode(&capture);
                // Undecodable but stored captures are also retained, as live intake does.
                if let Err(error) = &transfers {
                    retained.push((slot, payload.to_vec(), error.0));
                }
                decoded.push((capture, raw, transfers));
            }
            Err(error) => retained.push((slot, payload.to_vec(), error.0)),
        }
    }
    let items: Vec<BatchItem<'_>> = decoded
        .iter()
        .map(|(capture, raw, transfers)| BatchItem {
            capture,
            raw,
            decoded: transfers,
        })
        .collect();
    let offsets: Vec<(i32, i64)> = offsets.into_iter().collect();
    let fallbacks = store.stats().fallbacks;
    let results = store.persist_batch_acknowledged(
        &items,
        Some(&Acknowledge::Kafka {
            group,
            topic,
            offsets: &offsets,
            retained: &retained,
        }),
    );
    if store.stats().fallbacks > fallbacks
        || results
            .iter()
            .any(|r| matches!(r, Err(e) if e.0.starts_with("storage_")))
    {
        return Err(Error("storage_write_failed"));
    }
    totals.consumed += messages.len() as u64;
    totals.inserted += results
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .sum::<usize>() as u64;
    totals.retained += retained.len() as u64;
    totals.batches += 1;
    Ok(())
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
    fn private_network_opt_in_admits_named_brokers_only_when_set() {
        assert_eq!(
            plaintext_allowed("kafka:9092", false).unwrap_err().0,
            "kafka_tls_required"
        );
        assert!(plaintext_allowed("kafka:9092", true).is_ok());
        assert_eq!(
            plaintext_allowed("", true).unwrap_err().0,
            "kafka_tls_required"
        );
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

    /// Waits until the broker reports `name=value` for `topic`, so a test observes its own change.
    fn wait_for_config(topic: &str, name: &str, value: &str) {
        let admin: AdminClient<DefaultClientContext> = config(&test_brokers()).create().unwrap();
        for _ in 0..80 {
            let described = block_on(
                admin.describe_configs([&ResourceSpecifier::Topic(topic)], &AdminOptions::new()),
            )
            .unwrap()
            .unwrap();
            let current = described
                .into_iter()
                .next()
                .and_then(|r| r.ok())
                .and_then(|r| r.get(name).and_then(|e| e.value.clone()));
            if current.as_deref() == Some(value) {
                return;
            }
            std::thread::sleep(Duration::from_millis(125));
        }
        panic!("{name} did not become {value}");
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
        wait_for_config(&topic.0, "retention.ms", "86400000");
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
        wait_for_config(&topic.0, "cleanup.policy", "compact");
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
/// Provisional: one 60-second, 194-slot finalized USDC sample showed no out-of-order
/// arrivals (docs/validation/2026-11-14-kafka-consumer-throughput.md); one sample is not a bound.
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

#[cfg(test)]
mod consumer_tests {
    use super::{
        IntakeProducer, consume,
        tests::{TestTopic, test_brokers},
    };
    use crate::{
        live::{PROTO_SCHEMA, tests::update},
        storage::{Store, pg::TestDatabase},
    };
    use prost::Message;
    use std::time::Duration;

    /// A valid one-transfer update with its own signature.
    fn signed(n: u8, slot: u64) -> laserstream_core_proto::geyser::SubscribeUpdateTransaction {
        let mut tx = update();
        tx.slot = slot;
        let info = tx.transaction.as_mut().unwrap();
        info.signature = vec![n; 64];
        info.transaction.as_mut().unwrap().signatures = vec![vec![n; 64]];
        tx
    }

    fn produce(
        topic: &str,
        updates: &[laserstream_core_proto::geyser::SubscribeUpdateTransaction],
    ) {
        let mut producer = IntakeProducer::new(&test_brokers(), topic).unwrap();
        for tx in updates {
            producer
                .send(tx.slot, &tx.encode_to_vec(), PROTO_SCHEMA)
                .unwrap();
        }
        producer.flush(Duration::from_secs(20)).unwrap();
    }

    fn stored_positions(test: &TestDatabase) -> i64 {
        test.sql()
            .query_one(
                "SELECT COALESCE(SUM(next_offset),0)::bigint FROM kafka_offsets",
                &[],
            )
            .unwrap()
            .get(0)
    }

    #[test]
    fn input_is_stored_once_failures_are_retained_and_restarts_resume() {
        let test = TestDatabase::new();
        let topic = TestTopic::new(2);
        let mut broken = signed(7, 125);
        broken.transaction.as_mut().unwrap().meta = None;
        produce(&topic.0, &[signed(9, 123), signed(8, 124), broken]);
        let first = consume(&test.0, &test_brokers(), &topic.0, 6).unwrap();
        assert_eq!(first["consumed"], 3);
        assert_eq!(first["inserted_transfers"], 2);
        assert_eq!(first["retained_failures"], 1);
        assert_eq!(stored_positions(&test), 3);
        let status: String = test
            .sql()
            .query_one("SELECT status FROM live_raw", &[])
            .unwrap()
            .get(0);
        assert_eq!(status, "missing_execution_metadata");
        let again = consume(&test.0, &test_brokers(), &topic.0, 4).unwrap();
        assert_eq!(
            again["consumed"], 0,
            "a restart resumes from stored positions"
        );
        assert_eq!(Store::query(&test.0, None, 100).unwrap().len(), 2);
    }

    #[test]
    fn a_message_without_a_slot_key_is_retained_with_its_own_class() {
        use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
        let test = TestDatabase::new();
        let topic = TestTopic::new(1);
        let producer: BaseProducer = super::config(&test_brokers()).create().unwrap();
        producer
            .send(BaseRecord::<(), [u8]>::to(&topic.0).payload(&signed(9, 123).encode_to_vec()))
            .map_err(|(e, _)| e)
            .unwrap();
        producer.flush(Duration::from_secs(10)).unwrap();
        let report = consume(&test.0, &test_brokers(), &topic.0, 5).unwrap();
        assert_eq!(report["retained_failures"], 1);
        let status: String = test
            .sql()
            .query_one("SELECT status FROM live_raw", &[])
            .unwrap()
            .get(0);
        assert_eq!(status, "kafka_message_missing_key");
        assert_eq!(
            stored_positions(&test),
            1,
            "the unkeyed message is not replayed forever"
        );
    }

    #[test]
    fn a_poison_input_is_retained_and_its_partition_moves_on() {
        let test = TestDatabase::new();
        let topic = TestTopic::new(1);
        let poison = bs58::encode([8u8; 64]).into_string();
        produce(&topic.0, &[signed(9, 123), signed(8, 124), signed(6, 126)]);
        Store::open(&test.0).unwrap();
        test.sql()
            .batch_execute(&format!("CREATE FUNCTION poison() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.signature='{poison}' THEN RAISE EXCEPTION 'poison'; END IF; RETURN NEW; END$$; CREATE TRIGGER poison BEFORE INSERT ON captures FOR EACH ROW EXECUTE FUNCTION poison();"))
            .unwrap();
        let report = consume(&test.0, &test_brokers(), &topic.0, 6).unwrap();
        // The good inputs may be stored by the batch fallback before the per-message retry,
        // so the stored rows, not the newly-inserted count, are the measure.
        assert_eq!(Store::query(&test.0, None, 100).unwrap().len(), 2);
        assert_eq!(report["storage_failures_retained"], 1);
        assert_eq!(stored_positions(&test), 3, "the partition is not blocked");
        let status: String = test
            .sql()
            .query_one("SELECT status FROM live_raw", &[])
            .unwrap()
            .get(0);
        assert_eq!(status, "storage_write_failed");
        // Nothing is lost: once the cause is fixed, offline retry stores the retained input.
        test.sql()
            .batch_execute("DROP TRIGGER poison ON captures;")
            .unwrap();
        let mut intake = crate::live::Intake::open(&test.0).unwrap();
        assert_eq!(intake.retry().unwrap()["decoded"], 1);
        assert_eq!(Store::query(&test.0, None, 100).unwrap().len(), 3);
    }

    #[test]
    fn a_systemic_storage_fault_stops_after_a_bounded_number_of_retained_inputs() {
        let test = TestDatabase::new();
        let topic = TestTopic::new(1);
        let updates: Vec<_> = (1..=6u8).map(|n| signed(n, 100 + u64::from(n))).collect();
        produce(&topic.0, &updates);
        Store::open(&test.0).unwrap();
        test.sql()
            .batch_execute("CREATE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_transfers BEFORE INSERT ON transfers FOR EACH ROW EXECUTE FUNCTION fail();")
            .unwrap();
        assert_eq!(
            consume(&test.0, &test_brokers(), &topic.0, 6)
                .unwrap_err()
                .0,
            "storage_write_failed"
        );
        let retained: i64 = test
            .sql()
            .query_one("SELECT COUNT(*) FROM live_raw", &[])
            .unwrap()
            .get(0);
        assert_eq!(
            retained,
            super::MAX_CONSECUTIVE_RETAINED as i64,
            "the topic is not drained"
        );
        assert_eq!(
            stored_positions(&test),
            3,
            "only the retained inputs advanced"
        );
        test.sql()
            .batch_execute("DROP TRIGGER fail_transfers ON transfers;")
            .unwrap();
        consume(&test.0, &test_brokers(), &topic.0, 6).unwrap();
        let mut intake = crate::live::Intake::open(&test.0).unwrap();
        assert_eq!(intake.retry().unwrap()["decoded"], 3);
        assert_eq!(Store::query(&test.0, None, 100).unwrap().len(), 6);
    }

    #[test]
    fn positions_never_advance_when_a_failed_input_cannot_be_retained() {
        let test = TestDatabase::new();
        let topic = TestTopic::new(1);
        produce(&topic.0, &[signed(9, 123), signed(8, 124)]);
        Store::open(&test.0).unwrap();
        test.sql()
            .batch_execute("CREATE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_transfers BEFORE INSERT ON transfers FOR EACH ROW EXECUTE FUNCTION fail(); CREATE TRIGGER fail_raw BEFORE INSERT ON live_raw FOR EACH ROW EXECUTE FUNCTION fail();")
            .unwrap();
        assert_eq!(
            consume(&test.0, &test_brokers(), &topic.0, 6)
                .unwrap_err()
                .0,
            "storage_write_failed"
        );
        assert_eq!(stored_positions(&test), 0);
        test.sql()
            .batch_execute(
                "DROP TRIGGER fail_transfers ON transfers; DROP TRIGGER fail_raw ON live_raw;",
            )
            .unwrap();
        let retry = consume(&test.0, &test_brokers(), &topic.0, 6).unwrap();
        assert_eq!(retry["inserted_transfers"], 2);
    }

    #[test]
    fn two_consumers_split_partitions_and_store_every_input_once() {
        let test = TestDatabase::new();
        let topic = TestTopic::new(6);
        let updates: Vec<_> = (1..=40u8)
            .map(|n| signed(n, 1_000 + u64::from(n)))
            .collect();
        produce(&topic.0, &updates);
        let workers: Vec<_> = (0..2)
            .map(|_| {
                let (db, name) = (test.0.clone(), topic.0.clone());
                std::thread::spawn(move || consume(&db, &test_brokers(), &name, 12).unwrap())
            })
            .collect();
        let reports: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        assert_eq!(Store::query(&test.0, None, 100).unwrap().len(), 40);
        assert_eq!(stored_positions(&test), 40);
        let consumed: u64 = reports
            .iter()
            .map(|r| r["consumed"].as_u64().unwrap())
            .sum();
        assert!(
            consumed >= 40,
            "every input consumed; redelivery is allowed"
        );
    }

    /// Opt-in: drain time for INDEXER_MEASURE_INPUTS inputs (default 2,400) over 6
    /// partitions with 1, 2 and 4 consumers, against the local broker and the local test
    /// database, or the hosted one named by INDEXER_HOSTED_DATABASE_URL (with its TLS
    /// settings). Ordinary tests never use a hosted database.
    #[test]
    #[ignore = "opt-in throughput measurement"]
    fn measure_consumer_group_throughput() {
        let inputs: u16 = std::env::var("INDEXER_MEASURE_INPUTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2_400);
        let updates: Vec<_> = (0..inputs)
            .map(|i| {
                let mut tx = signed(1, 2_000 + u64::from(i));
                let mut signature = vec![(i % 251) as u8 + 1; 64];
                signature[..2].copy_from_slice(&i.to_be_bytes());
                let info = tx.transaction.as_mut().unwrap();
                info.signature = signature.clone();
                info.transaction.as_mut().unwrap().signatures = vec![signature];
                tx
            })
            .collect();
        let mut results = Vec::new();
        for consumers in [1usize, 2, 4] {
            let test = std::env::var("INDEXER_HOSTED_DATABASE_URL")
                .map_or_else(|_| TestDatabase::new(), TestDatabase::on);
            let topic = TestTopic::new(6);
            produce(&topic.0, &updates);
            Store::open(&test.0).unwrap();
            let started = std::time::Instant::now();
            let workers: Vec<_> = (0..consumers)
                .map(|_| {
                    let (db, name) = (test.0.clone(), topic.0.clone());
                    std::thread::spawn(move || consume(&db, &test_brokers(), &name, 60).unwrap())
                })
                .collect();
            let drained = loop {
                if stored_positions(&test) >= i64::from(inputs) {
                    break started.elapsed().as_secs_f64();
                }
                assert!(started.elapsed() < Duration::from_secs(60), "not drained");
                std::thread::sleep(Duration::from_millis(50));
            };
            for worker in workers {
                worker.join().unwrap();
            }
            assert_eq!(
                Store::query(&test.0, None, 1000).unwrap().len(),
                usize::from(inputs).min(1000)
            );
            results.push(serde_json::json!({"consumers":consumers,"inputs":inputs,
                "drain_seconds":drained,"transactions_per_second":f64::from(inputs)/drained}));
        }
        println!("{}", serde_json::Value::Array(results));
    }

    #[test]
    fn a_stale_consumer_cannot_rewind_stored_positions() {
        use crate::storage::Acknowledge;
        let test = TestDatabase::new();
        let mut store = Store::open(&test.0).unwrap();
        for offset in [10, 5] {
            store.persist_batch_acknowledged(
                &[],
                Some(&Acknowledge::Kafka {
                    group: "g",
                    topic: "t",
                    offsets: &[(0, offset)],
                    retained: &[],
                }),
            );
        }
        let stored: i64 = test
            .sql()
            .query_one("SELECT next_offset FROM kafka_offsets", &[])
            .unwrap()
            .get(0);
        assert_eq!(stored, 10);
    }

    // Crash matrix (#36). Consumer crashes kill a real child process; producer crashes are
    // reproduced from the state they leave, since the stream loop has no offline source yet (#59).

    /// Runs `consume` in a child copy of this test binary so a test can SIGKILL it.
    fn spawn_consumer(test: &TestDatabase, topic: &str) -> std::process::Child {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "kafka::consumer_tests::child_consumer",
                "--exact",
                "--ignored",
            ])
            .env(
                "INDEXER_TEST_CHILD_CONSUMER",
                format!("{} {topic}", test.0.session()),
            )
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap()
    }

    #[test]
    #[ignore = "run as a child process by the crash tests"]
    fn child_consumer() {
        let Ok(spec) = std::env::var("INDEXER_TEST_CHILD_CONSUMER") else {
            return;
        };
        let (session, topic) = spec.split_once(' ').unwrap();
        let db = crate::storage::Database::new(TestDatabase::url(), session).unwrap();
        let _ = consume(&db, &test_brokers(), topic, 60);
    }

    fn wait_for_positions(test: &TestDatabase, expected: i64) {
        let started = std::time::Instant::now();
        while stored_positions(test) < expected {
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "positions stalled"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Waits until a session backend is blocked on a lock: the child is inside its write.
    fn wait_for_lock_waiter(test: &TestDatabase) {
        let started = std::time::Instant::now();
        loop {
            let waiting: i64 = test
                .sql()
                .query_one(
                    "SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND query ILIKE '%kafka_offsets%'",
                    &[],
                )
                .unwrap()
                .get(0);
            if waiting > 0 {
                return;
            }
            assert!(
                started.elapsed() < Duration::from_secs(30),
                "the child never reached its write"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn consume_in_background(
        test: &TestDatabase,
        topic: &str,
        seconds: u64,
    ) -> std::thread::JoinHandle<serde_json::Value> {
        let (db, name) = (test.0.clone(), topic.to_string());
        std::thread::spawn(move || consume(&db, &test_brokers(), &name, seconds).unwrap())
    }

    fn live_coverage(test: &TestDatabase) -> serde_json::Value {
        let report = crate::live::Intake::report(&test.0).unwrap();
        serde_json::json!([report["coverage"], report["verified_boundary"]])
    }

    #[test]
    fn a_consumer_killed_before_or_after_its_commit_stores_each_transfer_once() {
        let test = TestDatabase::new();
        let topic = TestTopic::new(2);
        produce(
            &topic.0,
            &(1..=6u8)
                .map(|n| signed(n, 100 + u64::from(n)))
                .collect::<Vec<_>>(),
        );
        Store::open(&test.0).unwrap();

        // Killed while its batch write waits on the position row: nothing is committed.
        let mut lock = test.sql();
        lock.batch_execute("BEGIN; LOCK TABLE kafka_offsets IN EXCLUSIVE MODE;")
            .unwrap();
        let mut child = spawn_consumer(&test, &topic.0);
        wait_for_lock_waiter(&test);
        child.kill().unwrap();
        child.wait().unwrap();
        lock.batch_execute("COMMIT;").unwrap();
        assert_eq!(stored_positions(&test), 0);
        assert_eq!(Store::query(&test.0, None, 100).unwrap().len(), 0);
        // The dead member holds its partitions until the session timeout moves them.
        let restarted = consume_in_background(&test, &topic.0, 25);
        wait_for_positions(&test, 6);
        assert_eq!(
            restarted.join().unwrap()["inserted_transfers"],
            6,
            "the killed batch is replayed"
        );

        // Killed after its batch committed: the restart replays nothing already stored.
        produce(
            &topic.0,
            &(7..=10u8)
                .map(|n| signed(n, 100 + u64::from(n)))
                .collect::<Vec<_>>(),
        );
        let mut child = spawn_consumer(&test, &topic.0);
        wait_for_positions(&test, 10);
        child.kill().unwrap();
        child.wait().unwrap();
        let after = consume_in_background(&test, &topic.0, 25);
        // One new input proves the restart owns the partitions, so its count is not vacuous.
        produce(&topic.0, &[signed(11, 111)]);
        wait_for_positions(&test, 11);
        assert_eq!(
            after.join().unwrap()["consumed"],
            1,
            "committed inputs are not replayed"
        );
        assert_eq!(Store::query(&test.0, None, 100).unwrap().len(), 11);
        // Delivery and stored positions never claim coverage.
        assert_eq!(live_coverage(&test), serde_json::json!(["unknown", null]));
    }

    #[test]
    fn a_rebalance_mid_run_stores_each_transfer_once() {
        let test = TestDatabase::new();
        let topic = TestTopic::new(6);
        Store::open(&test.0).unwrap();
        let first = {
            let (db, name) = (test.0.clone(), topic.0.clone());
            std::thread::spawn(move || consume(&db, &test_brokers(), &name, 20).unwrap())
        };
        produce(
            &topic.0,
            &(1..=20u8)
                .map(|n| signed(n, 200 + u64::from(n)))
                .collect::<Vec<_>>(),
        );
        std::thread::sleep(Duration::from_secs(3));
        // A second member joins; partitions move while the first may hold a buffered batch.
        let second = {
            let (db, name) = (test.0.clone(), topic.0.clone());
            std::thread::spawn(move || consume(&db, &test_brokers(), &name, 14).unwrap())
        };
        // Input after the join lands on partitions now split between both members.
        std::thread::sleep(Duration::from_secs(5));
        produce(
            &topic.0,
            &(21..=40u8)
                .map(|n| signed(n, 200 + u64::from(n)))
                .collect::<Vec<_>>(),
        );
        let reports = [first.join().unwrap(), second.join().unwrap()];
        assert_eq!(Store::query(&test.0, None, 100).unwrap().len(), 40);
        assert_eq!(stored_positions(&test), 40);
        let consumed: u64 = reports
            .iter()
            .map(|r| r["consumed"].as_u64().unwrap())
            .sum();
        assert!(consumed >= 40, "redelivery is allowed, loss is not");
        assert!(
            reports[1]["consumed"].as_u64().unwrap() > 0,
            "partitions moved to the joining member"
        );
        assert_eq!(live_coverage(&test), serde_json::json!(["unknown", null]));
    }

    #[test]
    fn a_producer_crash_before_publish_or_before_its_cursor_is_replayed_once() {
        use crate::storage::live::LiveLog;
        let mint = crate::live::USDC;
        let test = TestDatabase::new();
        let topic = TestTopic::new(2);
        // Slot 800 is older than the reorder margin below the cursor, so it is not replayed.
        let stream: Vec<_> = std::iter::once(signed(9, 800))
            .chain((1..=8u8).map(|n| signed(n, 1_000 + u64::from(n))))
            .collect();
        // Run 1 delivers 800 and 1001-1005 and stores its cursor, then crashes before 1006.
        let mut producer = IntakeProducer::new(&test_brokers(), &topic.0).unwrap();
        for tx in &stream[..6] {
            producer
                .send(tx.slot, &tx.encode_to_vec(), PROTO_SCHEMA)
                .unwrap();
        }
        let cursor = producer.flush(Duration::from_secs(20)).unwrap().unwrap();
        assert_eq!(cursor, 1_005);
        LiveLog::open(&test.0)
            .unwrap()
            .advance_producer_cursor(mint, cursor as i64)
            .unwrap();
        // Run 2 resumes, delivers 1006-1008, then crashes before storing its cursor.
        produce(&topic.0, &stream[6..]);
        // Run 3 resumes from the cursor still stored, so the stream replays everything after it.
        let report = crate::live::Intake::report(&test.0).unwrap();
        let resume = report["producer_cursors"][0]["resume_from"]
            .as_u64()
            .unwrap();
        assert_eq!(resume, 1_005 - super::REORDER_MARGIN_SLOTS);
        assert!(resume <= 1_006, "the unpublished slot is replayed");
        let replayed: Vec<_> = stream
            .iter()
            .filter(|tx| tx.slot >= resume)
            .cloned()
            .collect();
        produce(&topic.0, &replayed);
        let consumed = consume(&test.0, &test_brokers(), &topic.0, 6).unwrap();
        assert_eq!(consumed["consumed"], 17, "duplicates reach the consumer");
        assert_eq!(Store::query(&test.0, None, 100).unwrap().len(), 9);
        assert_eq!(live_coverage(&test), serde_json::json!(["unknown", null]));
    }
}
