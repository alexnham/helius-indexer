//! Kafka as the durable live intake log (#36). Until compressed object storage holds
//! replay inputs (#40), the topic itself is the raw retention, so it keeps 37 days with
//! no byte cap; watch broker disk usage instead of letting Kafka delete early.
use crate::{Error, Result};
use rdkafka::{
    ClientConfig,
    admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication},
    client::DefaultClientContext,
    error::RDKafkaErrorCode,
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
