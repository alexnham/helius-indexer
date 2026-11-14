//! Opt-in check against a hosted PostgreSQL. Never runs by default: it needs
//! INDEXER_HOSTED_DATABASE_URL (never printed) and, for self-signed servers,
//! INDEXER_DB_TLS_SHA256 obtained out of band. Uses a throwaway session it drops.
use helius_indexer::{
    decoder,
    storage::{BatchItem, Database, Store},
};
use serde_json::{Value, json};
use std::time::Instant;

#[test]
#[ignore = "opt-in: contacts the hosted database named by INDEXER_HOSTED_DATABASE_URL"]
fn hosted_database_tls_latency_and_write_throughput() {
    let url =
        std::env::var("INDEXER_HOSTED_DATABASE_URL").expect("set INDEXER_HOSTED_DATABASE_URL");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    let db = Database::new(url, &format!("hosted_check_{nanos}")).unwrap();
    // Drops the throwaway schema even when an assertion fails part way.
    struct Cleanup(Database);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = self.0.test_client().batch_execute(&format!(
                "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                self.0.session()
            ));
        }
    }
    let _cleanup = Cleanup(db.clone());
    let started = Instant::now();
    let mut sql = db.test_client();
    let connect_ms = started.elapsed().as_secs_f64() * 1000.0;
    let mut round_trips: Vec<f64> = (0..10)
        .map(|_| {
            let started = Instant::now();
            sql.query_one("SELECT 1", &[]).unwrap();
            started.elapsed().as_secs_f64() * 1000.0
        })
        .collect();
    round_trips.sort_by(f64::total_cmp);
    let cases: Vec<Value> =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-coverage.json")).unwrap();
    let mut store = Store::open(&db).unwrap();
    let started = Instant::now();
    let mut inserted = 0;
    for case in &cases {
        let raw = serde_json::to_vec(&case["capture"]).unwrap();
        let capture = decoder::parse_capture(&raw).unwrap();
        inserted += store
            .persist(&capture, &raw, &decoder::decode(&capture))
            .unwrap();
    }
    let seconds = started.elapsed().as_secs_f64();
    let stored = Store::query(&db, None, 1000).unwrap().len();
    // The same transactions as one batch, in a second throwaway session.
    let batch_db = Database::new(
        std::env::var("INDEXER_HOSTED_DATABASE_URL").unwrap(),
        &format!("hosted_batch_{nanos}"),
    )
    .unwrap();
    let _batch_cleanup = Cleanup(batch_db.clone());
    let decoded: Vec<_> = cases
        .iter()
        .map(|case| {
            let raw = serde_json::to_vec(&case["capture"]).unwrap();
            let capture = decoder::parse_capture(&raw).unwrap();
            let transfers = decoder::decode(&capture);
            (capture, transfers, raw)
        })
        .collect();
    let items: Vec<BatchItem> = decoded
        .iter()
        .map(|(capture, transfers, raw)| BatchItem {
            capture,
            raw,
            decoded: transfers,
        })
        .collect();
    let mut batch_store = Store::open(&batch_db).unwrap();
    let (warm, steady) = items.split_at(items.len() / 2);
    // The first batch also creates the slot partition and prepares the batch statements.
    let started = Instant::now();
    let mut batched: usize = batch_store
        .persist_batch(warm)
        .into_iter()
        .map(|r| r.unwrap())
        .sum();
    let first_batch_seconds = started.elapsed().as_secs_f64();
    let started = Instant::now();
    batched += batch_store
        .persist_batch(steady)
        .into_iter()
        .map(|r| r.unwrap())
        .sum::<usize>();
    let steady_seconds = started.elapsed().as_secs_f64();
    assert_eq!(batched, inserted);
    // A full live batch: 100 distinct signatures over the captured mainnet transaction.
    let template: Value =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-transfer.json")).unwrap();
    let full: Vec<_> = (0..100u8)
        .map(|n| {
            let mut value = template.clone();
            // Same epoch as the warm-up batches, so no partition is created while timing.
            value["result"]["slot"] = cases[0]["capture"]["result"]["slot"].clone();
            value["result"]["transaction"]["signatures"][0] =
                json!(bs58::encode([n.max(1); 64]).into_string());
            let raw = serde_json::to_vec(&value).unwrap();
            let capture = decoder::parse_capture(&raw).unwrap();
            let transfers = decoder::decode(&capture);
            (capture, transfers, raw)
        })
        .collect();
    let full_items: Vec<BatchItem> = full
        .iter()
        .map(|(capture, transfers, raw)| BatchItem {
            capture,
            raw,
            decoded: transfers,
        })
        .collect();
    let started = Instant::now();
    let full_results = batch_store.persist_batch(&full_items);
    let full_seconds = started.elapsed().as_secs_f64();
    assert!(full_results.iter().all(|r| r.is_ok()));
    assert_eq!(batch_store.stats().fallbacks, 0);
    assert_eq!(stored, inserted);
    println!(
        "{}",
        json!({"connect_ms":connect_ms,"round_trip_ms_median":round_trips[5],"round_trip_ms_max":round_trips[9],
            "transactions":cases.len(),"transfers":inserted,"transactions_per_second":cases.len() as f64/seconds,
            "first_batch":{"transactions":warm.len(),"seconds":first_batch_seconds},
            "full_batch":{"transactions":full_items.len(),"seconds":full_seconds,"transactions_per_second":full_items.len() as f64/full_seconds},
            "steady_batch":{"transactions":steady.len(),"seconds":steady_seconds,"round_trips_estimate":steady_seconds*1000.0/round_trips[5]},
            "coverage":"not_applicable"})
    );
}
