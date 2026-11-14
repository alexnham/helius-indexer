//! Opt-in check against a hosted PostgreSQL. Never runs by default: it needs
//! INDEXER_HOSTED_DATABASE_URL (never printed) and, for self-signed servers,
//! INDEXER_DB_TLS_SHA256 obtained out of band. Uses a throwaway session it drops.
use helius_indexer::{
    decoder,
    storage::{Database, Store},
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
    assert_eq!(stored, inserted);
    println!(
        "{}",
        json!({"connect_ms":connect_ms,"round_trip_ms_median":round_trips[5],"round_trip_ms_max":round_trips[9],
            "transactions":cases.len(),"transfers":inserted,"transactions_per_second":cases.len() as f64/seconds,
            "coverage":"not_applicable"})
    );
}
