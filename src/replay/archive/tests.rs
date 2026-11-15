use super::*;
use crate::{
    live::USDC,
    replay::{route::measure_with, status},
    storage::pg::TestDatabase,
};
use std::collections::BTreeMap;

const ARCHIVE: &str = "old-faithful-jetstreamer";

struct Source {
    blocks: BTreeMap<u64, Vec<u8>>,
    calls: Vec<u64>,
}
impl BlockSource for Source {
    fn provenance(&self) -> &'static str {
        ARCHIVE
    }
    fn capture_source(&self) -> &'static str {
        ARCHIVE
    }
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        self.calls.push(slot);
        self.blocks
            .get(&slot)
            .cloned()
            .ok_or(Error("test_source_unavailable"))
    }
}
fn block(slot: u64, with_transfer: bool) -> Vec<u8> {
    let fixture: Value = serde_json::from_slice(include_bytes!(
        "../../../tests/fixtures/mainnet-transfer.json"
    ))
    .unwrap();
    let txs = if with_transfer {
        vec![fixture["result"].clone()]
    } else {
        vec![]
    };
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{"blockhash":bs58::encode([slot as u8;32]).into_string(),"previousBlockhash":bs58::encode([(slot-1) as u8;32]).into_string(),"parentSlot":slot-1,"blockTime":1794430000i64+slot as i64,"transactions":txs}})).unwrap()
}
fn source(slots: impl IntoIterator<Item = u64>) -> Source {
    Source {
        blocks: slots.into_iter().map(|s| (s, block(s, s == 101))).collect(),
        calls: vec![],
    }
}
/// Job 100..=149 routed archive 100..=109, uncovered 110..=129, replay 130..=149.
fn routed(db: &Database, archive_last: u64, replay_first: u64) {
    crate::replay::create(db, "job", USDC, 100, 149).unwrap();
    measure_with(
        db,
        "job",
        || Ok((Some(replay_first), 500)),
        || Ok(Some(archive_last)),
        &mut Telemetry::default(),
    )
    .unwrap();
}
fn work(db: &Database, source: &mut Source) -> Result<Value> {
    batch(
        db,
        "job",
        Default::default(),
        source,
        |_| (1, 2, 3),
        || false,
        &mut Telemetry::default(),
    )
}

#[test]
fn unrouted_and_replay_only_jobs_charge_nothing() {
    let db = TestDatabase::new();
    crate::replay::create(&db.0, "job", USDC, 100, 149).unwrap();
    let mut empty = source([]);
    assert_eq!(work(&db.0, &mut empty), Err(Error("replay_route_missing")));
    measure_with(
        &db.0,
        "job",
        || Ok((Some(90), 500)),
        || panic!("not probed"),
        &mut Telemetry::default(),
    )
    .unwrap();
    let report = work(&db.0, &mut empty).unwrap();
    assert_eq!(report["reason"], "no_pending_archive_slots");
    assert_eq!(report["job"]["archive"]["batches_charged"], 0);
    assert!(empty.calls.is_empty());
}

#[test]
fn batches_are_charged_before_work_and_resume_at_the_first_incomplete_slot() {
    let db = TestDatabase::new();
    routed(&db.0, 109, 130);
    // 108 is unavailable on the first pass.
    let mut first = source((100..=107).chain([109]));
    let report = work(&db.0, &mut first).unwrap();
    assert_eq!(
        (report["start"].clone(), report["end"].clone()),
        (json!(100), json!(107))
    );
    assert_eq!(report["status"], "finished");
    let report = work(&db.0, &mut first).unwrap();
    assert_eq!(
        (report["start"].clone(), report["end"].clone()),
        (json!(108), json!(109))
    );
    assert_eq!(report["status"], "incomplete");
    assert_eq!(first.calls, (100..=109).collect::<Vec<_>>());

    // Restart with a fresh source: only the incomplete slot is fetched again.
    let mut second = source([108]);
    let report = work(&db.0, &mut second).unwrap();
    assert_eq!(report["status"], "finished");
    assert_eq!(second.calls, vec![108]);

    let job = &report["job"];
    assert_eq!(job["archive"]["batches_charged"], 3);
    assert_eq!(job["archive"]["batches_remaining"], 1);
    assert_eq!(job["archive"]["attempts"][0]["limits"]["requests"], 32);
    assert_eq!(
        job["archive"]["attempts"][0]["observed"]["cached_chunks"],
        3
    );
    assert_eq!(job["archive"]["by_status"]["incomplete"]["attempts"], 1);
    let slots = &job["archive"]["slot_status"];
    assert_eq!(slots["evidence"], "full_block_slot_status");
    assert_eq!(
        slots["segments"],
        json!([{"start":100,"end":109,"kind":"archive","complete":10,"skipped":0,"pending":0,"oldest_pending_age_seconds":0}])
    );
    // Archive slot status never becomes replay or range coverage.
    assert_eq!(job["coverage"], "unknown");
    assert_eq!(job["routing"]["state"], "routed_not_processed");
    assert_eq!(
        crate::storage::Store::query(&db.0, Some(USDC), 10)
            .unwrap()
            .len(),
        2,
        "the fixture transaction holds two transfers"
    );
    let archived: i64 = db
        .sql()
        .query_one(
            "SELECT COUNT(*) FROM historical_source_slots WHERE source_kind=$1 AND status='complete'",
            &[&ARCHIVE],
        )
        .unwrap()
        .get(0);
    assert_eq!(archived, 10);

    // Nothing left: no charge, no source call.
    let report = work(&db.0, &mut second).unwrap();
    assert_eq!(report["reason"], "no_pending_archive_slots");
    assert_eq!(report["job"]["archive"]["batches_charged"], 3);
}

#[test]
fn a_failing_source_still_consumes_its_charge_and_exhaustion_needs_a_grant() {
    let db = TestDatabase::new();
    routed(&db.0, 109, 130);
    let mut failing = source([]);
    for _ in 0..4 {
        assert_eq!(work(&db.0, &mut failing).unwrap()["status"], "incomplete");
    }
    assert_eq!(
        work(&db.0, &mut failing),
        Err(Error("archive_budget_exhausted"))
    );
    let report = status(&db.0, "job").unwrap();
    assert_eq!(report["archive"]["batches_remaining"], 0);
    assert_eq!(
        report["archive"]["slot_status"]["segments"][0]["pending"],
        10
    );
    grant(&db.0, "job", &mut Telemetry::default()).unwrap();
    // A replay grant does not replenish archive work.
    let mut ok = source(100..=109);
    assert_eq!(work(&db.0, &mut ok).unwrap()["status"], "finished");
    assert_eq!(
        status(&db.0, "job").unwrap()["archive"]["batches_charged"],
        5
    );
}

#[test]
fn a_crash_keeps_the_charge_and_one_worker_owns_the_job() {
    let db = TestDatabase::new();
    routed(&db.0, 109, 130);
    let held = JobLog::archive_reserve(&db.0, "job", 1, 100, 107).unwrap();
    let mut ok = source(100..=109);
    assert_eq!(work(&db.0, &mut ok), Err(Error("archive_job_busy")));
    assert!(ok.calls.is_empty());
    // Dropping without finishing is a crash: the lock goes, the charge stays.
    drop(held);
    assert_eq!(work(&db.0, &mut ok).unwrap()["status"], "finished");
    let report = status(&db.0, "job").unwrap();
    assert_eq!(report["archive"]["batches_charged"], 2);
    assert_eq!(report["archive"]["attempts"][1]["status"], "interrupted");
}

#[test]
fn cancellation_and_overlap_segments() {
    let db = TestDatabase::new();
    // Archive through 109 and replay from 105: 105..=109 is overlap.
    routed(&db.0, 109, 105);
    let mut ok = source(100..=109);
    let report = batch(
        &db.0,
        "job",
        Default::default(),
        &mut ok,
        |_| (0, 0, 0),
        || true,
        &mut Telemetry::default(),
    )
    .unwrap();
    assert_eq!(report["status"], "cancelled");
    assert!(ok.calls.is_empty());
    work(&db.0, &mut ok).unwrap();
    let report = work(&db.0, &mut ok).unwrap();
    assert_eq!(
        (report["start"].clone(), report["end"].clone()),
        (json!(105), json!(109))
    );
    let segments = &report["job"]["archive"]["slot_status"]["segments"];
    assert_eq!(segments[1]["kind"], "overlap");
    assert_eq!(segments[1]["complete"], 5);
    // Overlap is archive-complete here, not reconciled against replay.
    assert_eq!(report["job"]["coverage"], "unknown");
    assert_eq!(segments.as_array().unwrap().len(), 2);
}

#[test]
fn only_the_latest_route_revision_is_worked() {
    let db = TestDatabase::new();
    routed(&db.0, 109, 130);
    // Re-measured: replay now reaches the whole job, so no archive slots remain.
    measure_with(
        &db.0,
        "job",
        || Ok((Some(100), 500)),
        || panic!("not probed"),
        &mut Telemetry::default(),
    )
    .unwrap();
    let mut ok = source(100..=109);
    let report = work(&db.0, &mut ok).unwrap();
    assert_eq!(report["reason"], "no_pending_archive_slots");
    assert!(ok.calls.is_empty());
    assert_eq!(report["job"]["archive"]["slot_status"]["route_revision"], 2);
    assert_eq!(
        report["job"]["archive"]["slot_status"]["segments"],
        json!([])
    );
}
