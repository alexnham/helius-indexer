use super::*;
use crate::storage::Store;
use crate::storage::pg::TestDatabase;
use std::collections::BTreeMap;

struct Source {
    archive: bool,
    blocks: BTreeMap<u64, Vec<u8>>,
    calls: Vec<u64>,
}
impl BlockSource for Source {
    fn provenance(&self) -> &'static str {
        if self.archive {
            archive::SOURCE
        } else {
            "finalized-mainnet-rpc"
        }
    }
    fn capture_source(&self) -> &'static str {
        if self.archive {
            archive::SOURCE
        } else {
            "solana-mainnet-rpc"
        }
    }
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        self.calls.push(slot);
        self.blocks
            .get(&slot)
            .cloned()
            .ok_or(Error("test_source_unavailable"))
    }
}
fn block(slot: u64, parent: u64, with_transfer: bool) -> Vec<u8> {
    let fixture: Value = serde_json::from_slice(include_bytes!(
        "../../../tests/fixtures/mainnet-transfer.json"
    ))
    .unwrap();
    let txs = if with_transfer {
        vec![fixture["result"].clone()]
    } else {
        vec![]
    };
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{"blockhash":bs58::encode([slot as u8;32]).into_string(),"previousBlockhash":bs58::encode([parent as u8;32]).into_string(),"parentSlot":parent,"blockTime":1794430000i64+slot as i64,"transactions":txs}})).unwrap()
}
fn setup(end: u64) -> (TestDatabase, Database, Plan, Source, Source) {
    let dir = TestDatabase::new();
    let path = dir.0.clone();
    let plan = Plan {
        mint: crate::live::USDC.into(),
        start: 10,
        tip: 12,
        end,
        snapshot: json!({"last_observed_archive_slot":12,"reference_slot":end,"tip_exact_within_probed_epochs":true,"measured_at_unix_seconds":1}),
    };
    save(&BridgeLog::open(&path).unwrap(), &plan).unwrap();
    let a = Source {
        archive: true,
        blocks: (10..=12).map(|s| (s, block(s, s - 1, s == 12))).collect(),
        calls: vec![],
    };
    let r = Source {
        archive: false,
        blocks: (12..=end).map(|s| (s, block(s, s - 1, s == 12))).collect(),
        calls: vec![],
    };
    (dir, path, plan, a, r)
}
fn work(path: &Database, plan: &Plan, a: &mut Source, r: &mut Source) -> Value {
    batch(
        path,
        plan,
        a,
        r,
        Arc::new(WriteGate::default()),
        &mut Telemetry::default(),
        || false,
    )
    .unwrap()
}
#[test]
fn routes_both_sides_verifies_overlap_and_resume_is_duplicate_free() {
    let (_dir, path, plan, mut a, mut r) = setup(15);
    let first = work(&path, &plan, &mut a, &mut r);
    assert_eq!(a.calls, vec![10, 11, 12]);
    assert_eq!(r.calls, vec![12, 13, 14, 15]);
    assert_eq!(first["historical"]["complete"], true);
    assert_eq!(first["historical"]["overlap_verified"], true);
    assert_eq!(first["historical"]["contiguous_through"], 15);
    assert_eq!(first["live_tail"]["coverage"], "unknown");
    assert_eq!(first["live_tail"]["start"], 16);
    assert_eq!(
        Store::summary(&path, &plan.mint).unwrap()["transfer_count"],
        2
    );
    let retry = work(&path, &plan, &mut a, &mut r);
    assert_eq!(retry["attempts"], json!([]));
    assert_eq!(a.calls.len(), 3);
    assert_eq!(r.calls.len(), 4);
    assert_eq!(Store::query(&path, None, 100).unwrap().len(), 2);
    assert_eq!(
        status(&path, &plan.mint, plan.start).unwrap()["historical"]["pending_slots"],
        0
    );
}
#[test]
fn source_failure_keeps_checkpoint_before_boundary_and_resume_repairs_it() {
    let (_dir, path, plan, mut a, mut r) = setup(15);
    let tip = r.blocks.remove(&12).unwrap();
    let first = work(&path, &plan, &mut a, &mut r);
    assert_eq!(first["historical"]["complete"], false);
    assert_eq!(first["historical"]["contiguous_through"], 11);
    assert_eq!(first["historical"]["pending_slots"], 1);
    assert_eq!(first["historical"]["overlap_verified"], false);
    r.blocks.insert(12, tip);
    let repaired = work(&path, &plan, &mut a, &mut r);
    assert_eq!(repaired["historical"]["complete"], true);
    assert_eq!(
        Store::summary(&path, &plan.mint).unwrap()["transfer_count"],
        2
    );
}
#[test]
fn crash_between_output_and_boundary_proof_cannot_claim_completion() {
    let (_dir, path, plan, mut a, mut r) = setup(15);
    path.test_client().batch_execute("CREATE OR REPLACE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_proof BEFORE INSERT ON bridge_overlap_proofs FOR EACH ROW EXECUTE FUNCTION fail();").unwrap();
    let interrupted = work(&path, &plan, &mut a, &mut r);
    assert_eq!(interrupted["historical"]["complete"], false);
    assert_eq!(interrupted["historical"]["overlap_verified"], false);
    assert_eq!(Store::query(&path, None, 100).unwrap().len(), 2);
    path.test_client()
        .batch_execute("DROP TRIGGER fail_proof ON bridge_overlap_proofs;")
        .unwrap();
    assert_eq!(
        work(&path, &plan, &mut a, &mut r)["historical"]["complete"],
        true
    );
    assert_eq!(Store::query(&path, None, 100).unwrap().len(), 2);
    path.test_client()
        .execute(
            "UPDATE historical_slots SET attempts=attempts+1 WHERE slot=12",
            &[],
        )
        .unwrap();
    assert_eq!(
        status(&path, &plan.mint, 10).unwrap()["historical"]["overlap_verified"],
        false
    );
}
#[test]
fn parent_evidence_resolves_skips_at_the_source_handoff() {
    let (_dir, path, plan, mut a, mut r) = setup(15);
    a.blocks.remove(&11);
    a.blocks.insert(12, block(12, 10, true));
    r.blocks.insert(12, block(12, 10, true));
    let result = work(&path, &plan, &mut a, &mut r);
    assert_eq!(result["historical"]["complete"], true);
    assert_eq!(
        History::open(&path)
            .unwrap()
            .coverage(&plan.mint, 10, 15)
            .unwrap()
            .slots[1]
            .status,
        "skipped"
    );
}
#[test]
fn cancellation_and_batch_limits_leave_unscheduled_work_visible() {
    let (_dir, path, plan, mut a, mut r) = setup(25);
    let cancelled = batch(
        &path,
        &plan,
        &mut a,
        &mut r,
        Arc::new(WriteGate::default()),
        &mut Telemetry::default(),
        || true,
    )
    .unwrap();
    assert!(a.calls.is_empty() && r.calls.is_empty());
    assert_eq!(cancelled["historical"]["pending_slots"], 16);
    let first = work(&path, &plan, &mut a, &mut r);
    assert_eq!(first["historical"]["contiguous_through"], 17);
    assert_eq!(first["historical"]["pending_slots"], 8);
    assert_eq!(a.calls.len() + r.calls.len(), 9);
    assert_eq!(
        work(&path, &plan, &mut a, &mut r)["historical"]["complete"],
        true
    );
}
#[test]
fn frozen_plan_is_revision_scoped_and_large_gap_report_is_bounded() {
    let (_dir, path, mut plan, _a, _r) = setup(15);
    let db = BridgeLog::open(&path).unwrap();
    plan.end = 1_000_000;
    plan.snapshot["reference_slot"] = json!(plan.end);
    // Save never moves the same plan's endpoint implicitly.
    save(&db, &plan).unwrap();
    assert_eq!(load(&db, &plan.mint, 10).unwrap().unwrap().end, 15);
    let state = progress(&db, &plan).unwrap();
    assert_eq!(state.pending_slots, 999991);
    assert_eq!(state.unfinished.len(), 2);
    assert_eq!(state.unfinished[0].source_kind, "archive");
    assert_eq!(state.unfinished[1].source_kind, "rpc");
    path.test_client()
        .execute("UPDATE bridge_plans SET decoder='old'", &[])
        .unwrap();
    assert!(load(&db, &plan.mint, 10).unwrap().is_none());
    plan.snapshot["tip_exact_within_probed_epochs"] = json!(false);
    assert!(save(&db, &plan).is_err());
}
#[test]
fn conflicting_cross_source_parent_link_fails_before_new_transfers() {
    let (_dir, path, plan, mut a, mut r) = setup(15);
    let mut bad: Value = serde_json::from_slice(&r.blocks[&13]).unwrap();
    bad["result"]["previousBlockhash"] = json!(bs58::encode([99; 32]).into_string());
    r.blocks.insert(13, serde_json::to_vec(&bad).unwrap());
    let result = work(&path, &plan, &mut a, &mut r);
    assert_eq!(result["historical"]["contiguous_through"], 12);
    assert_eq!(result["historical"]["pending_slots"], 1);
    assert_eq!(
        History::open(&path)
            .unwrap()
            .coverage(&plan.mint, 13, 13)
            .unwrap()
            .slots[0]
            .error_class
            .as_deref(),
        Some("block_parent_conflict")
    );
    assert_eq!(Store::query(&path, None, 100).unwrap().len(), 2);
}

#[test]
fn skipped_run_longer_than_a_batch_does_not_starve_its_produced_child() {
    let (_dir, path, mut plan, mut a, mut r) = setup(32);
    let db = BridgeLog::open(&path).unwrap();
    path.test_client()
        .execute("DELETE FROM bridge_plans", &[])
        .unwrap();
    plan.tip = 30;
    plan.snapshot["last_observed_archive_slot"] = json!(30);
    save(&db, &plan).unwrap();
    a.blocks = (26..=30)
        .map(|s| (s, block(s, if s == 26 { 9 } else { s - 1 }, false)))
        .collect();
    r.blocks = (30..=32).map(|s| (s, block(s, s - 1, false))).collect();
    for _ in 0..5 {
        work(&path, &plan, &mut a, &mut r);
    }
    let result = status(&path, &plan.mint, 10).unwrap();
    assert_eq!(result["historical"]["complete"], true);
    assert_eq!(result["historical"]["contiguous_through"], 32);
    assert!(a.calls.contains(&26));
    let mut invalid = plan.clone();
    invalid.start = 31;
    assert!(save(&db, &invalid).is_err());
}

#[test]
fn expired_rpc_budget_fails_before_any_network_request() {
    let mut source =
        RpcSource::bounded("https://api.mainnet-beta.solana.com".into(), Instant::now()).unwrap();
    assert_eq!(source.block(12).unwrap_err().0, "rpc_time_limit");
}
#[test]
fn conflicting_overlap_cannot_acknowledge_either_source_as_a_verified_boundary() {
    let (_dir, path, plan, mut a, mut r) = setup(15);
    let mut value: Value = serde_json::from_slice(&r.blocks[&12]).unwrap();
    value["result"]["blockTime"] = json!(99);
    r.blocks.insert(12, serde_json::to_vec(&value).unwrap());
    let report = work(&path, &plan, &mut a, &mut r);
    assert_eq!(report["historical"]["overlap_verified"], false);
    assert_eq!(report["historical"]["contiguous_through"], 11);
    assert_eq!(
        report["attempts"][1]["error_class"],
        "archive_bridge_block_conflict"
    );
}

#[test]
fn elapsed_budget_rotates_scheduling_without_advancing_verified_coverage() {
    let (_dir, path, plan, mut a, mut r) = setup(25);
    let checks = std::cell::Cell::new(0);
    let stopped = batch(
        &path,
        &plan,
        &mut a,
        &mut r,
        Arc::new(WriteGate::default()),
        &mut Telemetry::default(),
        || {
            let n = checks.get();
            checks.set(n + 1);
            n > 3
        },
    )
    .unwrap();
    assert_eq!(stopped["cancelled"], true);
    assert_eq!(stopped["historical"]["contiguous_through"], 11);
    assert_eq!(stopped["historical"]["next_pending_slot"], 18);
    work(&path, &plan, &mut a, &mut r);
    assert!(r.calls.contains(&25));
    assert_eq!(
        work(&path, &plan, &mut a, &mut r)["historical"]["complete"],
        true
    );
}

#[test]
fn parent_arriving_after_child_must_match_its_retained_hash_link() {
    let (_dir, path, _plan, _a, _r) = setup(15);
    let mut store = Store::open(&path).unwrap();
    let child: Value = serde_json::from_slice(&block(13, 12, false)).unwrap();
    store
        .observe_block_identity(13, &child["result"], &[])
        .unwrap();
    let mut parent: Value = serde_json::from_slice(&block(12, 11, false)).unwrap();
    parent["result"]["blockhash"] = json!(bs58::encode([99; 32]).into_string());
    assert_eq!(
        store
            .observe_block_identity(12, &parent["result"], &[])
            .unwrap_err()
            .0,
        "block_parent_conflict"
    );
    assert_eq!(
        path.test_client()
            .query_one("SELECT COUNT(*) FROM historical_block_identity", &[])
            .unwrap()
            .get::<_, i64>(0),
        1
    );
}
#[test]
fn missing_interval_samples_truncate_without_losing_total_counts() {
    let (_dir, path, plan, _a, _r) = setup(120);
    let db = BridgeLog::open(&path).unwrap();
    for slot in (14i64..=120).step_by(2) {
        path.test_client().execute("INSERT INTO historical_slots(mint,decoder,slot,status,first_pending) VALUES($1,$2,$3,'skipped',1)",&[&plan.mint, &(decoder::DECODER_VERSION), &slot]).unwrap();
    }
    let report = progress(&db, &plan).unwrap();
    assert!(report.ranges_truncated);
    assert_eq!(report.unfinished.len(), 32);
    assert!(report.missing_ranges > 32);
    assert_eq!(report.pending_slots, 57);
    assert_eq!(report.contiguous_through, None);
}

#[test]
fn corroborated_boundary_is_ready_but_native_rpc_stays_incomplete_even_after_restart() {
    let (_dir, path, plan, mut a, mut r) = setup(15);
    let cases: Vec<Value> = serde_json::from_slice(include_bytes!(
        "../../../tests/fixtures/mainnet-truncated-rpc.json"
    ))
    .unwrap();
    for (source, capture) in [(&mut a, &cases[0]["archive"]), (&mut r, &cases[0]["rpc"])] {
        let mut value: Value = serde_json::from_slice(&block(12, 11, false)).unwrap();
        value["result"]["transactions"] = json!([capture["result"]]);
        source
            .blocks
            .insert(12, serde_json::to_vec(&value).unwrap());
    }
    path.test_client().batch_execute("CREATE OR REPLACE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_pair_proof BEFORE INSERT ON bridge_overlap_proofs FOR EACH ROW EXECUTE FUNCTION fail();").unwrap();
    let interrupted = work(&path, &plan, &mut a, &mut r);
    assert_eq!(interrupted["historical"]["boundary_ready"], false);
    assert_eq!(interrupted["historical"]["complete"], false);
    assert!(!Store::query(&path, None, 100).unwrap().is_empty());
    path.test_client()
        .batch_execute("DROP TRIGGER fail_pair_proof ON bridge_overlap_proofs;")
        .unwrap();
    let ready = work(&path, &plan, &mut a, &mut r);
    assert_eq!(ready["historical"]["boundary_ready"], true);
    assert_eq!(ready["historical"]["complete"], true);
    assert_eq!(ready["historical"]["overlap_verified"], false);
    assert_eq!(ready["historical"]["overlap_recovered"], true);
    assert_eq!(ready["live_tail"]["coverage"], "unknown");
    let native:String=path.test_client().query_one("SELECT status FROM historical_source_slots WHERE slot=12 AND source_kind='finalized-mainnet-rpc'", &[]).unwrap().get(0);
    assert_eq!(native, "unknown");
    assert_eq!(
        status(&path, &plan.mint, 10).unwrap()["historical"]["boundary_ready"],
        true
    );
    path.test_client()
        .execute(
            "UPDATE historical_slots SET attempts=attempts+1 WHERE slot=12",
            &[],
        )
        .unwrap();
    assert_eq!(
        status(&path, &plan.mint, 10).unwrap()["historical"]["boundary_ready"],
        false
    );
    let count = Store::query(&path, None, 100).unwrap().len();
    work(&path, &plan, &mut a, &mut r);
    assert_eq!(Store::query(&path, None, 100).unwrap().len(), count);
}
