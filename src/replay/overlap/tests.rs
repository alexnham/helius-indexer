use super::*;
use crate::{
    backfill::BlockSource,
    live::{self, Intake, USDC},
    replay::{ReplayRun, archive, cycle, route::measure_with, status},
    storage::{Store, pg::TestDatabase, replay::Retained},
};
use laserstream_core_proto::geyser::SubscribeUpdateTransaction;

const SLOT: u64 = 102;

/// The replay fixture transaction, delivered at `SLOT`.
fn update() -> SubscribeUpdateTransaction {
    let mut tx = live::tests::update();
    tx.slot = SLOT;
    tx
}
/// The same transaction in the archive's full-block form.
fn archive_tx() -> Value {
    let capture: Value = serde_json::from_slice(&live::normalize(&update()).unwrap()).unwrap();
    capture["result"].clone()
}
struct Blocks(BTreeMap<u64, Vec<Value>>);
impl BlockSource for Blocks {
    fn provenance(&self) -> &'static str {
        ARCHIVE_SOURCE
    }
    fn capture_source(&self) -> &'static str {
        ARCHIVE_SOURCE
    }
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        let txs = self.0.get(&slot).cloned().unwrap_or_default();
        Ok(serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{"blockhash":bs58::encode([slot as u8;32]).into_string(),"previousBlockhash":bs58::encode([(slot-1) as u8;32]).into_string(),"parentSlot":slot-1,"blockTime":1794430000i64+slot as i64,"transactions":txs}})).unwrap())
    }
}
/// Job 100..=103: archive 100..=101, overlap 102..=103 (archive through 103, replay from 102).
fn setup() -> TestDatabase {
    let db = TestDatabase::new();
    crate::replay::create(&db.0, "job", USDC, 100, 103).unwrap();
    route(&db.0);
    db
}
fn route(db: &Database) {
    measure_with(
        db,
        "job",
        || Ok((Some(102), 500)),
        || Ok(Some(103)),
        &mut Telemetry::default(),
    )
    .unwrap();
}
fn archive_all(db: &Database, blocks: &mut Blocks) {
    while archive::batch(
        db,
        "job",
        Default::default(),
        blocks,
        |_| (0, 0, 0),
        || false,
        &mut Telemetry::default(),
    )
    .unwrap()["reason"]
        .is_null()
    {}
}
fn deliver_replay(db: &Database) {
    let mut run = ReplayRun {
        scope: JobLog::open(db).unwrap().scope("job").unwrap(),
        reservation: JobLog::reserve(db, "job", 5).unwrap(),
    };
    let mut intake = Intake::open(db).unwrap();
    if let Retained::Pending(id) = run.input(&update()).unwrap() {
        let result = intake.process(id, &update());
        run.reservation.acknowledge(id, &result).unwrap();
        result.unwrap();
    }
    run.reservation.finish("end_observed").unwrap();
}
fn with_tx() -> Blocks {
    Blocks([(SLOT, vec![archive_tx()])].into())
}
fn outcome_at(report: &Value) -> Value {
    report["job"]["verified"].clone()
}

#[test]
fn predicate_matches_the_replay_filter() {
    let tx = archive_tx();
    assert!(touches_mint(&tx, USDC));
    assert!(!touches_mint(
        &tx,
        "So11111111111111111111111111111111111111112"
    ));
    let mint = "So11111111111111111111111111111111111111112";
    assert!(touches_mint(
        &json!({"meta":{"loadedAddresses":{"writable":[],"readonly":[mint]}}}),
        mint
    ));
    assert!(touches_mint(
        &json!({"meta":{"postTokenBalances":[{"mint":mint}]}}),
        mint
    ));
    assert!(touches_mint(
        &json!({"transaction":{"message":{"accountKeys":[{"pubkey":mint}]}}}),
        mint
    ));
}

#[test]
fn both_arrival_orders_and_retries_agree_and_verify_the_overlap() {
    for replay_first in [true, false] {
        let db = setup();
        if replay_first {
            deliver_replay(&db.0);
        }
        archive_all(&db.0, &mut with_tx());
        if !replay_first {
            deliver_replay(&db.0);
        }
        let report = reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
        assert_eq!(
            report["outcomes"],
            json!({"consistent":2}),
            "replay first: {replay_first}"
        );
        // A retry appends a receipt and keeps the result.
        deliver_replay(&db.0);
        let report = reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
        let verified = outcome_at(&report);
        assert_eq!(verified["verified_through"], 103);
        assert_eq!(verified["archive_slots"], 2);
        assert_eq!(verified["overlap_slots"], 2);
        assert_eq!(verified["unknown_slots"], 0);
        assert_eq!(Store::query(&db.0, Some(USDC), 10).unwrap().len(), 1);
        let receipts: i64 = db
            .sql()
            .query_one("SELECT COUNT(*) FROM replay_overlap_outcomes", &[])
            .unwrap()
            .get(0);
        assert_eq!(receipts, 4);
    }
}

#[test]
fn replay_omission_conflict_and_pending_are_distinct() {
    // Archive holds a mint transaction replay never delivered: a subset, still verified.
    let db = setup();
    archive_all(&db.0, &mut with_tx());
    let report = reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
    assert_eq!(
        report["outcomes"],
        json!({"consistent":1,"replay_omission":1})
    );
    assert_eq!(outcome_at(&report)["verified_through"], 103);

    // Replay delivered a transaction a complete archive slot does not contain.
    let db = setup();
    deliver_replay(&db.0);
    archive_all(&db.0, &mut Blocks(BTreeMap::new()));
    let report = reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
    assert_eq!(report["outcomes"], json!({"conflict":1,"consistent":1}));
    let verified = outcome_at(&report);
    assert_eq!(
        verified["conflicts"],
        json!([{"slot":102,"reason":"replay_not_in_archive"}])
    );
    assert_eq!(verified["verified_through"], 101);

    // Archive not yet processed: pending, never verified.
    let db = setup();
    deliver_replay(&db.0);
    let report = reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
    assert_eq!(report["outcomes"], json!({"pending":2}));
    assert!(outcome_at(&report)["verified_through"].is_null());
}

#[test]
fn a_failed_archive_transaction_is_not_a_replay_omission() {
    // The replay filter excludes failed transactions, so their absence is expected.
    let mut failed = archive_tx();
    failed["transaction"]["signatures"][0] = json!(bs58::encode([7u8; 64]).into_string());
    failed["meta"]["err"] = json!({"InstructionError":[0,{"Custom":1}]});
    let db = setup();
    deliver_replay(&db.0);
    archive_all(
        &db.0,
        &mut Blocks([(SLOT, vec![archive_tx(), failed])].into()),
    );
    let report = reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
    assert_eq!(report["outcomes"], json!({"consistent":2}));
}

#[test]
fn differing_effects_conflict_and_a_decoder_revision_change_is_pending() {
    let db = setup();
    deliver_replay(&db.0);
    archive_all(&db.0, &mut Blocks(BTreeMap::new()));
    // An archive observation of the same signature with a different amount.
    let mut tx = archive_tx();
    let data = bs58::decode(
        tx["transaction"]["message"]["instructions"][0]["data"]
            .as_str()
            .unwrap(),
    )
    .into_vec()
    .unwrap();
    let mut changed = data.clone();
    changed[1] ^= 1;
    tx["transaction"]["message"]["instructions"][0]["data"] =
        json!(bs58::encode(changed).into_string());
    let capture = json!({"schema_version":1,"cluster":"mainnet-beta","commitment":"finalized","source":ARCHIVE_SOURCE,"result":tx});
    let signature = tx["transaction"]["signatures"][0]
        .as_str()
        .unwrap()
        .to_owned();
    db.sql().execute("INSERT INTO capture_inputs(signature,source,result_digest,slot,result_json,capture_json,status) VALUES($1,$2,'x',$3,$4,$5,'received')",
        &[&signature, &ARCHIVE_SOURCE, &(SLOT as i64), &tx.to_string(), &serde_json::to_vec(&capture).unwrap()]).unwrap();
    let report = reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
    assert_eq!(
        outcome_at(&report)["conflicts"][0]["reason"],
        "effects_differ"
    );

    db.sql()
        .execute("UPDATE captures SET decoder_version='older'", &[])
        .unwrap();
    let report = reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
    assert_eq!(report["outcomes"]["pending"], 1);
}

#[test]
fn a_failed_outcome_write_verifies_nothing_and_a_rerun_recovers() {
    let db = setup();
    deliver_replay(&db.0);
    archive_all(&db.0, &mut with_tx());
    let mut sql = db.sql();
    sql.batch_execute("CREATE FUNCTION fail() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$ LANGUAGE plpgsql;
        CREATE TRIGGER fail BEFORE INSERT ON replay_overlap_outcomes FOR EACH ROW EXECUTE FUNCTION fail();").unwrap();
    assert!(reconcile(&db.0, "job", &mut Telemetry::default()).is_err());
    let verified = status(&db.0, "job").unwrap()["verified"].clone();
    assert_eq!(verified["overlap_slots"], 0);
    assert_eq!(verified["verified_through"], 101);
    sql.batch_execute("DROP TRIGGER fail ON replay_overlap_outcomes")
        .unwrap();
    assert_eq!(
        reconcile(&db.0, "job", &mut Telemetry::default()).unwrap()["job"]["verified"]["verified_through"],
        103
    );
}

#[test]
fn revision_changes_and_pruning_invalidate_coverage_but_keep_receipts() {
    let db = setup();
    deliver_replay(&db.0);
    archive_all(&db.0, &mut with_tx());
    reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
    // A new route revision: earlier overlap outcomes no longer count.
    route(&db.0);
    let verified = status(&db.0, "job").unwrap()["verified"].clone();
    assert_eq!(
        (
            verified["route_revision"].clone(),
            verified["overlap_slots"].clone()
        ),
        (json!(2), json!(0))
    );
    assert_eq!(verified["archive_slots"], 2);
    reconcile(&db.0, "job", &mut Telemetry::default()).unwrap();
    assert_eq!(
        status(&db.0, "job").unwrap()["verified"]["overlap_slots"],
        2
    );

    // Pruning to the one transfer at 102 removes older slot evidence, including the
    // boundary; verified coverage falls back to unknown, receipts survive.
    Store::prune(&db.0, 1).unwrap();
    let verified = status(&db.0, "job").unwrap()["verified"].clone();
    assert!(verified["verified_through"].is_null());
    assert_eq!(verified["archive_slots"], 0);
    let receipts: i64 = db
        .sql()
        .query_one("SELECT COUNT(*) FROM replay_overlap_outcomes", &[])
        .unwrap()
        .get(0);
    assert_eq!(receipts, 4);
}

#[test]
fn a_cycle_runs_every_step_in_order_even_after_failures() {
    let order = &std::cell::RefCell::new(Vec::new());
    let step = |name: &'static str, fail: bool| -> crate::replay::Step<'_> {
        (
            name,
            Box::new(move || {
                order.borrow_mut().push(name);
                if fail {
                    Err(Error("replay_budget_exhausted"))
                } else {
                    Ok(json!(name))
                }
            }),
        )
    };
    let out = cycle(vec![
        step("retry", false),
        step("replay", true),
        step("archive", true),
        step("overlap", false),
    ]);
    assert_eq!(
        *order.borrow(),
        vec!["retry", "replay", "archive", "overlap"]
    );
    assert_eq!(out["replay"]["error_class"], "replay_budget_exhausted");
    assert_eq!(out["overlap"]["outcome"], "reported");
}

#[test]
fn an_archive_only_route_skips_replay_and_still_reconciles() {
    let db = TestDatabase::new();
    crate::replay::create(&db.0, "job", USDC, 100, 103).unwrap();
    measure_with(
        &db.0,
        "job",
        || Ok((Some(900), 1000)),
        || Ok(Some(103)),
        &mut Telemetry::default(),
    )
    .unwrap();
    let out = crate::replay::historical_cycle(&db.0, "job", 5, None, Default::default()).unwrap();
    assert!(out.get("replay").is_none());
    assert_eq!(out["retry"]["outcome"], "reported");
    assert_eq!(out["overlap"]["outcome"], "reported");
    // With replayable slots the replay step runs; offline it fails without stopping the cycle.
    let db = setup();
    let out = crate::replay::historical_cycle(&db.0, "job", 5, None, Default::default()).unwrap();
    assert_eq!(out["replay"]["outcome"], "failed");
    assert_eq!(out["overlap"]["outcome"], "reported");
}
