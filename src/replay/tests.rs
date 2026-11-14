use super::*;
use crate::{
    live::USDC,
    storage::{Store, pg::TestDatabase},
};

fn update(slot: u64, signature: u8) -> SubscribeUpdateTransaction {
    let mut tx = live::tests::update();
    tx.slot = slot;
    let info = tx.transaction.as_mut().unwrap();
    info.signature = vec![signature; 64];
    info.transaction.as_mut().unwrap().signatures = vec![vec![signature; 64]];
    tx
}
fn attempt(db: &Database) -> ReplayRun {
    ReplayRun {
        scope: JobLog::open(db).unwrap().scope("fifty").unwrap(),
        reservation: JobLog::reserve(db, "fifty", 5).unwrap(),
    }
}
fn deliver(run: &mut ReplayRun, intake: &mut Intake, tx: &SubscribeUpdateTransaction) {
    if let Retained::Pending(id) = run.input(tx).unwrap() {
        let result = intake.process(id, tx);
        run.reservation.acknowledge(id, &result).unwrap();
        result.unwrap();
    }
}

#[test]
fn out_of_order_duplicates_and_later_slot_never_forge_completion() {
    let db = TestDatabase::new();
    create(&db.0, "fifty", USDC, 100, 149).unwrap();
    let mut run = attempt(&db.0);
    let mut intake = Intake::open(&db.0).unwrap();
    deliver(&mut run, &mut intake, &update(130, 1));
    deliver(&mut run, &mut intake, &update(110, 2));
    assert!(matches!(
        run.input(&update(130, 1)).unwrap(),
        Retained::AlreadyDecoded
    ));
    assert!(matches!(
        run.input(&update(150, 3)).unwrap(),
        Retained::PastEnd
    ));
    run.reservation.finish("end_observed").unwrap();
    let rows = Store::query(&db.0, Some(USDC), 100).unwrap();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|r| r.amount_raw == 42 && r.fee_raw == Some(0) && !r.confidential)
    );
    let report = status(&db.0, "fifty").unwrap();
    assert_eq!(report["resume_from"], 100);
    assert_eq!(report["coverage"], "unknown");
    assert!(report["verified_boundary"].is_null());
    assert_eq!(report["attempts"][0]["end_observed"], true);
    assert_eq!(report["receipts"]["decoded"], 2);
}

#[test]
fn replay_availability_records_absent_zero_expiry_and_unfinalized_end() {
    let db = TestDatabase::new();
    create(&db.0, "fifty", USDC, 100, 149).unwrap();
    let mut run = attempt(&db.0);
    assert_eq!(
        run.reservation
            .availability(&run.scope, None, 150)
            .unwrap_err()
            .0,
        "replay_info_unavailable"
    );
    assert!(status(&db.0, "fifty").unwrap()["attempts"][0]["first_available"].is_null());
    run.reservation
        .availability(&run.scope, Some(0), 150)
        .unwrap();
    assert_eq!(
        status(&db.0, "fifty").unwrap()["attempts"][0]["first_available"],
        0
    );
    assert_eq!(
        run.reservation
            .availability(&run.scope, Some(101), 150)
            .unwrap_err()
            .0,
        "outside_replay_window"
    );
    assert_eq!(
        run.reservation
            .availability(&run.scope, Some(0), 148)
            .unwrap_err()
            .0,
        "replay_end_not_finalized"
    );
    assert_eq!(
        status(&db.0, "fifty").unwrap()["receipts"]["unique_inputs"],
        0
    );
}

#[test]
fn offline_retry_preserves_unresolved_inputs_and_distinguishes_missing_raw() {
    let db = TestDatabase::new();
    create(&db.0, "fifty", USDC, 100, 149).unwrap();
    let mut run = attempt(&db.0);
    let mut bad = update(101, 1);
    bad.transaction.as_mut().unwrap().meta = None;
    let Retained::Pending(bad_id) = run.input(&bad).unwrap() else {
        panic!("missing bad input")
    };
    let Retained::Pending(missing_id) = run.input(&update(102, 2)).unwrap() else {
        panic!("missing input")
    };
    run.input(&update(103, 3)).unwrap();
    drop(run);
    db.0.test_client()
        .execute("DELETE FROM live_raw WHERE id=$1", &[&missing_id])
        .unwrap();
    let report = retry(&db.0, "fifty", &mut Telemetry::default()).unwrap();
    assert_eq!(report["decoded"], 1);
    assert_eq!(report["failed"], 2);
    let mut sql = db.0.test_client();
    assert_eq!(
        sql.query_one(
            "SELECT status FROM replay_receipts WHERE raw_id=$1",
            &[&bad_id]
        )
        .unwrap()
        .get::<_, String>(0),
        "missing_execution_metadata"
    );
    assert_eq!(
        sql.query_one(
            "SELECT status FROM replay_receipts WHERE raw_id=$1",
            &[&missing_id]
        )
        .unwrap()
        .get::<_, String>(0),
        "replay_input_needs_refetch"
    );
    assert_eq!(Store::query(&db.0, Some(USDC), 100).unwrap().len(), 1);
    assert_eq!(report["job"]["budget"]["connections_used"], 0);
}

#[test]
#[ignore = "subprocess helper for replay acknowledgment recovery"]
fn replay_process_exit_child() {
    let database = Database::new(
        TestDatabase::url(),
        &std::env::var("REPLAY_TEST_SESSION").unwrap(),
    )
    .unwrap();
    let phase = std::env::var("REPLAY_TEST_PHASE").unwrap();
    let mut run = attempt(&database);
    let tx = update(123, 9);
    let Retained::Pending(id) = run.input(&tx).unwrap() else {
        panic!("missing retained input")
    };
    if phase == "raw" {
        std::process::exit(77);
    }
    let result = Intake::open(&database).unwrap().process(id, &tx);
    result.unwrap();
    if phase == "output" {
        std::process::exit(77);
    }
    assert_eq!(phase, "receipt");
    run.reservation.acknowledge(id, &result).unwrap();
    std::process::exit(77);
}

#[test]
fn process_death_at_raw_output_and_receipt_boundaries_recovers_without_duplicates() {
    for phase in ["raw", "output", "receipt"] {
        let db = TestDatabase::new();
        create(&db.0, "fifty", USDC, 100, 149).unwrap();
        let exit = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "replay::tests::replay_process_exit_child",
            ])
            .env("REPLAY_TEST_SESSION", db.0.session())
            .env("REPLAY_TEST_PHASE", phase)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert_eq!(exit.code(), Some(77));
        let before = status(&db.0, "fifty").unwrap();
        assert_eq!(before["receipts"]["unique_inputs"], 1);
        assert_eq!(
            before["receipts"]["decoded"],
            if phase == "receipt" { 1 } else { 0 }
        );
        let report = retry(&db.0, "fifty", &mut Telemetry::default()).unwrap();
        assert_eq!(report["job"]["receipts"]["decoded"], 1);
        let rows = Store::query(&db.0, Some(USDC), 100).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].amount_raw, 42);
        assert_eq!(report["job"]["budget"]["seconds_charged"], 5);
        assert_eq!(report["job"]["coverage"], "unknown");
    }
}
