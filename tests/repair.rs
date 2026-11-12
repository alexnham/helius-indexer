//! Erroneous prior outputs below are seeded deliberately; capture bytes stay unchanged.
use helius_indexer::{decoder, model::Transfer, repair, storage::Store, telemetry::Telemetry};
use rusqlite::{Connection, params};
use serde_json::{Value, json};
use std::path::Path;

fn fixture() -> Value {
    serde_json::from_slice(include_bytes!("fixtures/mainnet-transfer.json")).unwrap()
}
fn slot() -> u64 {
    fixture()["result"]["slot"].as_u64().unwrap()
}
fn expected(value: &Value) -> Vec<Transfer> {
    decoder::decode(&decoder::parse_capture(&serde_json::to_vec(value).unwrap()).unwrap()).unwrap()
}
fn seed(path: &Path, value: &Value) {
    let raw = serde_json::to_vec(value).unwrap();
    let capture = decoder::parse_capture(&raw).unwrap();
    Store::open(path)
        .unwrap()
        .persist(&capture, &raw, &decoder::decode(&capture))
        .unwrap();
    let signature = &capture.result.transaction.signatures[0];
    let db = Connection::open(path).unwrap();
    db.execute(
        "UPDATE captures SET decoder_version='token-transfers-v3' WHERE signature=?1",
        [signature],
    )
    .unwrap();
    db.execute("UPDATE transfers SET record_json=json_set(record_json,'$.decoder_version','token-transfers-v3') WHERE signature=?1", [signature]).unwrap();
}
fn insert(db: &Connection, row: &Transfer) {
    db.execute(
        "INSERT INTO transfers VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            row.transfer_id,
            row.signature,
            row.slot as i64,
            row.instruction_index as i64,
            row.inner_instruction_index.map_or(-1, |n| n as i64),
            row.mint,
            serde_json::to_string(row).unwrap()
        ],
    )
    .unwrap();
}
fn run(path: &Path) -> Value {
    repair::run(path, slot(), slot(), &mut Telemetry::default(), || false).unwrap()
}
fn rows(path: &Path) -> Vec<Transfer> {
    let mut rows = Store::query(path, None, 1000).unwrap();
    rows.sort_by(|a, b| a.transfer_id.cmp(&b.transfer_id));
    rows
}
fn sorted(mut rows: Vec<Transfer>) -> Vec<Transfer> {
    rows.sort_by(|a, b| a.transfer_id.cmp(&b.transfer_id));
    rows
}

#[test]
fn correction_adds_changes_and_removes_full_transaction_output_without_new_input() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    let value = fixture();
    let correct = expected(&value);
    seed(&path, &value);
    let db = Connection::open(&path).unwrap();
    db.execute(
        "DELETE FROM transfers WHERE transfer_id=?1",
        [&correct[0].transfer_id],
    )
    .unwrap();
    db.execute("UPDATE transfers SET mint='old-wrong-mint',record_json=json_set(record_json,'$.mint','old-wrong-mint','$.amount_raw','999','$.fee_raw','7')",[]).unwrap();
    let mut false_row = correct[0].clone();
    false_row.transfer_id.push_str(":false");
    false_row.amount_raw = 123;
    false_row.decoder_version = "token-transfers-v3".into();
    insert(&db, &false_row);
    // An old unresolved flag must not be resurrected by a later target revision.
    db.execute("INSERT INTO decoder_repairs VALUES(?1,'older-target',?2,'older','unresolved','old_error',1,1)",params![correct[0].signature,slot() as i64]).unwrap();
    assert!(rows(&path).is_empty());
    let result = run(&path);
    assert_eq!(result["corrected"], 1);
    assert_eq!(result["progress"]["repair_complete"], true);
    assert_eq!(rows(&path), sorted(correct.clone()));
    assert_eq!(
        Store::summary(&path, "old-wrong-mint").unwrap()["transfer_count"],
        0
    );
    let summary = Store::summary(&path, &correct[0].mint).unwrap();
    assert_eq!(run(&path)["attempted"], 0);
    assert_eq!(Store::summary(&path, &correct[0].mint).unwrap(), summary);
    let raw: Vec<u8> = db
        .query_row("SELECT capture_json FROM captures", [], |r| r.get(0))
        .unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&raw).unwrap(), value);
}

#[test]
fn corrected_empty_output_removes_false_transfers_and_invalidates_slot_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    let value: Value =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-unwrap.json")).unwrap();
    let target = value["result"]["slot"].as_u64().unwrap();
    seed(&path, &value);
    let mut false_row = expected(&fixture())[0].clone();
    false_row.signature = value["result"]["transaction"]["signatures"][0]
        .as_str()
        .unwrap()
        .into();
    false_row.transfer_id = format!("solana:mainnet-beta:{}:0:outer", false_row.signature);
    false_row.slot = target;
    let db = Connection::open(&path).unwrap();
    insert(&db, &false_row);
    helius_indexer::backfill::History::open(&path).unwrap();
    db.execute(
        "INSERT INTO historical_slots VALUES(?1,?2,?3,'complete',NULL,1,1,1)",
        params![false_row.mint, decoder::DECODER_VERSION, target as i64],
    )
    .unwrap();
    let report = repair::run(&path, target, target, &mut Telemetry::default(), || false).unwrap();
    assert_eq!(report["written_rows"], 0);
    assert_eq!(report["progress"]["repair_complete"], true);
    assert!(rows(&path).is_empty());
    let coverage = helius_indexer::backfill::History::open(&path)
        .unwrap()
        .coverage(&false_row.mint, target, target)
        .unwrap();
    assert!(!coverage.complete);
    assert_eq!(coverage.contiguous_through, None);
    assert_eq!(
        coverage.slots[0].error_class.as_deref(),
        Some("decoder_repair_requires_verification")
    );
}

#[test]
fn failed_decode_hides_stale_output_but_keeps_original_input_rows_and_pending_age() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    let mut incomplete = fixture();
    incomplete["result"]["meta"]["innerInstructions"] = Value::Null;
    seed(&path, &incomplete);
    let false_row = expected(&fixture())[0].clone();
    let db = Connection::open(&path).unwrap();
    insert(&db, &false_row);
    assert_eq!(run(&path)["unresolved"], 1);
    assert!(rows(&path).is_empty());
    assert_eq!(
        Store::summary(&path, &false_row.mint).unwrap()["transfer_count"],
        0
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM transfers", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
    db.execute("UPDATE decoder_repairs SET first_pending=1", [])
        .unwrap();
    let result = run(&path);
    assert_eq!(result["progress"]["repair_complete"], false);
    assert_eq!(result["progress"]["first_unresolved_at"], 1);
    assert_eq!(
        db.query_row("SELECT attempts FROM decoder_repairs", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn failed_write_or_revision_ack_rolls_back_deletion_insertion_visibility_and_progress() {
    for trigger in [
        "CREATE TRIGGER fail BEFORE INSERT ON transfers BEGIN SELECT RAISE(ABORT,'injected'); END;",
        "CREATE TRIGGER fail BEFORE UPDATE OF decoder_version ON captures BEGIN SELECT RAISE(ABORT,'injected'); END;",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repair.sqlite");
        seed(&path, &fixture());
        let db = Connection::open(&path).unwrap();
        db.execute(
            "UPDATE transfers SET record_json=json_set(record_json,'$.amount_raw','999')",
            [],
        )
        .unwrap();
        let before = rows(&path);
        db.execute_batch(trigger).unwrap();
        assert_eq!(
            repair::run(&path, slot(), slot(), &mut Telemetry::default(), || false)
                .unwrap_err()
                .0,
            "storage_write_failed"
        );
        assert_eq!(rows(&path), before);
        assert_eq!(
            db.query_row("SELECT COUNT(*) FROM decoder_repairs", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            repair::status(&path, slot(), slot()).unwrap()["remaining_other_revision"],
            1
        );
        db.execute_batch("DROP TRIGGER fail").unwrap();
        run(&path);
        assert_eq!(rows(&path), sorted(expected(&fixture())));
    }
}

#[test]
#[ignore = "subprocess helper for interrupted decoder repair"]
fn repair_process_exit_child() {
    let path = std::env::var_os("INDEXER_TEST_REPAIR_DB").unwrap();
    let path = Path::new(&path);
    let db = Connection::open(path).unwrap();
    repair::run(path, slot(), slot(), &mut Telemetry::default(), || {
        let count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM decoder_repairs WHERE status='complete'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        if count > 0 {
            std::process::exit(77);
        }
        false
    })
    .unwrap();
    panic!("child must exit between transaction commits");
}

#[test]
fn process_exit_between_corrections_resumes_and_converges_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    let mut all = Vec::new();
    for byte in [11, 12, 13] {
        let mut value = fixture();
        value["result"]["transaction"]["signatures"][0] =
            json!(bs58::encode([byte; 64]).into_string());
        seed(&path, &value);
        all.extend(expected(&value));
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "repair_process_exit_child", "--ignored"])
        .env("INDEXER_TEST_REPAIR_DB", &path)
        .output()
        .unwrap()
        .status;
    assert_eq!(status.code(), Some(77));
    let progress = repair::status(&path, slot(), slot()).unwrap();
    assert_eq!(progress["corrected"], 1);
    assert_eq!(progress["remaining_other_revision"], 2);
    assert_eq!(progress["repair_complete"], false);
    assert_eq!(run(&path)["corrected"], 2);
    assert_eq!(rows(&path), sorted(all));
    assert_eq!(run(&path)["attempted"], 0);
}

#[test]
fn bounded_batches_prioritize_unattempted_work_over_an_unresolved_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    for byte in 1..=101 {
        let mut value = fixture();
        value["result"]["transaction"]["signatures"][0] =
            json!(bs58::encode([byte; 64]).into_string());
        if byte != 101 {
            value["result"]["meta"]["innerInstructions"] = Value::Null;
        }
        seed(&path, &value);
    }
    let first = run(&path);
    assert_eq!(first["attempted"], 100);
    let second = run(&path);
    assert_eq!(second["attempted"], 100);
    let progress = repair::status(&path, slot(), slot()).unwrap();
    assert_eq!(progress["corrected"], 1);
    assert_eq!(progress["unresolved"], 100);
    assert_eq!(progress["repair_complete"], false);
    assert_eq!(rows(&path).len(), 2);
}

#[test]
fn invalid_scope_does_not_create_database_and_legacy_status_requires_no_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    assert!(repair::run(&path, 0, 128, &mut Telemetry::default(), || false).is_err());
    assert!(!path.exists());
    seed(&path, &fixture());
    Connection::open(&path)
        .unwrap()
        .execute("DROP TABLE decoder_repairs", [])
        .unwrap();
    assert_eq!(
        repair::status(&path, slot(), slot()).unwrap()["remaining_other_revision"],
        1
    );
    assert_eq!(rows(&path).len(), 2);
    assert_eq!(run(&path)["corrected"], 1);
}

#[test]
fn failed_transaction_correction_deletes_false_output_despite_failure_logs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    let mut failed = fixture();
    failed["result"]["meta"]["err"] = json!({"InstructionError":[0,{"Custom":1}]});
    failed["result"]["meta"]["logMessages"]
        .as_array_mut()
        .unwrap()
        .push(json!(format!(
            "Program {} failed: injected",
            decoder::SPL_TOKEN
        )));
    seed(&path, &failed);
    insert(&Connection::open(&path).unwrap(), &expected(&fixture())[0]);
    assert_eq!(run(&path)["corrected"], 1);
    assert!(rows(&path).is_empty());
    assert_eq!(
        Connection::open(&path)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM transfers", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[test]
fn offline_block_repair_reuses_prior_revision_inputs_and_imports_previously_rejected_transactions()
{
    use helius_indexer::backfill::History;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    let mut sparse = fixture();
    sparse["result"]["meta"]["innerInstructions"] = Value::Null;
    seed(&path, &sparse);
    History::open(&path).unwrap();
    let mut additional = fixture();
    additional["result"]["transaction"]["signatures"][0] =
        json!(bs58::encode([77; 64]).into_string());
    let raw=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{"parentSlot":slot()-1,"blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([6;32]).into_string(),"blockTime":fixture()["result"]["blockTime"],"transactions":[fixture()["result"],additional["result"]]}})).unwrap();
    let db = Connection::open(&path).unwrap();
    db.execute("INSERT INTO historical_inputs VALUES(?1,'token-transfers-v3',?2,1,'finalized-mainnet-rpc',1,?3)",params![expected(&fixture())[0].mint,slot() as i64,raw]).unwrap();
    let report = repair::history(
        &path,
        &mut Telemetry::default(),
        &expected(&fixture())[0].mint,
        slot(),
        slot(),
        || false,
    )
    .unwrap();
    assert_eq!(report["metrics"]["source_attempts"], 0);
    assert_eq!(report["metrics"]["cached_blocks"], 1);
    assert_eq!(report["coverage"]["complete"], true);
    let mut all = expected(&fixture());
    all.extend(expected(&additional));
    assert_eq!(rows(&path), sorted(all));
    assert_eq!(
        repair::history(
            &path,
            &mut Telemetry::default(),
            &expected(&fixture())[0].mint,
            slot(),
            slot(),
            || { false }
        )
        .unwrap()["metrics"]["source_attempts"],
        0
    );
    // A missing retained block stays unknown and cannot trigger any provider fallback.
    let gap = repair::history(
        &path,
        &mut Telemetry::default(),
        &expected(&fixture())[0].mint,
        slot() + 1,
        slot() + 1,
        || false,
    )
    .unwrap();
    assert_eq!(gap["coverage"]["complete"], false);
    assert_eq!(
        gap["coverage"]["slots"][0]["error_class"],
        "retained_block_unavailable"
    );
}

#[test]
fn malformed_retained_envelope_is_unresolved_and_cli_events_keep_repair_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    seed(&path, &fixture());
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE captures SET capture_json=?1",
            [b"broken-json".as_slice()],
        )
        .unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_helius-indexer"))
        .args([
            "repair",
            path.to_str().unwrap(),
            &slot().to_string(),
            &slot().to_string(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["unresolved"], 1);
    assert!(rows(&path).is_empty());
    for line in String::from_utf8(output.stderr).unwrap().lines() {
        let event: Value = serde_json::from_str(line).unwrap();
        assert_eq!(event["run_id"], report["run_id"]);
        assert_eq!(event["component"], "decoder_repair");
        assert!(event.get("signature").is_none());
    }
}

#[test]
fn offline_block_repair_failure_hides_stale_records_while_preserving_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    let mut incomplete = fixture();
    incomplete["result"]["meta"]["innerInstructions"] = Value::Null;
    seed(&path, &incomplete);
    helius_indexer::backfill::History::open(&path).unwrap();
    let old = expected(&fixture())[0].clone();
    let db = Connection::open(&path).unwrap();
    insert(&db, &old);
    let raw=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{"parentSlot":slot()-1,"blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([6;32]).into_string(),"blockTime":fixture()["result"]["blockTime"],"transactions":[incomplete["result"]]}})).unwrap();
    db.execute("INSERT INTO historical_inputs VALUES(?1,'token-transfers-v3',?2,1,'finalized-mainnet-rpc',1,?3)",params![old.mint,slot() as i64,raw]).unwrap();
    let report = repair::history(
        &path,
        &mut Telemetry::default(),
        &old.mint,
        slot(),
        slot(),
        || false,
    )
    .unwrap();
    assert_eq!(report["coverage"]["complete"], false);
    assert!(rows(&path).is_empty());
    assert_eq!(
        Store::summary(&path, &old.mint).unwrap()["transfer_count"],
        0
    );
    assert_eq!(
        repair::status(&path, slot(), slot()).unwrap()["unresolved"],
        1
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM transfers", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn an_older_binary_cannot_repair_a_newer_numbered_decoder_revision() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    seed(&path, &fixture());
    Connection::open(&path)
        .unwrap()
        .execute(
            "UPDATE captures SET decoder_version='token-transfers-v999'",
            [],
        )
        .unwrap();
    let before = rows(&path);
    assert_eq!(
        repair::run(&path, slot(), slot(), &mut Telemetry::default(), || false)
            .unwrap_err()
            .0,
        "decoder_downgrade_not_supported"
    );
    assert_eq!(rows(&path), before);
}

#[test]
fn current_revision_unresolved_marker_can_be_reverified_after_metadata_arrives() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repair.sqlite");
    let mut incomplete = fixture();
    incomplete["result"]["meta"]["innerInstructions"] = Value::Null;
    seed(&path, &incomplete);
    helius_indexer::backfill::History::open(&path).unwrap();
    let db = Connection::open(&path).unwrap();
    db.execute(
        "UPDATE captures SET decoder_version=?1",
        [decoder::DECODER_VERSION],
    )
    .unwrap();
    let signature = incomplete["result"]["transaction"]["signatures"][0]
        .as_str()
        .unwrap();
    db.execute("INSERT INTO decoder_repairs VALUES(?1,?2,?3,?2,'unresolved','missing_inner_instructions',1,1)",params![signature,decoder::DECODER_VERSION,slot() as i64]).unwrap();
    let raw = serde_json::to_vec(&fixture()).unwrap();
    let capture = decoder::parse_capture(&raw).unwrap();
    Store::open(&path)
        .unwrap()
        .persist(&capture, &raw, &decoder::decode(&capture))
        .unwrap();
    assert!(rows(&path).is_empty());
    assert_eq!(
        repair::status(&path, slot(), slot()).unwrap()["unresolved"],
        1
    );
    assert_eq!(run(&path)["corrected"], 1);
    assert_eq!(rows(&path), sorted(expected(&fixture())));
    assert_eq!(
        repair::status(&path, slot(), slot()).unwrap()["repair_complete"],
        true
    );
}
