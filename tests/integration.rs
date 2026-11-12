use helius_indexer::{
    decoder::{self, SPL_TOKEN, TOKEN_2022},
    model::Transfer,
    storage::Store,
};
use serde_json::{Value, json};
use std::{
    path::Path,
    process::{Command, Output},
};
use tempfile::TempDir;

fn key(byte: u8) -> String {
    bs58::encode([byte; 32]).into_string()
}
fn signature() -> String {
    bs58::encode([9u8; 64]).into_string()
}
fn instruction(tag: u8, amount: u64, accounts: Vec<usize>) -> Value {
    let mut bytes = vec![tag];
    bytes.extend(amount.to_le_bytes());
    if tag == 12 {
        bytes.push(6);
    }
    json!({"programIdIndex":0,"accounts":accounts,"data":bs58::encode(bytes).into_string()})
}
fn capture() -> Value {
    let balance = |index, owner| {
        json!({"accountIndex":index,"mint":key(3),"owner":owner,
        "programId":SPL_TOKEN,"uiTokenAmount":{"amount":"0","decimals":6}})
    };
    json!({"schema_version":1,"cluster":"mainnet-beta","commitment":"finalized","source":"fixture",
        "result":{"slot":123,"blockTime":1794430000i64,"version":"legacy",
            "transaction":{"signatures":[signature()],"message":{
                "accountKeys":[SPL_TOKEN,key(1),key(2),key(3),key(4)],
                "instructions":[instruction(3,42,vec![1,2,4])] }},
            "meta":{"err":null,"innerInstructions":[],"logMessages":[],
                "preTokenBalances":[balance(1,key(4)),balance(2,key(5))],
                "postTokenBalances":[balance(1,key(4)),balance(2,key(5))]}}})
}
fn decode(value: &Value) -> helius_indexer::Result<Vec<Transfer>> {
    decoder::decode(&decoder::parse_capture(
        &serde_json::to_vec(value).unwrap(),
    )?)
}
fn persist(db: &Path, value: &Value) -> helius_indexer::Result<usize> {
    let raw = serde_json::to_vec(value).unwrap();
    let parsed = decoder::parse_capture(&raw)?;
    let decoded = decoder::decode(&parsed);
    let inserted = Store::open(db)?.persist(&parsed, &raw, &decoded)?;
    decoded?;
    Ok(inserted)
}
fn cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_helius-indexer"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn captured_mainnet_transfers_match_independent_expected_records() {
    let raw = include_bytes!("fixtures/mainnet-transfer.json");
    let tx = decoder::parse_capture(raw).unwrap();
    let actual = decoder::decode(&tx).unwrap();
    let expected: Vec<Transfer> =
        serde_json::from_str(include_str!("fixtures/mainnet-expected.json")).unwrap();
    assert_eq!(actual, expected);
    assert_eq!(
        actual.iter().map(|t| t.amount_raw).collect::<Vec<_>>(),
        [23_360_000, 23_360_000]
    );
}

#[test]
fn exact_u64_zero_self_checked_and_inner_positions_survive_queries() {
    let mut value = capture();
    value["result"]["transaction"]["message"]["instructions"] = json!([
        instruction(3, u64::MAX, vec![1, 2, 4]),
        instruction(12, 0, vec![1, 3, 1, 4]),
    ]);
    value["result"]["meta"]["innerInstructions"] =
        json!([{"index":0,"instructions":[instruction(3,1,vec![2,1,4])]}]);
    value["result"]["meta"]["logMessages"] = json!([
        format!("Program {SPL_TOKEN} invoke [1]"),
        format!("Program {SPL_TOKEN} success"),
        format!("Program {SPL_TOKEN} invoke [2]"),
        format!("Program {SPL_TOKEN} success"),
        format!("Program {SPL_TOKEN} invoke [1]"),
        format!("Program {SPL_TOKEN} success"),
    ]);
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("test.sqlite");
    assert_eq!(persist(&db, &value).unwrap(), 3);
    let rows = Store::query(&db, None, 1000).unwrap();
    assert_eq!(
        rows.iter().map(|t| t.amount_raw).collect::<Vec<_>>(),
        [u64::MAX, 1, 0]
    );
    assert_eq!(rows[0].inner_instruction_index, None);
    assert_eq!(rows[1].inner_instruction_index, Some(0));
    assert_eq!(
        rows[2].source_token_account,
        rows[2].destination_token_account
    );
    assert_ne!(rows[0].transfer_id, rows[1].transfer_id);
    assert_eq!(
        serde_json::to_value(&rows[0]).unwrap()["amount_raw"],
        u64::MAX.to_string()
    );
    assert_eq!(serde_json::to_value(&rows[0]).unwrap()["fee_raw"], "0");
}

#[test]
fn unresolved_or_changed_owners_are_null_not_authorities() {
    let mut value = capture();
    value["result"]["meta"]["postTokenBalances"][0]["owner"] = json!(key(8));
    value["result"]["meta"]["preTokenBalances"][1]
        .as_object_mut()
        .unwrap()
        .remove("owner");
    let rows = decode(&value).unwrap();
    assert_eq!(rows[0].source_owner, None);
    assert_eq!(rows[0].destination_owner, None);
    let mut checked = capture();
    checked["result"]["transaction"]["message"]["instructions"] =
        json!([instruction(12, 42, vec![1, 3, 2, 4])]);
    checked["result"]["meta"]["preTokenBalances"] = Value::Null;
    checked["result"]["meta"]["postTokenBalances"] = Value::Null;
    let row = decode(&checked).unwrap().remove(0);
    assert_eq!(row.mint, key(3));
    assert_eq!(row.source_owner, None);
}

#[test]
fn failed_transactions_native_movements_minting_and_burning_are_excluded() {
    let mut failed = capture();
    failed["result"]["meta"]["err"] = json!({"InstructionError":[0,"Custom"]});
    assert!(decode(&failed).unwrap().is_empty());
    let mut value = capture();
    value["result"]["transaction"]["message"]["instructions"] = json!([
        instruction(7, 100, vec![3, 1, 4]),
        instruction(8, 100, vec![1, 3, 4]),
        instruction(14, 100, vec![3, 1, 4]),
        instruction(15, 100, vec![1, 3, 4]),
    ]);
    assert!(decode(&value).unwrap().is_empty());
    value["result"]["transaction"]["message"]["accountKeys"][0] =
        json!("11111111111111111111111111111111");
    value["result"]["transaction"]["message"]["instructions"] =
        json!([instruction(3, 5, vec![1, 2, 4])]);
    assert!(decode(&value).unwrap().is_empty());
}

#[test]
fn missing_or_invalid_metadata_never_silently_becomes_complete() {
    let mutations: [(&str, Value); 8] = [
        ("/result/meta/innerInstructions", Value::Null),
        (
            "/result/transaction/message/instructions/0/programIdIndex",
            json!(999),
        ),
        (
            "/result/transaction/message/instructions/0/accounts/0",
            json!(999),
        ),
        (
            "/result/transaction/message/instructions/0/data",
            json!("invalid-secret-$"),
        ),
        ("/result/meta/postTokenBalances/0/mint", json!(key(8))),
        ("/result/meta/preTokenBalances/0/accountIndex", json!(999)),
        (
            "/result/meta/preTokenBalances/0/uiTokenAmount/decimals",
            json!(7),
        ),
        ("/result/transaction/signatures/0", json!("invalid")),
    ];
    for (pointer, new_value) in mutations {
        let mut value = capture();
        *value.pointer_mut(pointer).unwrap() = new_value;
        assert!(decode(&value).is_err(), "{pointer}");
    }
    let mut missing = capture();
    missing["result"]["meta"]
        .as_object_mut()
        .unwrap()
        .remove("err");
    assert!(decode(&missing).is_err());
    missing = capture();
    missing["commitment"] = json!("processed");
    assert!(decode(&missing).is_err());
    missing = capture();
    missing["result"]["version"] = json!(99);
    assert!(decode(&missing).is_err());
    missing = capture();
    missing["result"]["meta"]["preTokenBalances"] = json!([]);
    missing["result"]["meta"]["postTokenBalances"] = json!([]);
    assert!(decode(&missing).is_err());
    missing = capture();
    missing["result"]["meta"]["innerInstructions"] = json!([{"index":99,"instructions":[]}]);
    assert!(decode(&missing).is_err());
}

#[test]
fn token_2022_unknown_opcodes_and_owner_changes_are_explicitly_unsupported() {
    let mut value = capture();
    value["result"]["transaction"]["message"]["accountKeys"][0] = json!(TOKEN_2022);
    assert_eq!(decode(&value).unwrap_err().0, "conflicting_token_program");
    value = capture();
    value["result"]["transaction"]["message"]["instructions"][0] =
        instruction(255, 1, vec![1, 2, 4]);
    assert_eq!(
        decode(&value).unwrap_err().0,
        "unsupported_token_instruction"
    );
    value["result"]["transaction"]["message"]["instructions"][0] = instruction(6, 1, vec![1, 2, 4]);
    assert_eq!(
        decode(&value).unwrap_err().0,
        "unsupported_authority_change"
    );
}

#[test]
fn inner_failure_or_truncated_logs_prevent_false_success() {
    let mut value: Value =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-transfer.json")).unwrap();
    value["result"]["meta"]["logMessages"] = Value::Null;
    assert_eq!(decode(&value).unwrap_err().0, "missing_execution_evidence");
    value["result"]["meta"]["logMessages"] = json!([format!("Program {SPL_TOKEN} invoke [2]")]);
    assert_eq!(
        decode(&value).unwrap_err().0,
        "incomplete_execution_evidence"
    );
    value["result"]["meta"]["logMessages"] =
        json!([format!("Program {SPL_TOKEN} failed: ignored-secret")]);
    assert_eq!(
        decode(&value).unwrap_err().0,
        "unsupported_caught_token_failure"
    );
    value["result"]["meta"]["logMessages"] =
        json!([format!("Program {} failed: ignored-secret", key(8))]);
    assert!(decode(&value).is_err());
}

#[test]
fn versioned_loaded_addresses_are_resolved_in_runtime_order() {
    let mut value = capture();
    value["result"]["version"] = json!(0);
    value["result"]["transaction"]["message"]["accountKeys"] = json!([SPL_TOKEN, key(1)]);
    value["result"]["transaction"]["message"]["addressTableLookups"] = json!([{}]);
    value["result"]["meta"]["loadedAddresses"] =
        json!({"writable":[key(2),key(3)],"readonly":[key(4)]});
    assert_eq!(decode(&value).unwrap()[0].destination_token_account, key(2));
    value["result"]["meta"]
        .as_object_mut()
        .unwrap()
        .remove("loadedAddresses");
    assert!(decode(&value).is_err());
}

#[test]
fn replay_is_idempotent_and_conflicting_input_does_not_overwrite() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("test.sqlite");
    let mut value = capture();
    let expected = decode(&value).unwrap();
    assert_eq!(persist(&db, &value).unwrap(), 1);
    value["source"] = json!("solana-mainnet-rpc");
    assert_eq!(persist(&db, &value).unwrap(), 0);
    assert_eq!(Store::query(&db, Some(&key(3)), 1000).unwrap(), expected);
    assert!(Store::query(&db, Some(&key(8)), 1000).unwrap().is_empty());
    value["result"]["transaction"]["message"]["instructions"][0] =
        instruction(3, 999, vec![1, 2, 4]);
    assert_eq!(persist(&db, &value).unwrap_err().0, "capture_conflict");
    assert_eq!(Store::query(&db, None, 1000).unwrap(), expected);
}

#[test]
fn failed_decode_is_retained_without_partial_transfers() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("test.sqlite");
    let mut value = capture();
    value["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
        .push(instruction(255, 1, vec![1, 2, 4]));
    assert!(persist(&db, &value).is_err());
    assert!(Store::query(&db, None, 1000).unwrap().is_empty());
    let conn = rusqlite::Connection::open(db).unwrap();
    let (raw, status): (Vec<u8>, String) = conn
        .query_row("SELECT capture_json,status FROM captures", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(serde_json::from_slice::<Value>(&raw).unwrap(), value);
    assert_eq!(status, "unsupported_token_instruction");
}

#[test]
fn storage_failure_rolls_back_raw_and_all_transfers() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("test.sqlite");
    drop(Store::open(&db).unwrap());
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch("CREATE TRIGGER fail_write BEFORE INSERT ON transfers BEGIN SELECT RAISE(ABORT,'test failure'); END;").unwrap();
    assert!(persist(&db, &capture()).is_err());
    for table in ["captures", "transfers"] {
        assert_eq!(
            conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r
                .get::<_, u32>(0))
                .unwrap(),
            0
        );
    }
}

#[test]
fn cli_process_restart_metrics_replay_and_redaction() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("index.sqlite");
    let fixture = dir.path().join("capture.json");
    let metrics = dir.path().join("metrics.prom");
    std::fs::write(&fixture, serde_json::to_vec(&capture()).unwrap()).unwrap();
    let args = [
        "ingest",
        db.to_str().unwrap(),
        fixture.to_str().unwrap(),
        "--metrics",
        metrics.to_str().unwrap(),
    ];
    let ingest = cli(&args);
    assert!(
        ingest.status.success(),
        "{}",
        String::from_utf8_lossy(&ingest.stderr)
    );
    let result: Value = serde_json::from_slice(&ingest.stdout).unwrap();
    assert_eq!(result["inserted"], 1);
    assert_eq!(result["coverage"], "unknown");
    let query = cli(&["query", db.to_str().unwrap()]);
    assert!(query.status.success());
    let rows: Value = serde_json::from_slice(&query.stdout).unwrap();
    assert_eq!(rows["transfers"][0]["amount_raw"], "42");
    let telemetry = std::fs::read_to_string(&metrics).unwrap();
    assert!(
        telemetry.contains("indexer_transactions_total{stage=\"decode\",outcome=\"success\"} 1")
    );
    assert!(telemetry.contains("indexer_transfers_decoded_total 1"));
    assert!(!telemetry.contains(&signature()));
    assert!(!telemetry.contains(&key(3)));
    for line in String::from_utf8(ingest.stderr).unwrap().lines() {
        assert!(serde_json::from_str::<Value>(line).is_ok());
    }
    // Existing telemetry files are never overwritten; ingest itself still succeeds.
    let replay = cli(&args);
    assert!(replay.status.success());
    assert_eq!(
        serde_json::from_slice::<Value>(&replay.stdout).unwrap()["inserted"],
        0
    );
    assert_eq!(std::fs::read_to_string(&metrics).unwrap(), telemetry);
    let mut invalid = capture();
    invalid["source"] = json!("https://example.invalid?api-key=private-secret");
    std::fs::write(&fixture, serde_json::to_vec(&invalid).unwrap()).unwrap();
    let bad = cli(&["ingest", db.to_str().unwrap(), fixture.to_str().unwrap()]);
    assert!(!bad.status.success());
    assert!(!String::from_utf8_lossy(&bad.stderr).contains("private-secret"));
    assert!(!String::from_utf8_lossy(&bad.stderr).contains("example.invalid"));
    assert_eq!(Store::query(&db, None, 1000).unwrap().len(), 1);
}

#[test]
fn telemetry_export_failure_cannot_overwrite_database_or_capture() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("index.sqlite");
    let input = dir.path().join("capture.json");
    let bytes = serde_json::to_vec(&capture()).unwrap();
    std::fs::write(&input, &bytes).unwrap();
    for destination in [&input, &db] {
        let output = cli(&[
            "ingest",
            db.to_str().unwrap(),
            input.to_str().unwrap(),
            "--metrics",
            destination.to_str().unwrap(),
        ]);
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("metrics_export_failed"));
        assert_eq!(std::fs::read(&input).unwrap(), bytes);
        assert_eq!(Store::query(&db, None, 1000).unwrap().len(), 1);
    }
}

#[test]
fn malformed_input_and_decode_errors_export_sanitized_failure_metrics() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("index.sqlite");
    let input = dir.path().join("capture.json");
    let metrics = dir.path().join("bad.prom");
    std::fs::write(&input, b"not-json-private-secret").unwrap();
    let malformed = cli(&[
        "ingest",
        db.to_str().unwrap(),
        input.to_str().unwrap(),
        "--metrics",
        metrics.to_str().unwrap(),
    ]);
    assert!(!malformed.status.success());
    assert!(!db.exists());
    assert!(!String::from_utf8_lossy(&malformed.stderr).contains("private-secret"));
    let text = std::fs::read_to_string(metrics).unwrap();
    assert!(text.contains("stage=\"input\",outcome=\"rejected\""));
    assert!(!text.contains("stage=\"decode\""));
    let mut value = capture();
    value["result"]["transaction"]["message"]["instructions"][0] =
        instruction(255, 1, vec![1, 2, 4]);
    std::fs::write(&input, serde_json::to_vec(&value).unwrap()).unwrap();
    let metrics = dir.path().join("decode.prom");
    let failed = cli(&[
        "ingest",
        db.to_str().unwrap(),
        input.to_str().unwrap(),
        "--metrics",
        metrics.to_str().unwrap(),
    ]);
    assert!(!failed.status.success());
    let text = std::fs::read_to_string(metrics).unwrap();
    assert!(
        text.contains("indexer_decode_failures_total{failure_class=\"invalid_or_unsupported\"} 1")
    );
    assert!(text.contains("stage=\"decode\",outcome=\"failed\""));
    assert!(text.contains("stage=\"storage\",outcome=\"success\""));
    assert!(Store::query(&db, None, 1000).unwrap().is_empty());
    let oversized = vec![b' '; decoder::MAX_CAPTURE_BYTES + 1];
    assert_eq!(
        decoder::parse_capture(&oversized).unwrap_err().0,
        "capture_too_large"
    );
}

#[test]
fn unavailable_stderr_does_not_abort_ingestion() {
    use std::process::Stdio;
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("index.sqlite");
    let input = dir.path().join("capture.json");
    std::fs::write(&input, serde_json::to_vec(&capture()).unwrap()).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_helius-indexer"))
        .args(["ingest", db.to_str().unwrap(), input.to_str().unwrap()])
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    drop(child.stderr.take());
    assert!(child.wait().unwrap().success());
    assert_eq!(Store::query(&db, None, 1000).unwrap().len(), 1);
}

#[test]
fn application_failure_messages_are_not_runtime_failures() {
    let mut value: Value =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-transfer.json")).unwrap();
    let expected = decode(&value).unwrap();
    value["result"]["meta"]["logMessages"]
        .as_array_mut()
        .unwrap()
        .extend([
            json!("Program log: failed: user message"),
            json!(
                "Program log: nested Program 11111111111111111111111111111111 failed: user message"
            ),
            json!("Program not-a-program-id failed: user message"),
        ]);
    assert_eq!(decode(&value).unwrap(), expected);
    value["result"]["meta"]["logMessages"]
        .as_array_mut()
        .unwrap()
        .push(json!(format!("Program {} failed: caught CPI", key(8))));
    assert_eq!(
        decode(&value).unwrap_err().0,
        "unsupported_caught_token_failure"
    );
}

#[test]
fn every_failure_event_keeps_context_even_when_decode_and_storage_fail() {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("capture.json");
    let mut value = capture();
    value["result"]["transaction"]["message"]["instructions"][0] =
        instruction(255, 1, vec![1, 2, 4]);
    std::fs::write(&input, serde_json::to_vec(&value).unwrap()).unwrap();
    // A directory cannot be opened as a SQLite database or as a new metrics file.
    let result = cli(&[
        "ingest",
        dir.path().to_str().unwrap(),
        input.to_str().unwrap(),
        "--metrics",
        dir.path().to_str().unwrap(),
    ]);
    assert!(!result.status.success());
    let events: Vec<Value> = String::from_utf8(result.stderr)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let run_id = events[0]["run_id"].clone();
    for event in &events {
        assert_eq!(event["run_id"], run_id);
        assert!(event["timestamp_unix_ms"].is_number());
        assert_eq!(event["component"], "offline_indexer");
        assert_eq!(event["decoder_version"], decoder::DECODER_VERSION);
        assert!(event["operation"].is_string());
        if event["outcome"] == "failed" {
            assert_eq!(event["slot"], 123);
            assert!(event["error_class"].is_string());
        }
    }
    assert!(
        events
            .iter()
            .any(|e| e["operation"] == "decode"
                && e["error_class"] == "unsupported_token_instruction")
    );
    assert!(
        events
            .iter()
            .any(|e| e["operation"] == "storage" && e["error_class"] == "storage_open_failed")
    );
    assert!(events.iter().any(|e| e["event"] == "metrics_export_failed"));
    assert!(events.iter().any(|e| e["event"] == "command_failed"));
}

fn token_2022_capture(amount: u64, fee: Option<u64>) -> Value {
    let mut value = capture();
    value["result"]["transaction"]["message"]["accountKeys"][0] = json!(TOKEN_2022);
    for field in ["preTokenBalances", "postTokenBalances"] {
        for balance in value["result"]["meta"][field].as_array_mut().unwrap() {
            balance["programId"] = json!(TOKEN_2022);
        }
    }
    let mut bytes = if fee.is_some() { vec![26, 1] } else { vec![12] };
    bytes.extend(amount.to_le_bytes());
    bytes.push(6);
    if let Some(fee) = fee {
        bytes.extend(fee.to_le_bytes());
    }
    value["result"]["transaction"]["message"]["instructions"] = json!([
        {"programIdIndex":0,"accounts":[1,3,2,4],"data":bs58::encode(bytes).into_string()}
    ]);
    value
}

#[test]
fn token_2022_fee_evidence_survives_persistence_exactly() {
    for (amount, fee) in [(u64::MAX, u64::MAX), (100, 1), (100, 0), (0, 0)] {
        let mut value = token_2022_capture(amount, Some(fee));
        let ix = value["result"]["transaction"]["message"]["instructions"][0].clone();
        value["result"]["meta"]["innerInstructions"] = json!([{"index":0,"instructions":[ix]}]);
        value["result"]["meta"]["logMessages"] = json!([
            format!("Program {TOKEN_2022} invoke [1]"),
            format!("Program {TOKEN_2022} invoke [2]"),
            format!("Program {TOKEN_2022} success"),
            format!("Program {TOKEN_2022} success")
        ]);
        let dir = TempDir::new().unwrap();
        let db = dir.path().join("fee.sqlite");
        assert_eq!(persist(&db, &value).unwrap(), 2);
        assert_eq!(persist(&db, &value).unwrap(), 0);
        let rows = Store::query(&db, None, 1000).unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row.amount_raw, amount);
            assert_eq!(row.fee_raw, Some(fee));
            assert!(!row.confidential);
        }
        assert_eq!(rows[0].inner_instruction_index, None);
        assert_eq!(rows[1].inner_instruction_index, Some(0));
        assert_ne!(rows[0].transfer_id, rows[1].transfer_id);
        value["result"]["meta"]["logMessages"] = json!([]);
        assert_eq!(
            decode(&value).unwrap_err().0,
            "incomplete_execution_evidence"
        );
    }
}

#[test]
fn unknown_fee_preserves_transfer_and_zero_remains_known() {
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("unknown.sqlite");
    let value = token_2022_capture(100, None);
    assert_eq!(persist(&db, &value).unwrap(), 1);
    let rows = Store::query(&db, None, 1000).unwrap();
    assert_eq!(rows[0].amount_raw, 100);
    assert_eq!(rows[0].fee_raw, None);
    let summary = Store::summary(&db, &rows[0].mint).unwrap();
    assert_eq!(summary["known_volume_raw"], "100");
    assert_eq!(summary["known_fees_raw"], "0");
    assert_eq!(summary["unknown_fee_count"], 1);
    let conn = rusqlite::Connection::open(&db).unwrap();
    let (status, raw): (String, Vec<u8>) = conn
        .query_row("SELECT status,capture_json FROM captures", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    assert_eq!(status, "decoded");
    assert_eq!(serde_json::from_slice::<Value>(&raw).unwrap(), value);
    assert_eq!(
        decode(&token_2022_capture(0, None)).unwrap()[0].fee_raw,
        Some(0)
    );
    let mut self_transfer = token_2022_capture(100, Some(1));
    self_transfer["result"]["transaction"]["message"]["instructions"][0]["accounts"][2] = json!(1);
    assert_eq!(decode(&self_transfer).unwrap()[0].fee_raw, Some(0));
    assert_eq!(
        decode(&token_2022_capture(1, Some(2))).unwrap_err().0,
        "invalid_transfer_fee"
    );
}

#[test]
fn captured_token_2022_transfers_preserve_unknown_fees() {
    let capture =
        decoder::parse_capture(include_bytes!("fixtures/mainnet-token-2022.json")).unwrap();
    assert_eq!(capture.result.slot, 468265270);
    let rows = decoder::decode(&capture).unwrap();
    assert!(!rows.is_empty());
    assert!(
        rows.iter()
            .any(|r| r.token_program == TOKEN_2022 && r.fee_raw.is_none())
    );
}

#[test]
fn token_2022_caught_parent_failure_and_metrics_are_bounded() {
    let mut value = token_2022_capture(100, Some(1));
    let ix = value["result"]["transaction"]["message"]["instructions"][0].clone();
    value["result"]["meta"]["innerInstructions"] = json!([{"index":0,"instructions":[ix]}]);
    value["result"]["meta"]["logMessages"] = json!([
        format!("Program {TOKEN_2022} invoke [1]"),
        format!("Program {TOKEN_2022} success"),
        format!("Program {TOKEN_2022} invoke [2]"),
        format!("Program {TOKEN_2022} success"),
        format!("Program {} failed: caught parent failure", key(8))
    ]);
    assert_eq!(
        decode(&value).unwrap_err().0,
        "unsupported_caught_token_failure"
    );
    let dir = TempDir::new().unwrap();
    let db = dir.path().join("failed.sqlite");
    let input = dir.path().join("input.json");
    let metrics = dir.path().join("metrics.prom");
    std::fs::write(&input, serde_json::to_vec(&value).unwrap()).unwrap();
    let result = cli(&[
        "ingest",
        db.to_str().unwrap(),
        input.to_str().unwrap(),
        "--metrics",
        metrics.to_str().unwrap(),
    ]);
    assert!(!result.status.success());
    assert!(Store::query(&db, None, 1000).unwrap().is_empty());
    let text = std::fs::read_to_string(metrics).unwrap();
    assert!(text.contains("failure_class=\"invalid_or_unsupported\""));
    assert!(!text.contains(TOKEN_2022));
    assert!(!text.contains(&signature()));
}

#[test]
fn token_2022_unchecked_and_malformed_extension_boundaries() {
    let mut value = token_2022_capture(42, None);
    value["result"]["transaction"]["message"]["instructions"][0] =
        instruction(3, 42, vec![1, 2, 4]);
    assert_eq!(decode(&value).unwrap()[0].fee_raw, Some(0));
    for bytes in [vec![26], vec![26, 1], vec![26, 5], vec![27], vec![37]] {
        value["result"]["transaction"]["message"]["instructions"][0]["data"] =
            json!(bs58::encode(bytes).into_string());
        assert!(decode(&value).is_err());
    }
}

#[test]
fn nontransfer_exclusions_require_known_program_and_instruction_shape() {
    for program in [SPL_TOKEN, TOKEN_2022] {
        for data in [vec![45, 0], [&[45, 1][..], &42u64.to_le_bytes()].concat()] {
            let mut value = capture();
            value["result"]["transaction"]["message"]["accountKeys"][0] = json!(program);
            value["result"]["transaction"]["message"]["instructions"][0]["data"] =
                json!(bs58::encode(&data).into_string());
            assert!(decode(&value).unwrap().is_empty());
            let mut malformed = data;
            malformed.push(0);
            value["result"]["transaction"]["message"]["instructions"][0]["data"] =
                json!(bs58::encode(malformed).into_string());
            assert!(decode(&value).is_err());
        }
    }
    let mut value = token_2022_capture(42, None);
    value["result"]["transaction"]["message"]["instructions"][0]["data"] =
        json!(bs58::encode([26, 4]).into_string());
    assert!(decode(&value).unwrap().is_empty());
    value["result"]["transaction"]["message"]["instructions"][0]["data"] =
        json!(bs58::encode([26, 4, 0]).into_string());
    assert!(decode(&value).is_err());
    value["result"]["transaction"]["message"]["instructions"][0]["data"] =
        json!(bs58::encode([26, 4]).into_string());
    value["result"]["transaction"]["message"]["accountKeys"][0] = json!(SPL_TOKEN);
    assert!(decode(&value).is_err());
    let mut mixed = capture();
    mixed["result"]["transaction"]["message"]["accountKeys"]
        .as_array_mut()
        .unwrap()
        .push(json!(TOKEN_2022));
    mixed["result"]["transaction"]["message"]["instructions"]
        .as_array_mut()
        .unwrap()
        .push(json!({"programIdIndex":5,"accounts":[1],"data":bs58::encode([26,4]).into_string()}));
    let rows = decode(&mixed).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].amount_raw, 42);
    assert_eq!(rows[0].token_program, SPL_TOKEN);
}
