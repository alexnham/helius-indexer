//! Query CLI defaults to the 30-day block-time window; `--all-stored` keeps the pre-window
//! inspection output. Real PostgreSQL; block times are relative to the real clock.
use helius_indexer::{decoder::SPL_TOKEN, storage::pg::TestDatabase};
use serde_json::{Value, json};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::TempDir;

const DAY: i64 = 24 * 60 * 60;

fn key(byte: u8) -> String {
    bs58::encode([byte; 32]).into_string()
}

/// A mainnet-shaped SPL transfer of `amount` in `slot`; `block_time` is omitted when `None`.
fn capture(byte: u8, slot: u64, block_time: Option<i64>, amount: u64) -> Value {
    let mut bytes = vec![3u8];
    bytes.extend(amount.to_le_bytes());
    let balance = |index, owner| {
        json!({"accountIndex":index,"mint":key(3),"owner":owner,
        "programId":SPL_TOKEN,"uiTokenAmount":{"amount":"0","decimals":6}})
    };
    let mut result = json!({"slot":slot,"version":"legacy",
        "transaction":{"signatures":[bs58::encode([byte; 64]).into_string()],"message":{
            "accountKeys":[SPL_TOKEN,key(1),key(2),key(3),key(4)],
            "instructions":[{"programIdIndex":0,"accounts":[1,2,4],
                "data":bs58::encode(bytes).into_string()}]}},
        "meta":{"err":null,"innerInstructions":[],"logMessages":[],
            "preTokenBalances":[balance(1,key(4)),balance(2,key(5))],
            "postTokenBalances":[balance(1,key(4)),balance(2,key(5))]}});
    if let Some(time) = block_time {
        result["blockTime"] = json!(time);
    }
    json!({"schema_version":1,"cluster":"mainnet-beta","commitment":"finalized",
        "source":"fixture","result":result})
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn cli(session: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_helius-indexer"))
        .env("DATABASE_URL", TestDatabase::url())
        .arg(args[0])
        .arg(session)
        .args(&args[1..])
        .output()
        .unwrap()
}

fn ok(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn keys(value: &Value) -> Vec<String> {
    let mut keys: Vec<String> = value.as_object().unwrap().keys().cloned().collect();
    keys.sort();
    keys
}

fn sorted(names: &[&str]) -> Vec<String> {
    let mut names: Vec<String> = names.iter().map(|name| name.to_string()).collect();
    names.sort();
    names
}

/// Ingests one row per (signature byte, slot, block time, amount) and returns the session.
fn seeded(db: &TestDatabase, rows: &[(u8, u64, Option<i64>, u64)]) -> String {
    let session = db.0.session().to_owned();
    let dir = TempDir::new().unwrap();
    for &(byte, slot, time, amount) in rows {
        let path = dir.path().join(format!("{byte}-{slot}.json"));
        std::fs::write(
            &path,
            serde_json::to_vec(&capture(byte, slot, time, amount)).unwrap(),
        )
        .unwrap();
        ok(cli(&session, &["ingest", path.to_str().unwrap()]));
    }
    session
}

/// Old (40 days), recent (1 day) and unknown block time, one row each.
fn three_rows(db: &TestDatabase) -> String {
    let now = now();
    seeded(
        db,
        &[
            (1, 100, Some(now - 40 * DAY), 11),
            (2, 101, Some(now - DAY), 22),
            (3, 102, None, 33),
        ],
    )
}

fn signatures(value: &Value) -> Vec<u64> {
    value["transfers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["amount_raw"].as_str().unwrap().parse().unwrap())
        .collect()
}

#[test]
fn default_query_excludes_old_and_unknown_time_rows_and_counts_unknown() {
    let db = TestDatabase::new();
    let session = three_rows(&db);
    let page = ok(cli(&session, &["query", "--limit", "1000"]));
    assert_eq!(signatures(&page), [22]);
    assert_eq!(page["null_block_time"], 1);
    assert_eq!(page["coverage"]["status"], "unknown");
    assert!(page["cutoff"].is_i64() && page["now"].is_i64());
    assert_eq!(page["window"]["to"], page["now"]);
    assert_eq!(page["window"]["from"], page["cutoff"]);
    let newest = ok(cli(&session, &["query-recent"]));
    assert_eq!(signatures(&newest), [22]);
    assert_eq!(newest["order"], "newest_first");
}

#[test]
fn all_stored_includes_rows_past_the_window_and_unknown_time_rows() {
    let db = TestDatabase::new();
    let session = three_rows(&db);
    let page = ok(cli(&session, &["query", "--all-stored"]));
    let mut amounts = signatures(&page);
    amounts.sort();
    assert_eq!(amounts, [11, 22, 33]);
    let recent = ok(cli(&session, &["query-recent", "--all-stored"]));
    assert_eq!(recent["transfers"].as_array().unwrap().len(), 3);
}

#[test]
fn all_stored_output_keeps_the_pre_window_key_set() {
    let db = TestDatabase::new();
    let session = three_rows(&db);
    let query = ok(cli(&session, &["query", "--all-stored"]));
    assert_eq!(
        keys(&query),
        sorted(&["coverage", "limit", "live", "transfers"])
    );
    assert_eq!(query["coverage"], "unknown");
    assert_eq!(query["limit"], 1000);
    let recent = ok(cli(&session, &["query-recent", "--all-stored"]));
    assert_eq!(
        keys(&recent),
        sorted(&["coverage", "limit", "live", "order", "transfers"])
    );
    assert_eq!(recent["order"], "newest_first");
    let summary = ok(cli(&session, &["summary", key(3).as_str(), "--all-stored"]));
    assert_eq!(
        keys(&summary),
        sorted(&[
            "coverage",
            "confidential_transfer_count",
            "known_fees_raw",
            "known_volume_raw",
            "mint",
            "other_decoder_transfer_count",
            "target_decoder",
            "transfer_count",
            "unknown_amount_count",
            "unknown_fee_count",
            "window",
        ])
    );
    assert_eq!(summary["window"], "all_stored");
    assert_eq!(summary["transfer_count"], 3);
    assert_eq!(summary["known_volume_raw"], "66");
}

#[test]
fn default_summary_excludes_old_and_unknown_time_rows() {
    let db = TestDatabase::new();
    let session = three_rows(&db);
    let summary = ok(cli(&session, &["summary", key(3).as_str()]));
    assert_eq!(summary["transfer_count"], 1);
    assert_eq!(summary["known_volume_raw"], "22");
    assert_eq!(summary["null_block_time"], 1);
    assert_eq!(summary["aggregate_scope"], "known_block_time_in_window");
}

#[test]
fn all_stored_rejects_window_and_cursor_flags() {
    let db = TestDatabase::new();
    let session = three_rows(&db);
    let mint = key(3);
    let rejected = [
        vec!["query", "--all-stored", "--window-seconds", "60"],
        vec!["query-recent", "--all-stored", "--from", "1", "--to", "2"],
        vec!["query", "--all-stored", "--cursor", "00"],
        vec![
            "summary",
            mint.as_str(),
            "--all-stored",
            "--window-seconds",
            "60",
        ],
        vec![
            "summary",
            mint.as_str(),
            "--all-stored",
            "--from",
            "1",
            "--to",
            "2",
        ],
    ];
    for args in rejected {
        let output = cli(&session, &args);
        assert!(!output.status.success(), "{args:?} should be rejected");
        assert!(output.stdout.is_empty(), "{args:?} printed output");
    }
}
