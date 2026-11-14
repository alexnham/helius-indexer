use helius_indexer::{
    Result,
    backfill::{BlockSource, History},
    decoder,
    model::Transfer,
    storage::{Store, pg::TestDatabase},
};
use serde_json::{Value, json};
use std::process::Command;

fn cases() -> Vec<Value> {
    serde_json::from_slice(include_bytes!("fixtures/confidential-transfers.json")).unwrap()
}
fn decode(value: &Value) -> Result<Vec<Transfer>> {
    decoder::decode(&decoder::parse_capture(
        &serde_json::to_vec(value).unwrap(),
    )?)
}
fn set_instruction(value: &mut Value, data: &[u8], accounts: Value) {
    value["result"]["transaction"]["message"]["instructions"][0] = json!({
        "programIdIndex":0,"accounts":accounts,"data":bs58::encode(data).into_string()
    });
}

#[test]
fn confidential_outer_inner_hidden_values_survive_restart_replay_and_summary() {
    for case in cases() {
        let db = TestDatabase::new();
        let mut value = case["capture"].clone();
        let ix = value["result"]["transaction"]["message"]["instructions"][0].clone();
        value["result"]["meta"]["innerInstructions"] = json!([{"index":0,"instructions":[ix]}]);
        let program = decoder::TOKEN_2022;
        value["result"]["meta"]["logMessages"] = json!([
            format!("Program {program} invoke [1]"),
            format!("Program {program} invoke [2]"),
            format!("Program {program} success"),
            format!("Program {program} success")
        ]);
        let raw = serde_json::to_vec(&value).unwrap();
        let capture = decoder::parse_capture(&raw).unwrap();
        let rows = decoder::decode(&capture).unwrap();
        let serialized = serde_json::to_value(&rows[0]).unwrap();
        for (field, expected) in case["expected"].as_object().unwrap() {
            assert_eq!(&serialized[field], expected, "{field}");
        }
        assert_eq!(rows[1].inner_instruction_index, Some(0));
        assert_ne!(rows[0].transfer_id, rows[1].transfer_id);
        assert_eq!(
            Store::open(&db.0)
                .unwrap()
                .persist(&capture, &raw, &Ok(rows.clone()))
                .unwrap(),
            2
        );
        assert_eq!(
            Store::open(&db.0)
                .unwrap()
                .persist(&capture, &raw, &Ok(rows.clone()))
                .unwrap(),
            0
        );
        assert_eq!(Store::query(&db.0, None, 100).unwrap(), rows);
        let summary = Store::summary(&db.0, &rows[0].mint).unwrap();
        assert_eq!(summary["known_volume_raw"], "0");
        assert_eq!(summary["unknown_amount_count"], 2);
        assert_eq!(summary["unknown_fee_count"], 2);
        assert_eq!(summary["confidential_transfer_count"], 2);
        assert_eq!(summary["coverage"], "unknown");
        value["result"]["meta"]["logMessages"] = json!([]);
        assert_eq!(
            decode(&value).unwrap_err().0,
            "incomplete_execution_evidence"
        );
    }
}

#[test]
fn confidential_metadata_is_optional_but_conflicts_are_rejected_and_decimals_enrich() {
    let mut value = cases()[0]["capture"].clone();
    let without = decode(&value).unwrap();
    let mint = without[0].mint.clone();
    let program = decoder::TOKEN_2022;
    let balance =
        json!({"accountIndex":1,"mint":mint,"programId":program,"uiTokenAmount":{"decimals":6}});
    value["result"]["meta"]["preTokenBalances"] = json!([balance]);
    let with = decode(&value).unwrap();
    assert_eq!(with[0].decimals, Some(6));
    assert_eq!(with[0].amount_raw, None);
    for observations in [
        [&cases()[0]["capture"], &value],
        [&value, &cases()[0]["capture"]],
    ] {
        let db = TestDatabase::new();
        for observation in observations {
            let raw = serde_json::to_vec(observation).unwrap();
            let parsed = decoder::parse_capture(&raw).unwrap();
            Store::open(&db.0)
                .unwrap()
                .persist(&parsed, &raw, &decoder::decode(&parsed))
                .unwrap();
        }
        assert_eq!(Store::query(&db.0, None, 100).unwrap(), with);
    }
    value["result"]["meta"]["preTokenBalances"][0]["mint"] = json!(without[0].source_token_account);
    assert_eq!(decode(&value).unwrap_err().0, "conflicting_token_metadata");
    value["result"]["meta"]["preTokenBalances"][0]["mint"] = json!(mint);
    value["result"]["meta"]["preTokenBalances"][0]["programId"] = json!(decoder::SPL_TOKEN);
    assert_eq!(decode(&value).unwrap_err().0, "conflicting_token_program");
}

#[test]
fn confidential_non_transfer_layouts_never_create_edges() {
    for (variant, length, accounts) in [
        (0, 67, 1),
        (1, 35, 2),
        (2, 47, 4),
        (3, 2, 3),
        (4, 3, 3),
        (5, 11, 3),
        (6, 49, 4),
        (8, 46, 2),
        (9, 2, 2),
        (10, 2, 2),
        (11, 2, 2),
        (12, 2, 2),
        (14, 2, 3),
    ] {
        let mut value = cases()[0]["capture"].clone();
        let mut data = vec![0; length];
        data[0] = 27;
        data[1] = variant;
        set_instruction(&mut value, &data, json!(vec![1; accounts]));
        assert!(decode(&value).unwrap().is_empty(), "variant {variant}");
        data.push(0);
        set_instruction(&mut value, &data, json!(vec![1; accounts]));
        assert_eq!(
            decode(&value).unwrap_err().0,
            "invalid_confidential_instruction"
        );
    }
}

struct Source(Vec<u8>);
impl BlockSource for Source {
    fn block(&mut self, _slot: u64) -> Result<Vec<u8>> {
        Ok(self.0.clone())
    }
}
#[test]
fn unsupported_candidates_remain_durable_retryable_and_block_coverage() {
    let mut value = cases()[0]["capture"].clone();
    let mint = value["result"]["transaction"]["message"]["accountKeys"][3]
        .as_str()
        .unwrap()
        .to_owned();
    set_instruction(&mut value, &[27, 255], json!([1, 3, 2, 5, 4]));
    assert_eq!(
        decode(&value).unwrap_err().0,
        "unsupported_confidential_instruction"
    );
    let tx = value["result"].clone();
    let hash = bs58::encode([8; 32]).into_string();
    let mut source = Source(
        serde_json::to_vec(
            &json!({"jsonrpc":"2.0","id":1,"result":{"blockhash":hash,"previousBlockhash":hash,
        "parentSlot":122,"blockTime":1794430000,"transactions":[tx]}}),
        )
        .unwrap(),
    );
    let db = TestDatabase::new();
    for _ in 0..2 {
        let (coverage, _) = History::open(&db.0)
            .unwrap()
            .run(&mint, 123, 123, &mut source, || false)
            .unwrap();
        assert!(!coverage.complete);
        assert_eq!(coverage.contiguous_through, None);
        assert!(Store::query(&db.0, None, 100).unwrap().is_empty());
    }
    let mut client = db.0.test_client();
    let row = client
        .query_one("SELECT error_class,attempts FROM historical_slots", &[])
        .unwrap();
    assert_eq!(
        row.get::<_, String>(0),
        "unsupported_confidential_instruction"
    );
    assert_eq!(row.get::<_, i64>(1), 2);
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("capture.json");
    std::fs::write(&input, serde_json::to_vec(&value).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_helius-indexer"))
        .env("DATABASE_URL", TestDatabase::url())
        .args(["ingest", db.0.session(), input.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let events = String::from_utf8(output.stderr).unwrap();
    assert!(
        !events.contains(
            value["result"]["transaction"]["message"]["instructions"][0]["data"]
                .as_str()
                .unwrap()
        )
    );
    assert!(
        events
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .any(|event| event["operation"] == "decode"
                && event["error_class"] == "unsupported_confidential_instruction"
                && event["slot"] == 123)
    );
}

#[test]
fn malformed_missing_mint_and_unknown_layouts_are_never_guessed() {
    for case in cases() {
        let mut value = case["capture"].clone();
        let encoded = value["result"]["transaction"]["message"]["instructions"][0]["data"]
            .as_str()
            .unwrap();
        let data = bs58::decode(encoded).into_vec().unwrap();
        for length in [1, 2, data.len() - 1, data.len() + 1] {
            let mut invalid = data.clone();
            invalid.resize(length, 0);
            set_instruction(&mut value, &invalid, json!([1, 3, 2, 5, 4]));
            assert_eq!(
                decode(&value).unwrap_err().0,
                "invalid_confidential_instruction"
            );
        }
        set_instruction(&mut value, &data, json!([1, 99, 2, 5, 4]));
        assert_eq!(decode(&value).unwrap_err().0, "invalid_instruction_index");
        set_instruction(&mut value, &data, json!([1, 3, 2, 4]));
        assert_eq!(
            decode(&value).unwrap_err().0,
            "invalid_confidential_instruction"
        );
        value["result"]["meta"]["err"] = json!({"InstructionError":[0,"Custom"]});
        assert!(decode(&value).unwrap().is_empty());
    }
}

#[test]
fn known_zero_and_exact_volume_remain_distinct_from_hidden_amounts() {
    let mut value = cases()[0]["capture"].clone();
    for amount in [0, u64::MAX] {
        let mut data = vec![26, 1];
        data.extend(amount.to_le_bytes());
        data.push(6);
        data.extend(0u64.to_le_bytes());
        value["result"]["transaction"]["message"]["instructions"].as_array_mut().unwrap()
            .push(json!({"programIdIndex":0,"accounts":[1,3,2,4],"data":bs58::encode(data).into_string()}));
    }
    let rows = decode(&value).unwrap();
    assert_eq!(rows[0].amount_raw, None);
    assert_eq!(rows[1].amount_raw, Some(0));
    assert_eq!(rows[1].fee_raw, Some(0));
    let db = TestDatabase::new();
    let raw = serde_json::to_vec(&value).unwrap();
    let parsed = decoder::parse_capture(&raw).unwrap();
    Store::open(&db.0)
        .unwrap()
        .persist(&parsed, &raw, &Ok(rows.clone()))
        .unwrap();
    let summary = Store::summary(&db.0, &rows[0].mint).unwrap();
    assert_eq!(summary["transfer_count"], 3);
    assert_eq!(summary["known_volume_raw"], u64::MAX.to_string());
    assert_eq!(summary["unknown_amount_count"], 1);
    assert_eq!(summary["unknown_fee_count"], 1);
    assert_eq!(summary["confidential_transfer_count"], 1);
}

#[test]
fn context_proofs_and_explicit_revision_repair_preserve_identity() {
    let mut value = cases()[0]["capture"].clone();
    let encoded = value["result"]["transaction"]["message"]["instructions"][0]["data"]
        .as_str()
        .unwrap();
    let mut data = bs58::decode(encoded).into_vec().unwrap();
    data[166..].fill(0);
    set_instruction(&mut value, &data, json!([1, 3, 2, 5, 5, 5, 4]));
    let expected = decode(&value).unwrap();
    let db = TestDatabase::new();
    let raw = serde_json::to_vec(&value).unwrap();
    let parsed = decoder::parse_capture(&raw).unwrap();
    Store::open(&db.0)
        .unwrap()
        .persist(
            &parsed,
            &raw,
            &Err(helius_indexer::Error("unsupported_token_instruction")),
        )
        .unwrap();
    db.0.test_client()
        .execute(
            "UPDATE captures SET decoder_version='token-transfers-v5'",
            &[],
        )
        .unwrap();
    let report = helius_indexer::repair::run(
        &db.0,
        123,
        123,
        &mut helius_indexer::telemetry::Telemetry::default(),
        || false,
    )
    .unwrap();
    assert_eq!(report["corrected"], 1);
    assert_eq!(Store::query(&db.0, None, 100).unwrap(), expected);
    assert_eq!(
        expected[0].transfer_id,
        cases()[0]["expected"]["transfer_id"].as_str().unwrap()
    );
}
