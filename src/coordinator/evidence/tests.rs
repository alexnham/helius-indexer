use super::*;
use crate::{backfill::History, decoder, storage::Store};
use serde_json::{Value, json};

fn cases() -> Vec<Value> {
    serde_json::from_slice(include_bytes!(
        "../../../tests/fixtures/mainnet-truncated-rpc.json"
    ))
    .unwrap()
}
fn block(capture: &Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{"blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([6;32]).into_string(),"parentSlot":468265446,"blockTime":capture["result"]["blockTime"],"transactions":[capture["result"]]}})).unwrap()
}
#[test]
fn captured_truncation_is_recovered_without_acknowledging_native_rpc() {
    for case in cases() {
        let slot = 468265447;
        let mint = crate::live::USDC;
        let rpc = decoder::parse_capture(&serde_json::to_vec(&case["rpc"]).unwrap()).unwrap();
        assert_eq!(
            decoder::decode(&rpc).unwrap_err().0,
            "incomplete_execution_evidence"
        );
        let a = block(&case["archive"]);
        let b = block(&case["rpc"]);
        let path = crate::storage::pg::TestDatabase::new();
        let path = &path.0;
        let mut history = History::open(path).unwrap();
        let outcome = ingest(&mut history, mint, slot, a.clone(), b.clone(), &|| false).unwrap();
        assert!(outcome.ready() && outcome.recovered && outcome.archive_complete);
        assert!(!outcome.rpc_complete);
        assert_eq!(outcome.repaired_transactions, 1);
        let rows = Store::query(path, None, 100).unwrap();
        assert_eq!(rows.len(), case["expected"].as_array().unwrap().len());
        for (row, edge) in rows.iter().zip(case["expected"].as_array().unwrap()) {
            let actual = serde_json::to_value(row).unwrap();
            for (field, want) in edge.as_object().unwrap() {
                assert_eq!(&actual[field], want, "{field}");
            }
        }
        let mut db = path.test_client();
        let native:String=db.query_one("SELECT status FROM historical_source_slots WHERE source_kind='finalized-mainnet-rpc'", &[]).unwrap().get(0);
        assert_eq!(native, "unknown");
        assert_eq!(
            db.query_one("SELECT COUNT(*) FROM historical_inputs", &[])
                .unwrap()
                .get::<_, i64>(0),
            3
        );
        let original:Vec<u8>=db.query_one("SELECT response FROM historical_inputs WHERE source_kind='finalized-mainnet-rpc' ORDER BY attempt DESC LIMIT 1", &[]).unwrap().get(0);
        assert_eq!(original, b);
        let raw:Vec<u8>=db.query_one("SELECT response FROM historical_inputs WHERE source_kind='archive-rpc-corroborated' ORDER BY attempt DESC LIMIT 1", &[]).unwrap().get(0);
        let restored: Value = serde_json::from_slice(&raw).unwrap();
        assert_eq!(
            restored["evidence_provenance"]["archive_input_sha256"],
            format!("{:x}", Sha256::digest(&a))
        );
        assert_eq!(
            restored["evidence_provenance"]["rpc_input_sha256"],
            format!("{:x}", Sha256::digest(&b))
        );
        assert!(history.coverage(mint, slot, slot).unwrap().complete);
        let metrics = &outcome.metrics[2];
        assert!(metrics.corroborated_input);
        assert!(
            metrics
                .prometheus(&history.coverage(mint, slot, slot).unwrap())
                .contains("source_kind=\"corroborated\"")
        );
        let count = rows.len();
        let again = ingest(&mut history, mint, slot, a, b, &|| false).unwrap();
        assert!(again.ready());
        assert_eq!(Store::query(path, None, 100).unwrap().len(), count);
    }
}
#[test]
fn missing_marker_conflicting_prefix_suffix_balances_or_full_failures_do_not_recover() {
    let case = &cases()[0];
    let a = block(&case["archive"]);
    let b = block(&case["rpc"]);
    for modification in 0..5 {
        let mut aa: Value = serde_json::from_slice(&a).unwrap();
        let mut bb: Value = serde_json::from_slice(&b).unwrap();
        match modification {
            0 => {
                let logs = bb["result"]["transactions"][0]["meta"]["logMessages"]
                    .as_array_mut()
                    .unwrap();
                logs.retain(|l| l != "Log truncated");
            }
            1 => {
                bb["result"]["transactions"][0]["meta"]["logMessages"][0] =
                    json!("conflicting observed prefix")
            }
            2 => bb["result"]["transactions"][0]["meta"]["logMessages"]
                .as_array_mut()
                .unwrap()
                .push(json!("conflicting observed suffix")),
            3 => {
                bb["result"]["transactions"][0]["meta"]["preTokenBalances"][0]["mint"] =
                    json!(bs58::encode([99; 32]).into_string())
            }
            _ => aa["result"]["transactions"][0]["meta"]["logMessages"]
                .as_array_mut()
                .unwrap()
                .push(json!(format!("Program {} failed:", decoder::SPL_TOKEN))),
        }
        assert!(
            corroborate(
                &serde_json::to_vec(&aa).unwrap(),
                &serde_json::to_vec(&bb).unwrap(),
                468265447,
                &|| false
            )
            .is_err(),
            "modification {modification}"
        );
    }
}

#[test]
fn recovery_cancellation_and_transaction_limit_do_not_create_partial_evidence() {
    let all = cases();
    let mut a: Value = serde_json::from_slice(&block(&all[0]["archive"])).unwrap();
    let mut b: Value = serde_json::from_slice(&block(&all[0]["rpc"])).unwrap();
    a["result"]["transactions"] = json!([all[0]["archive"]["result"], all[1]["archive"]["result"]]);
    b["result"]["transactions"] = json!([all[0]["rpc"]["result"], all[1]["rpc"]["result"]]);
    let checks = std::cell::Cell::new(0);
    let cancelled = corroborate(
        &serde_json::to_vec(&a).unwrap(),
        &serde_json::to_vec(&b).unwrap(),
        468265447,
        &|| {
            let n = checks.get();
            checks.set(n + 1);
            n > 0
        },
    );
    assert_eq!(cancelled.unwrap_err().0, "cancelled");
    a["result"]["transactions"] = Value::Array(vec![all[0]["archive"]["result"].clone(); 17]);
    b["result"]["transactions"] = Value::Array(vec![all[0]["rpc"]["result"].clone(); 17]);
    assert_eq!(
        corroborate(
            &serde_json::to_vec(&a).unwrap(),
            &serde_json::to_vec(&b).unwrap(),
            468265447,
            &|| false
        )
        .unwrap_err()
        .0,
        "rpc_evidence_repair_limit"
    );
}
