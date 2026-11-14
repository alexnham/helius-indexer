use helius_indexer::{decoder, storage::Store};
use serde_json::Value;

#[test]
fn captured_authority_and_fee_maintenance_preserve_exact_ordinary_edges() {
    let cases: Vec<Value> =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-maintenance.json")).unwrap();
    let path = helius_indexer::storage::pg::TestDatabase::new();
    let path = &path.0;
    let mut store = Store::open(path).unwrap();
    for case in cases {
        let raw = serde_json::to_vec(&case["capture"]).unwrap();
        let capture = decoder::parse_capture(&raw).unwrap();
        let rows = decoder::decode(&capture).unwrap_or_else(|e| {
            panic!(
                "slot {} / {}: {}",
                capture.result.slot, capture.result.transaction.signatures[0], e.0
            )
        });
        let expected = case["expected"].as_array().unwrap();
        assert_eq!(rows.len(), expected.len());
        for (row, edge) in rows.iter().zip(expected) {
            let value = serde_json::to_value(row).unwrap();
            for (field, want) in edge.as_object().unwrap() {
                assert_eq!(&value[field], want, "{field}");
            }
        }
        store.persist(&capture, &raw, &Ok(rows.clone())).unwrap();
        assert_eq!(store.persist(&capture, &raw, &Ok(rows)).unwrap(), 0);
    }
    assert_eq!(Store::query(path, None, 1000).unwrap().len(), 61);
}

#[test]
fn revision_repair_adds_previously_rejected_rows_without_relabeling_old_output() {
    let cases: Vec<Value> =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-maintenance.json")).unwrap();
    let case = &cases[0];
    let raw = serde_json::to_vec(&case["capture"]).unwrap();
    let capture = decoder::parse_capture(&raw).unwrap();
    let path = helius_indexer::storage::pg::TestDatabase::new();
    let path = &path.0;
    let mut store = Store::open(path).unwrap();
    store
        .persist(
            &capture,
            &raw,
            &Err(helius_indexer::Error("unsupported_authority_change")),
        )
        .unwrap();
    let mut db = path.test_client();
    db.execute(
        "UPDATE captures SET decoder_version='token-transfers-v4'",
        &[],
    )
    .unwrap();
    assert!(Store::query(path, None, 100).unwrap().is_empty());
    let report = helius_indexer::repair::run(
        path,
        capture.result.slot,
        capture.result.slot,
        &mut helius_indexer::telemetry::Telemetry::default(),
        || false,
    )
    .unwrap();
    assert_eq!(report["corrected"], 1);
    assert_eq!(
        Store::query(path, None, 100).unwrap().len(),
        case["expected"].as_array().unwrap().len()
    );
    assert_eq!(
        helius_indexer::repair::run(
            path,
            capture.result.slot,
            capture.result.slot,
            &mut helius_indexer::telemetry::Telemetry::default(),
            || false
        )
        .unwrap()["attempted"],
        0
    );
}

#[test]
fn retained_archive_revision_repair_recovers_previously_undecodable_candidates_offline() {
    let cases: Vec<Value> =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-maintenance.json")).unwrap();
    let case = &cases[0];
    let slot = case["capture"]["result"]["slot"].as_u64().unwrap();
    let mint = case["expected"][0]["mint"].as_str().unwrap();
    let raw=serde_json::to_vec(&serde_json::json!({"jsonrpc":"2.0","id":1,"result":{"blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([6;32]).into_string(),"parentSlot":slot-1,"blockTime":case["capture"]["result"]["blockTime"],"transactions":[case["capture"]["result"]]}})).unwrap();
    let path = helius_indexer::storage::pg::TestDatabase::new();
    let path = &path.0;
    helius_indexer::backfill::History::open(path).unwrap();
    let mut db = path.test_client();
    db.execute("INSERT INTO historical_inputs(mint,decoder,slot,attempt,source_kind,received_at,produced_block,response) VALUES($1,'token-transfers-v4',$2,1,'old-faithful-jetstreamer',1,true,$3)",&[&mint, &(slot as i64), &raw]).unwrap();
    let output = helius_indexer::repair::archive_history(
        path,
        &mut helius_indexer::telemetry::Telemetry::default(),
        mint,
        slot,
        slot,
        || false,
    )
    .unwrap();
    assert_eq!(output["coverage"]["complete"], true);
    assert_eq!(output["metrics"]["source_attempts"], 0);
    assert_eq!(output["metrics"]["cached_blocks"], 1);
    assert_eq!(
        Store::query(path, None, 100).unwrap().len(),
        case["expected"].as_array().unwrap().len()
    );
    let again = helius_indexer::repair::archive_history(
        path,
        &mut helius_indexer::telemetry::Telemetry::default(),
        mint,
        slot,
        slot,
        || false,
    )
    .unwrap();
    assert_eq!(again["metrics"]["source_attempts"], 0);
    assert_eq!(again["coverage"]["complete"], true);
}

#[test]
fn captured_rpc_and_archive_authority_observations_reconcile_in_either_order() {
    let cases: Vec<Value> =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-maintenance.json")).unwrap();
    let case = &cases[0];
    let inputs = [&case["capture"], &case["alternate_capture"]];
    for order in [[0usize, 1], [1, 0]] {
        let path = helius_indexer::storage::pg::TestDatabase::new();
        let path = &path.0;
        let mut store = Store::open(path).unwrap();
        let mut expected = None;
        for index in order {
            let raw = serde_json::to_vec(inputs[index]).unwrap();
            let capture = decoder::parse_capture(&raw).unwrap();
            let rows = decoder::decode(&capture).unwrap();
            if let Some(prior) = &expected {
                assert_eq!(prior, &rows);
            } else {
                expected = Some(rows.clone());
            }
            store.persist(&capture, &raw, &Ok(rows)).unwrap();
        }
        let result = Store::query(path, None, 100).unwrap();
        assert_eq!(result.len(), case["expected"].as_array().unwrap().len());
        let mut db = path.test_client();
        assert_eq!(
            db.query_one("SELECT COUNT(DISTINCT source) FROM capture_inputs", &[])
                .unwrap()
                .get::<_, i64>(0),
            2
        );
    }
}
