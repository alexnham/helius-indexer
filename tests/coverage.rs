//! Independent classic SPL expectations from retained finalized RPC blocks.
use helius_indexer::{decoder, live::USDC, storage::Store};
use serde_json::Value;
use std::collections::BTreeMap;

#[test]
fn sampled_classic_usdc_edges_have_exact_amounts_and_positions() {
    let cases: Vec<Value> =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-coverage.json")).unwrap();
    let path = helius_indexer::storage::pg::TestDatabase::new();
    let path = &path.0;
    let mut store = Store::open(path).unwrap();
    let mut volume = 0u128;
    let mut expected_count = 0;
    let mut found = 0;
    let mut failures = BTreeMap::new();
    for case in cases {
        let raw = serde_json::to_vec(&case["capture"]).unwrap();
        let capture = decoder::parse_capture(&raw).unwrap();
        let expected = case["expected"].as_array().unwrap();
        expected_count += expected.len();
        match decoder::decode(&capture) {
            Ok(rows) => {
                store.persist(&capture, &raw, &Ok(rows.clone())).unwrap();
                assert_eq!(store.persist(&capture, &raw, &Ok(rows.clone())).unwrap(), 0);
                let actual: BTreeMap<_, _> = rows
                    .iter()
                    .filter(|r| r.mint == USDC)
                    .map(|r| (r.transfer_id.as_str(), r))
                    .collect();
                assert_eq!(actual.len(), expected.len());
                for edge in expected {
                    let row = actual[edge["transfer_id"].as_str().unwrap()];
                    assert_eq!(row.amount_raw.to_string(), edge["amount_raw"]);
                    assert_eq!(row.source_token_account, edge["source_token_account"]);
                    assert_eq!(
                        row.destination_token_account,
                        edge["destination_token_account"]
                    );
                    assert_eq!(row.fee_raw, Some(0));
                    volume += u128::from(row.amount_raw);
                    found += 1;
                }
            }
            Err(e) => *failures.entry(e.0).or_insert(0) += expected.len(),
        }
    }
    assert_eq!(expected_count, 97);
    println!("found={found}/{expected_count}; unresolved={failures:?}");
    assert_eq!(found, 97);
    let summary = Store::summary(path, USDC).unwrap();
    assert_eq!(summary["transfer_count"], 97);
    assert_eq!(summary["known_volume_raw"], volume.to_string());
    assert_eq!(summary["unknown_fee_count"], 0);
}

#[test]
fn captured_native_unwrap_and_fee_harvest_are_not_transfer_edges() {
    for bytes in [
        include_bytes!("fixtures/mainnet-unwrap.json").as_slice(),
        include_bytes!("fixtures/mainnet-fee-maintenance.json").as_slice(),
    ] {
        let capture = decoder::parse_capture(bytes).unwrap();
        assert!(decoder::decode(&capture).unwrap().is_empty());
    }
}
