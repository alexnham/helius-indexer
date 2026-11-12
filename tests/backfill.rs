use helius_indexer::{
    Error, Result,
    backfill::{BlockSource, History, MAX_SLOTS},
    decoder,
    storage::Store,
};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{cell::Cell, collections::BTreeMap, path::Path};
use tempfile::TempDir;

struct Source {
    blocks: BTreeMap<u64, Vec<u8>>,
    calls: Vec<u64>,
}
impl BlockSource for Source {
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        self.calls.push(slot);
        self.blocks
            .get(&slot)
            .cloned()
            .ok_or(Error("rpc_request_failed"))
    }
}
fn mint() -> String {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".into()
}
fn fixture() -> Value {
    serde_json::from_slice(include_bytes!("fixtures/mainnet-transfer.json")).unwrap()
}
fn block(slot: u64, parent: u64, transactions: Vec<Value>) -> Vec<u8> {
    let mut txs = transactions;
    for tx in &mut txs {
        tx.as_object_mut().unwrap().remove("slot");
        tx.as_object_mut().unwrap().remove("blockTime");
    }
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{"blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([6;32]).into_string(),"parentSlot":parent,"blockTime":fixture()["result"]["blockTime"],"transactions":txs,"test_slot":slot}})).unwrap()
}
fn source(blocks: Vec<(u64, Vec<u8>)>) -> Source {
    Source {
        blocks: blocks.into_iter().collect(),
        calls: Vec::new(),
    }
}
fn db() -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("history.sqlite");
    (dir, path)
}
fn sql(path: &Path, query: &str) {
    Connection::open(path)
        .unwrap()
        .execute_batch(query)
        .unwrap();
}

#[test]
fn captured_input_and_historical_block_have_identical_queries_and_restart_is_a_noop() {
    let (_dir, path) = db();
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    let mut source = source(vec![(
        slot,
        block(slot, slot - 1, vec![fixture()["result"].clone()]),
    )]);
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), slot, slot, &mut source, || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(coverage.contiguous_through, Some(slot));
    assert_eq!(metrics.decoded_transfers, 2);
    let expected = decoder::decode(
        &decoder::parse_capture(include_bytes!("fixtures/mainnet-transfer.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(Store::query(&path, Some(&mint()), 1000).unwrap(), expected);
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), slot, slot, &mut source, || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(metrics.source_attempts, 0);
    assert_eq!(source.calls.len(), 1);
    let db = Connection::open(&path).unwrap();
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM historical_inputs", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn empty_produced_skipped_and_unknown_slots_are_distinct_and_checkpoint_cannot_jump() {
    let (_dir, path) = db();
    let mut source = source(vec![
        (12, block(12, 10, vec![])),
        (14, block(14, 13, vec![])),
    ]);
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), 11, 15, &mut source, || false)
        .unwrap();
    assert_eq!(
        coverage
            .slots
            .iter()
            .map(|s| s.status.as_str())
            .collect::<Vec<_>>(),
        ["skipped", "complete", "unknown", "complete", "unknown"]
    );
    assert_eq!(coverage.contiguous_through, Some(12));
    assert!(!coverage.complete);
    assert_eq!(coverage.pending, 2);
    assert_eq!(metrics.source_errors, 3);
    assert!(
        metrics
            .prometheus(&coverage)
            .contains("indexer_coverage_valid{mode=\"historical\"} 0")
    );
    source.blocks.insert(13, block(13, 12, vec![]));
    source.blocks.insert(15, block(15, 14, vec![]));
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), 11, 15, &mut source, || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(coverage.contiguous_through, Some(15));
    assert_eq!(metrics.retries, 2);
    assert!(Store::query(&path, None, 1000).unwrap().is_empty());
    // A new configured mint has no inherited coverage, even for an observed empty range.
    let other = bs58::encode([42; 32]).into_string();
    assert!(
        !History::open(&path)
            .unwrap()
            .coverage(&other, 11, 15)
            .unwrap()
            .complete
    );
}

#[test]
fn missing_metadata_null_blocks_and_rpc_skipped_errors_stay_retryable() {
    let (_dir, path) = db();
    let mut tx = fixture()["result"].clone();
    tx["meta"]["innerInstructions"] = Value::Null;
    let mut source=source(vec![(20,block(20,19,vec![tx])),(21,serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":null})).unwrap()),(22,serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"error":{"code":-32007,"message":"slot skipped or missing"}})).unwrap())]);
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), 20, 22, &mut source, || false)
        .unwrap();
    assert_eq!(coverage.pending, 3);
    assert_eq!(coverage.contiguous_through, None);
    assert_eq!(metrics.decode_failures, 1);
    assert_eq!(metrics.source_errors, 2);
    assert_eq!(
        coverage.slots[0].error_class.as_deref(),
        Some("missing_inner_instructions")
    );
    assert!(Store::query(&path, None, 1000).unwrap().is_empty());
    assert_eq!(
        Connection::open(&path)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM historical_inputs", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        3
    );
}

#[test]
fn cancellation_after_raw_ack_replays_durable_input_without_source() {
    let (_dir, path) = db();
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    let mut source = source(vec![(
        slot,
        block(slot, slot - 1, vec![fixture()["result"].clone()]),
    )]);
    let checks = Cell::new(0);
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), slot, slot, &mut source, || {
            checks.set(checks.get() + 1);
            checks.get() > 1
        })
        .unwrap();
    assert!(metrics.cancelled);
    assert!(!coverage.complete);
    source.blocks.clear();
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), slot, slot, &mut source, || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(metrics.source_attempts, 0);
    assert_eq!(source.calls.len(), 1);
    assert_eq!(Store::query(&path, None, 1000).unwrap().len(), 2);
}

#[test]
fn crash_at_coverage_ack_leaves_output_durable_and_restart_deduplicates() {
    let (_dir, path) = db();
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    History::open(&path).unwrap();
    sql(
        &path,
        "CREATE TRIGGER fail_ack BEFORE UPDATE OF status ON historical_slots WHEN NEW.status='complete' BEGIN SELECT RAISE(ABORT,'injected'); END;",
    );
    let mut source = source(vec![(
        slot,
        block(slot, slot - 1, vec![fixture()["result"].clone()]),
    )]);
    assert!(
        History::open(&path)
            .unwrap()
            .run(&mint(), slot, slot, &mut source, || false)
            .is_err()
    );
    assert_eq!(Store::query(&path, None, 1000).unwrap().len(), 2);
    assert!(
        !History::open(&path)
            .unwrap()
            .coverage(&mint(), slot, slot)
            .unwrap()
            .complete
    );
    sql(&path, "DROP TRIGGER fail_ack;");
    source.blocks.clear();
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), slot, slot, &mut source, || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(metrics.source_attempts, 0);
    assert_eq!(Store::query(&path, None, 1000).unwrap().len(), 2);
}

#[test]
fn write_failure_cannot_advance_coverage_and_raw_is_retained() {
    let (_dir, path) = db();
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    History::open(&path).unwrap();
    sql(
        &path,
        "CREATE TRIGGER fail_capture BEFORE INSERT ON captures BEGIN SELECT RAISE(ABORT,'injected'); END;",
    );
    let mut source = source(vec![(
        slot,
        block(slot, slot - 1, vec![fixture()["result"].clone()]),
    )]);
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), slot, slot, &mut source, || false)
        .unwrap();
    assert!(!coverage.complete);
    assert_eq!(metrics.write_errors, 1);
    assert!(Store::query(&path, None, 1000).unwrap().is_empty());
    assert_eq!(
        Connection::open(&path)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM historical_inputs", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    sql(&path, "DROP TRIGGER fail_capture;");
    assert!(
        History::open(&path)
            .unwrap()
            .run(&mint(), slot, slot, &mut source, || false)
            .unwrap()
            .0
            .complete
    );
}

#[test]
fn range_and_response_boundaries_are_checked_offline() {
    let (_dir, path) = db();
    let mut source = source(vec![]);
    let mut history = History::open(&path).unwrap();
    for (start, end) in [(2, 1), (0, MAX_SLOTS), (i64::MAX as u64, u64::MAX)] {
        assert!(
            history
                .run(&mint(), start, end, &mut source, || false)
                .is_err()
        );
    }
    assert!(history.coverage("invalid", 1, 2).is_err());
    assert!(source.calls.is_empty());
    assert!(helius_indexer::backfill::RpcSource::new("http://secret.example".into()).is_err());
}

#[test]
fn enriched_historical_metadata_repairs_an_unresolved_candidate() {
    let (_dir, path) = db();
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    let mut tx = fixture()["result"].clone();
    tx["meta"]["innerInstructions"] = Value::Null;
    let mut source = source(vec![(slot, block(slot, slot - 1, vec![tx]))]);
    assert!(
        !History::open(&path)
            .unwrap()
            .run(&mint(), slot, slot, &mut source, || false)
            .unwrap()
            .0
            .complete
    );
    source.blocks.insert(
        slot,
        block(slot, slot - 1, vec![fixture()["result"].clone()]),
    );
    assert!(
        History::open(&path)
            .unwrap()
            .run(&mint(), slot, slot, &mut source, || false)
            .unwrap()
            .0
            .complete
    );
    assert_eq!(Store::query(&path, None, 1000).unwrap().len(), 2);
    assert_eq!(
        Connection::open(&path)
            .unwrap()
            .query_row("SELECT COUNT(*) FROM historical_inputs", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn raw_write_failure_never_decodes_or_acknowledges_output() {
    let (_dir, path) = db();
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    History::open(&path).unwrap();
    sql(
        &path,
        "CREATE TRIGGER fail_raw BEFORE INSERT ON historical_inputs BEGIN SELECT RAISE(ABORT,'injected'); END;",
    );
    let mut source = source(vec![(
        slot,
        block(slot, slot - 1, vec![fixture()["result"].clone()]),
    )]);
    assert!(
        History::open(&path)
            .unwrap()
            .run(&mint(), slot, slot, &mut source, || false)
            .is_err()
    );
    assert!(Store::query(&path, None, 1000).unwrap().is_empty());
    assert!(
        !History::open(&path)
            .unwrap()
            .coverage(&mint(), slot, slot)
            .unwrap()
            .complete
    );
    sql(&path, "DROP TRIGGER fail_raw;");
    assert!(
        History::open(&path)
            .unwrap()
            .run(&mint(), slot, slot, &mut source, || false)
            .unwrap()
            .0
            .complete
    );
}

#[test]
fn telemetry_distinguishes_unscheduled_age_from_an_observed_empty_backlog() {
    let (_dir, path) = db();
    let mut history = History::open(&path).unwrap();
    assert_eq!(
        history
            .coverage(&mint(), 10, 10)
            .unwrap()
            .oldest_pending_age_seconds,
        None
    );
    let mut source = source(vec![(10, block(10, 9, vec![]))]);
    let (coverage, metrics) = history.run(&mint(), 10, 10, &mut source, || false).unwrap();
    assert_eq!(coverage.oldest_pending_age_seconds, Some(0));
    let text = metrics.prometheus(&coverage);
    assert!(text.contains("indexer_missing_ranges{mode=\"historical\"} 0"));
    assert!(text.contains("indexer_oldest_pending_age_seconds{mode=\"historical\"} 0"));
    for line in text.lines() {
        assert!(!line.contains(&mint()));
        assert!(!line.contains("slot="));
    }
}

#[test]
fn malformed_earlier_response_cannot_break_valid_child_skip_evidence() {
    let (_dir, path) = db();
    let mut source = source(vec![
        (11, b"not-json-provider-response".to_vec()),
        (12, block(12, 10, vec![])),
    ]);
    let (coverage, _) = History::open(&path)
        .unwrap()
        .run(&mint(), 11, 12, &mut source, || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(coverage.slots[0].status, "skipped");
    assert_eq!(coverage.slots[1].status, "complete");
}
