use helius_indexer::{
    Error, Result,
    backfill::{BlockSource, History, MAX_SLOTS},
    decoder,
    storage::{Database, Store, pg::TestDatabase},
};
use serde_json::{Value, json};
use std::{cell::Cell, collections::BTreeMap};

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
    serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{"blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([7;32]).into_string(),"parentSlot":parent,"blockTime":fixture()["result"]["blockTime"],"transactions":txs,"test_slot":slot}})).unwrap()
}
fn source(blocks: Vec<(u64, Vec<u8>)>) -> Source {
    Source {
        blocks: blocks.into_iter().collect(),
        calls: Vec::new(),
    }
}
fn db() -> (TestDatabase, Database) {
    let test = TestDatabase::new();
    let database = test.0.clone();
    (test, database)
}
fn sql(path: &Database, query: &str) {
    path.test_client().batch_execute(query).unwrap();
}

#[test]
fn a_full_block_stores_only_transactions_that_touch_the_mint() {
    let (_dir, path) = db();
    let usdc = fixture()["result"].clone();
    let slot = usdc["slot"].as_u64().unwrap();
    // The same transaction for another mint, with its own signature.
    let other = bs58::encode([9; 32]).into_string();
    let mut unrelated: Value =
        serde_json::from_str(&usdc.to_string().replace(&mint(), &other)).unwrap();
    unrelated["transaction"]["signatures"][0] = json!(bs58::encode([5; 64]).into_string());
    let mut source = source(vec![(slot, block(slot, slot - 1, vec![usdc, unrelated]))]);
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), slot, slot, &mut source, || false)
        .unwrap();
    assert!(coverage.complete, "the slot is still read whole");
    assert_eq!(metrics.unrelated_transactions, 1);
    assert_eq!(metrics.decoded_transactions, 1);
    let expected = decoder::decode(
        &decoder::parse_capture(include_bytes!("fixtures/mainnet-transfer.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(Store::query(&path, None, 1000).unwrap(), expected);
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
    let mut db = path.test_client();
    assert_eq!(
        db.query_one("SELECT COUNT(*) FROM historical_inputs", &[])
            .unwrap()
            .get::<_, i64>(0),
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
        path.test_client()
            .query_one("SELECT COUNT(*) FROM historical_inputs", &[])
            .unwrap()
            .get::<_, i64>(0),
        3
    );
}

#[test]
fn retained_blocks_keep_unrelated_transactions_as_signatures_and_replay_offline() {
    let usdc = fixture()["result"].clone();
    let slot = usdc["slot"].as_u64().unwrap();
    let unrelated = numbered(1, false);
    let raw = block(slot, slot - 1, vec![usdc, unrelated.clone()]);
    let expected = decoder::decode(
        &decoder::parse_capture(include_bytes!("fixtures/mainnet-transfer.json")).unwrap(),
    )
    .unwrap();
    for workers in [1, 2] {
        let (_dir, path) = db();
        let mut source = source(vec![(slot, raw.clone())]);
        let checks = Cell::new(0);
        let mut history = parallel_history(&path, workers);
        let (_, metrics) = history
            .run(&mint(), slot, slot, &mut source, || {
                checks.set(checks.get() + 1);
                checks.get() > 1
            })
            .unwrap();
        assert!(metrics.cancelled);
        let row = path
            .test_client()
            .query_one("SELECT pruned,response FROM historical_inputs", &[])
            .unwrap();
        assert!(row.get::<_, bool>(0));
        let stored: Value = serde_json::from_slice(&row.get::<_, Vec<u8>>(1)).unwrap();
        let txs = stored["result"]["transactions"].as_array().unwrap();
        assert_eq!(txs[0]["meta"], fixture()["result"]["meta"]);
        assert_eq!(
            txs[1],
            json!({"transaction":{"signatures":unrelated["transaction"]["signatures"]}})
        );
        assert_eq!(
            stored["result"]["blockhash"],
            json!(bs58::encode([7; 32]).into_string())
        );
        assert!(metrics.retained_bytes > 0 && metrics.retained_bytes < metrics.input_bytes);
        // The pruned block alone completes the slot: same transfers, same block identity.
        source.blocks.clear();
        let (coverage, metrics) = parallel_history(&path, workers)
            .run(&mint(), slot, slot, &mut source, || false)
            .unwrap();
        assert!(coverage.complete);
        assert_eq!(metrics.source_attempts, 0);
        assert_eq!(metrics.unrelated_transactions, 1);
        assert_eq!(Store::query(&path, None, 1000).unwrap(), expected);
        let marked: Vec<bool> = path
            .test_client()
            .query("SELECT pruned FROM historical_inputs ORDER BY attempt", &[])
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(marked, [true, true], "a reused pruned block stays marked");
    }
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
        "CREATE OR REPLACE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_ack BEFORE UPDATE OF status ON historical_slots FOR EACH ROW WHEN (NEW.status='complete') EXECUTE FUNCTION fail();",
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
    sql(&path, "DROP TRIGGER fail_ack ON historical_slots;");
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
        "CREATE OR REPLACE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_capture BEFORE INSERT ON captures FOR EACH ROW EXECUTE FUNCTION fail();",
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
        path.test_client()
            .query_one("SELECT COUNT(*) FROM historical_inputs", &[])
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    sql(&path, "DROP TRIGGER fail_capture ON captures;");
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
        path.test_client()
            .query_one("SELECT COUNT(*) FROM historical_inputs", &[])
            .unwrap()
            .get::<_, i64>(0),
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
        "CREATE OR REPLACE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_raw BEFORE INSERT ON historical_inputs FOR EACH ROW EXECUTE FUNCTION fail();",
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
    sql(&path, "DROP TRIGGER fail_raw ON historical_inputs;");
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
    // Strict text-format parsers require each metric family's samples to be contiguous.
    let family = |line: &str| {
        let name = line.split(['{', ' ']).next().unwrap();
        ["_bucket", "_sum", "_count"]
            .iter()
            .find_map(|suffix| name.strip_suffix(suffix))
            .unwrap_or(name)
            .to_string()
    };
    let mut finished = std::collections::HashSet::new();
    let mut current = String::new();
    for line in text
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
    {
        let name = family(line);
        if name != current {
            assert!(finished.insert(current.clone()), "{current} is split");
            assert!(!finished.contains(&name), "{name} is split");
            current = name;
        }
    }
    assert!(text.contains(
        "indexer_stage_duration_seconds_count{operation=\"transfer_write\",mode=\"historical\"}"
    ));
    assert!(text.contains(
        "indexer_stage_duration_seconds_count{operation=\"historical_range\",mode=\"historical\"} 1"
    ));
}

#[test]
fn range_progress_and_rate_fields_describe_complete_and_incomplete_ranges() {
    let (_dir, path) = db();
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    let mut full = source(vec![
        (
            slot,
            block(slot, slot - 1, vec![fixture()["result"].clone()]),
        ),
        (slot + 1, block(slot + 1, slot, vec![])),
    ]);
    let (coverage, metrics) = History::open(&path)
        .unwrap()
        .run(&mint(), slot, slot + 1, &mut full, || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(coverage.progress.slots_requested, 2);
    assert_eq!(coverage.progress.slots_completed, 2);
    assert_eq!(coverage.progress.first_slot, slot);
    assert_eq!(coverage.progress.last_slot, slot + 1);
    let json = serde_json::to_value(&metrics).unwrap();
    assert_eq!(json["decoded_transactions"], 1);
    assert!(json["transactions_per_second"].as_f64().unwrap() >= 0.0);
    assert_eq!(
        serde_json::to_value(&coverage).unwrap()["progress"]["slots_completed"],
        2
    );
    let text = metrics.prometheus(&coverage);
    for line in [
        "indexer_historical_range_slots_requested{mode=\"historical\"} 2".to_string(),
        "indexer_historical_range_slots_completed{mode=\"historical\"} 2".into(),
        format!("indexer_historical_range_first_slot{{mode=\"historical\"}} {slot}"),
        format!(
            "indexer_historical_range_last_slot{{mode=\"historical\"}} {}",
            slot + 1
        ),
        "indexer_historical_transactions_per_second ".into(),
    ] {
        assert!(text.contains(&line), "{line}");
    }

    let (_dir, path) = db();
    let mut partial = source(vec![(
        slot,
        block(slot, slot - 1, vec![fixture()["result"].clone()]),
    )]);
    let (coverage, _) = History::open(&path)
        .unwrap()
        .run(&mint(), slot, slot + 1, &mut partial, || false)
        .unwrap();
    assert!(!coverage.complete);
    assert_eq!(coverage.progress.slots_requested, 2);
    assert_eq!(coverage.progress.slots_completed, 1);
    assert!(
        metrics
            .prometheus(&coverage)
            .contains("indexer_historical_range_slots_completed{mode=\"historical\"} 1")
    );
}

#[test]
#[ignore = "subprocess helper for range completion event tests"]
fn historical_range_event_child() {
    let session = std::env::var("INDEXER_TEST_EVENT_DB").unwrap();
    let path = &Database::new(TestDatabase::url(), &session).unwrap();
    let phase = std::env::var("INDEXER_TEST_EVENT_PHASE").unwrap();
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    let mut source = source(vec![
        (
            slot,
            block(slot, slot - 1, vec![fixture()["result"].clone()]),
        ),
        (slot + 1, block(slot + 1, slot, vec![])),
    ]);
    if phase == "incomplete" {
        source.blocks.remove(&(slot + 1));
    }
    // The cancel check runs at the top of each slot and once per transaction; the third call is slot+1.
    let checks = Cell::new(0);
    let result = History::open(path)
        .unwrap()
        .run(&mint(), slot, slot + 1, &mut source, || {
            checks.set(checks.get() + 1);
            phase == "cancel" && checks.get() > 2
        });
    if phase == "failure" {
        assert!(result.is_err());
    } else {
        result.unwrap();
    }
    std::process::exit(0);
}

#[test]
fn range_completion_event_is_emitted_once_only_after_the_whole_range_is_durable() {
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    for phase in ["success", "cancel", "incomplete", "failure"] {
        let (_dir, path) = db();
        History::open(&path).unwrap();
        if phase == "failure" {
            // The first verified-slot acknowledgment fails, so run returns an error.
            sql(
                &path,
                "CREATE OR REPLACE FUNCTION fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected'; END$$; CREATE TRIGGER fail_ack BEFORE UPDATE OF status ON historical_slots FOR EACH ROW WHEN (NEW.status='complete') EXECUTE FUNCTION fail();",
            );
        }
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "historical_range_event_child", "--ignored"])
            .env("INDEXER_TEST_EVENT_DB", path.session())
            .env("INDEXER_TEST_EVENT_PHASE", phase)
            .output()
            .unwrap();
        let stderr = String::from_utf8(output.stderr).unwrap();
        assert!(output.status.success(), "{phase}: {stderr}");
        let completed: Vec<&str> = stderr
            .lines()
            .filter(|line| line.contains("\"event\":\"historical_range_completed\""))
            .collect();
        if phase != "success" {
            assert!(completed.is_empty(), "{phase}: {stderr}");
            continue;
        }
        assert_eq!(completed.len(), 1);
        let event: Value = serde_json::from_str(completed[0]).unwrap();
        assert_eq!(event["outcome"], "success");
        assert_eq!(event["mode"], "historical");
        assert_eq!(event["mint"], mint());
        assert_eq!(event["slot_start"], slot);
        assert_eq!(event["slot_end"], slot + 1);
        assert_eq!(event["decoder_version"], decoder::DECODER_VERSION);
        // The per-slot durable_range_completed event is unchanged: one per acknowledged slot.
        assert_eq!(
            stderr
                .lines()
                .filter(|line| line.contains("\"event\":\"durable_range_completed\""))
                .count(),
            2
        );
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

// Synthetic second signature makes two distinguishable transaction commits from one captured shape.
fn recovery_transactions() -> Vec<Value> {
    let first = fixture()["result"].clone();
    let mut second = first.clone();
    second["transaction"]["signatures"][0] = json!(bs58::encode([42; 64]).into_string());
    vec![first, second]
}

#[test]
#[ignore = "subprocess helper for process-exit recovery tests"]
fn historical_process_exit_child() {
    let session = std::env::var("INDEXER_TEST_RECOVERY_DB").unwrap();
    let path = &Database::new(TestDatabase::url(), &session).unwrap();
    let phase = std::env::var("INDEXER_TEST_RECOVERY_PHASE").unwrap();
    let slot = fixture()["result"]["slot"].as_u64().unwrap();
    let mut history = History::open(path).unwrap();
    let db = Cell::new(Some(path.test_client()));
    let mut source = source(vec![(slot, block(slot, slot - 1, recovery_transactions()))]);
    history
        .run(&mint(), slot, slot, &mut source, || {
            let mut client = db.take().unwrap();
            let raw: i64 = client
                .query_one("SELECT COUNT(*) FROM historical_inputs", &[])
                .unwrap()
                .get(0);
            db.set(Some(client));
            let output = Store::query(path, None, 100).unwrap().len();
            if (phase == "after_raw" && raw > 0) || (phase == "after_output" && output > 0) {
                std::process::exit(77);
            }
            false
        })
        .unwrap();
    assert_eq!(phase, "after_progress");
    std::process::exit(77);
}

#[test]
fn historical_process_exit_restarts_from_retained_raw_without_advancing_a_gap() {
    for phase in ["after_raw", "after_output", "after_progress"] {
        let (_dir, path) = db();
        let slot = fixture()["result"]["slot"].as_u64().unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "historical_process_exit_child", "--ignored"])
            .env("INDEXER_TEST_RECOVERY_DB", path.session())
            .env("INDEXER_TEST_RECOVERY_PHASE", phase)
            .output()
            .unwrap()
            .status;
        assert_eq!(status.code(), Some(77), "{phase}");
        let mut history = History::open(&path).unwrap();
        let before = history.coverage(&mint(), slot, slot).unwrap();
        assert_eq!(before.complete, phase == "after_progress");
        assert_eq!(
            before.contiguous_through,
            (phase == "after_progress").then_some(slot)
        );
        // A block's output is written as one batch, so a crash after output sees all of it.
        let expected_before = if phase == "after_raw" { 0 } else { 4 };
        assert_eq!(
            Store::query(&path, None, 100).unwrap().len(),
            expected_before
        );
        let mut conn = path.test_client();
        let first: i64 = conn
            .query_one("SELECT first_pending FROM historical_slots", &[])
            .unwrap()
            .get(0);
        let mut unavailable = source(vec![]);
        let (coverage, metrics) = history
            .run(&mint(), slot, slot, &mut unavailable, || false)
            .unwrap();
        assert!(coverage.complete);
        assert_eq!(coverage.contiguous_through, Some(slot));
        assert_eq!(metrics.source_attempts, 0);
        assert!(unavailable.calls.is_empty());
        let mut expected = Vec::new();
        for result in recovery_transactions() {
            let mut envelope = fixture();
            envelope["result"] = result;
            expected.extend(
                decoder::decode(
                    &decoder::parse_capture(&serde_json::to_vec(&envelope).unwrap()).unwrap(),
                )
                .unwrap(),
            );
        }
        expected.sort_by(|a, b| a.transfer_id.cmp(&b.transfer_id));
        let mut actual = Store::query(&path, None, 100).unwrap();
        actual.sort_by(|a, b| a.transfer_id.cmp(&b.transfer_id));
        assert_eq!(actual, expected);
        assert_eq!(
            conn.query_one("SELECT first_pending FROM historical_slots", &[])
                .unwrap()
                .get::<_, i64>(0),
            first
        );
        let summary = Store::summary(&path, &mint()).unwrap();
        history
            .run(&mint(), slot, slot, &mut unavailable, || false)
            .unwrap();
        assert_eq!(Store::summary(&path, &mint()).unwrap(), summary);
    }
}

/// The fixture transfer with signature `n`, or, when `related` is false, a failed
/// transaction that neither touches the mint nor produces output.
fn numbered(n: u64, related: bool) -> Value {
    let mut tx = fixture()["result"].clone();
    let mut signature = [1u8; 64];
    signature[..8].copy_from_slice(&n.to_le_bytes());
    tx["transaction"]["signatures"][0] = json!(bs58::encode(signature).into_string());
    if !related {
        for key in tx["transaction"]["message"]["accountKeys"]
            .as_array_mut()
            .unwrap()
        {
            if *key == json!(mint()) {
                *key = json!("So11111111111111111111111111111111111111112");
            }
        }
        tx["meta"]["preTokenBalances"] = json!([]);
        tx["meta"]["postTokenBalances"] = json!([]);
        tx["meta"]["err"] = json!({"InstructionError": [0, {"Custom": 1}]});
    }
    tx
}

/// Local write throughput for historical blocks: `cargo test --release --test backfill
/// historical_block_throughput -- --ignored --nocapture`. Prints, never asserts, rates
/// for blocks where every transaction transfers the mint and where one in twenty does.
#[test]
#[ignore = "measurement: run explicitly against local PostgreSQL"]
fn historical_block_throughput() {
    const SLOTS: u64 = 32;
    const PER_BLOCK: u64 = 1_000;
    for related_every in [1, 20] {
        let (_dir, path) = db();
        let start = fixture()["result"]["slot"].as_u64().unwrap();
        let blocks = (0..SLOTS)
            .map(|n| {
                let txs = (0..PER_BLOCK)
                    .map(|i| numbered(n * PER_BLOCK + i, i % related_every == 0))
                    .collect();
                (start + n, block(start + n, start + n - 1, txs))
            })
            .collect();
        let mut source = source(blocks);
        let began = std::time::Instant::now();
        let (coverage, metrics) = History::open(&path)
            .unwrap()
            .run(&mint(), start, start + SLOTS - 1, &mut source, || false)
            .unwrap();
        let seconds = began.elapsed().as_secs_f64();
        assert!(coverage.complete);
        assert_eq!(
            metrics.decoded_transactions + metrics.unrelated_transactions,
            SLOTS * PER_BLOCK
        );
        let stored: i64 = path
            .test_client()
            .query_one("SELECT pg_total_relation_size('historical_inputs')", &[])
            .unwrap()
            .get(0);
        println!(
            "historical_block_throughput: 1 in {related_every} related, {} tx in {seconds:.2} s = {:.0} tx/s; raw {} MiB fetched, {} MiB retained, historical_inputs {} MiB on disk",
            SLOTS * PER_BLOCK,
            (SLOTS * PER_BLOCK) as f64 / seconds,
            metrics.input_bytes >> 20,
            metrics.retained_bytes >> 20,
            stored >> 20
        );
    }
}

/// Eight consecutive blocks of 30 transactions, one in three transferring the mint.
fn parallel_blocks() -> (u64, Vec<(u64, Vec<u8>)>) {
    let start = fixture()["result"]["slot"].as_u64().unwrap();
    let blocks = (0..8)
        .map(|n| {
            let txs = (0..30).map(|i| numbered(n * 30 + i, i % 3 == 0)).collect();
            (start + n, block(start + n, start + n - 1, txs))
        })
        .collect();
    (start, blocks)
}

fn sorted_transfers(path: &Database) -> Vec<helius_indexer::model::Transfer> {
    let mut transfers = Store::query(path, None, 1000).unwrap();
    transfers.sort_by(|a, b| a.transfer_id.cmp(&b.transfer_id));
    transfers
}

fn parallel_history(path: &Database, workers: usize) -> History {
    let mut history = History::open(path).unwrap();
    history.set_workers(workers).unwrap();
    history
}

#[test]
fn parallel_workers_store_what_a_single_worker_stores() {
    let (start, blocks) = parallel_blocks();
    let (_single_dir, single) = db();
    let (coverage, one) = parallel_history(&single, 1)
        .run(
            &mint(),
            start,
            start + 7,
            &mut source(blocks.clone()),
            || false,
        )
        .unwrap();
    assert!(coverage.complete);
    let (_parallel_dir, parallel) = db();
    let (coverage, four) = parallel_history(&parallel, 4)
        .run(&mint(), start, start + 7, &mut source(blocks), || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(coverage.contiguous_through, Some(start + 7));
    assert_eq!(sorted_transfers(&parallel), sorted_transfers(&single));
    assert_eq!(sorted_transfers(&parallel).len(), 8 * 10 * 2);
    assert_eq!(four.decoded_transactions, one.decoded_transactions);
    assert_eq!(four.decoded_transfers, one.decoded_transfers);
    assert_eq!(four.unrelated_transactions, one.unrelated_transactions);
    assert_eq!(four.storage.writes, one.storage.writes);
}

#[test]
fn a_failed_slot_stops_the_checkpoint_while_later_slots_complete_out_of_order() {
    let (start, blocks) = parallel_blocks();
    let (_dir, path) = db();
    let mut missing = source(blocks.clone());
    missing.blocks.remove(&(start + 3));
    let (coverage, metrics) = parallel_history(&path, 4)
        .run(&mint(), start, start + 7, &mut missing, || false)
        .unwrap();
    assert!(!coverage.complete);
    assert_eq!(coverage.contiguous_through, Some(start + 2));
    assert_eq!(coverage.slots[3].status, "unknown");
    assert!(coverage.slots[4..].iter().all(|s| s.status == "complete"));
    assert_eq!(metrics.source_errors, 1);
    let (coverage, metrics) = parallel_history(&path, 4)
        .run(&mint(), start, start + 7, &mut source(blocks), || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(metrics.source_attempts, 1);
    assert_eq!(sorted_transfers(&path).len(), 8 * 10 * 2);
}

#[test]
fn parallel_cancellation_claims_nothing_unverified_and_a_rerun_completes() {
    let (start, blocks) = parallel_blocks();
    let (_dir, path) = db();
    let checks = Cell::new(0);
    let (coverage, metrics) = parallel_history(&path, 4)
        .run(
            &mint(),
            start,
            start + 7,
            &mut source(blocks.clone()),
            || {
                checks.set(checks.get() + 1);
                checks.get() > 3
            },
        )
        .unwrap();
    assert!(metrics.cancelled);
    assert!(!coverage.complete);
    let mut client = path.test_client();
    // Every acknowledged slot holds its whole block's output.
    for slot in coverage.slots.iter().filter(|s| s.status == "complete") {
        let transfers: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM transfers WHERE slot=$1",
                &[&(slot.slot as i64)],
            )
            .unwrap()
            .get(0);
        assert_eq!(transfers, 20, "slot {}", slot.slot);
    }
    let (coverage, _) = parallel_history(&path, 4)
        .run(&mint(), start, start + 7, &mut source(blocks), || false)
        .unwrap();
    assert!(coverage.complete);
    assert_eq!(sorted_transfers(&path).len(), 8 * 10 * 2);
}

#[test]
#[ignore = "subprocess helper for parallel process-exit recovery"]
fn parallel_process_exit_child() {
    let session = std::env::var("INDEXER_TEST_RECOVERY_DB").unwrap();
    let path = &Database::new(TestDatabase::url(), &session).unwrap();
    let (start, blocks) = parallel_blocks();
    let mut history = History::open(path).unwrap();
    history.set_workers(4).unwrap();
    history
        .run(&mint(), start, start + 7, &mut source(blocks), || {
            // Exit as soon as any output is durable, with other blocks still in flight.
            if !Store::query(path, None, 1).unwrap().is_empty() {
                std::process::exit(77);
            }
            false
        })
        .unwrap();
    std::process::exit(0);
}

#[test]
fn parallel_process_exit_never_acknowledges_a_partial_block_and_a_rerun_recovers() {
    let (_dir, path) = db();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "parallel_process_exit_child", "--ignored"])
        .env("INDEXER_TEST_RECOVERY_DB", path.session())
        .output()
        .unwrap()
        .status;
    assert_eq!(status.code(), Some(77));
    let (start, blocks) = parallel_blocks();
    let before = History::open(&path)
        .unwrap()
        .coverage(&mint(), start, start + 7)
        .unwrap();
    assert!(!before.complete);
    let mut client = path.test_client();
    for slot in before.slots.iter().filter(|s| s.status == "complete") {
        let transfers: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM transfers WHERE slot=$1",
                &[&(slot.slot as i64)],
            )
            .unwrap()
            .get(0);
        assert_eq!(transfers, 20, "slot {}", slot.slot);
    }
    let (coverage, _) = parallel_history(&path, 4)
        .run(
            &mint(),
            start,
            start + 7,
            &mut source(blocks.clone()),
            || false,
        )
        .unwrap();
    assert!(coverage.complete);
    let (_single_dir, single) = db();
    parallel_history(&single, 1)
        .run(&mint(), start, start + 7, &mut source(blocks), || false)
        .unwrap();
    assert_eq!(sorted_transfers(&path), sorted_transfers(&single));
}

#[test]
fn parallel_workers_record_a_skipped_slot_from_its_childs_parent_link() {
    let (start, mut blocks) = parallel_blocks();
    let skipped = start + 3;
    for (slot, raw) in &mut blocks {
        if *slot == skipped {
            *raw = serde_json::to_vec(
                &json!({"jsonrpc":"2.0","id":1,"error":{"code":-32007,"message":"Slot was skipped"}}),
            )
            .unwrap();
        } else if *slot == skipped + 1 {
            let mut value: Value = serde_json::from_slice(raw).unwrap();
            value["result"]["parentSlot"] = json!(skipped - 1);
            *raw = serde_json::to_vec(&value).unwrap();
        }
    }
    // Completion order is not controlled here; `finish` also keeps skip evidence when the
    // child happens to finish before the skipped slot's own source error.
    for _ in 0..3 {
        let (_dir, path) = db();
        let (coverage, _) = parallel_history(&path, 4)
            .run(
                &mint(),
                start,
                start + 7,
                &mut source(blocks.clone()),
                || false,
            )
            .unwrap();
        assert_eq!(coverage.slots[3].status, "skipped");
        assert!(coverage.complete);
    }
}
