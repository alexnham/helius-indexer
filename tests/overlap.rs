//! Captured RPC effects plus a synthetic protobuf view; no credentialed stream claims.
use helius_indexer::{
    Error, Result,
    backfill::{BlockSource, History},
    coordinator::WriteGate,
    decoder,
    live::{self, Intake},
    storage::Store,
};
use laserstream_core_proto::{geyser::*, solana::storage::confirmed_block as wire};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

const MINT: &str = live::USDC;
fn fixture() -> Value {
    serde_json::from_slice(include_bytes!("fixtures/mainnet-transfer.json")).unwrap()
}
fn persist(path: &Path, value: &Value) -> Result<usize> {
    let raw = serde_json::to_vec(value).unwrap();
    let capture = decoder::parse_capture(&raw)?;
    let decoded = decoder::decode(&capture);
    let count = Store::open(path)?.persist(&capture, &raw, &decoded)?;
    decoded?;
    Ok(count)
}
fn instruction(v: &Value) -> wire::CompiledInstruction {
    wire::CompiledInstruction {
        program_id_index: v["programIdIndex"].as_u64().unwrap() as u32,
        accounts: v["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_u64().unwrap() as u8)
            .collect(),
        data: bs58::decode(v["data"].as_str().unwrap())
            .into_vec()
            .unwrap(),
    }
}
fn update(value: &Value) -> SubscribeUpdateTransaction {
    let r = &value["result"];
    let m = &r["transaction"]["message"];
    let meta = &r["meta"];
    let decode = |v: &Value| bs58::decode(v.as_str().unwrap()).into_vec().unwrap();
    let balances = |field: &str| {
        meta[field]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| wire::TokenBalance {
                account_index: b["accountIndex"].as_u64().unwrap() as u32,
                mint: b["mint"].as_str().unwrap().into(),
                owner: b["owner"].as_str().unwrap_or("").into(),
                program_id: b["programId"].as_str().unwrap_or("").into(),
                ui_token_amount: Some(wire::UiTokenAmount {
                    decimals: b["uiTokenAmount"]["decimals"].as_u64().unwrap() as u32,
                    ..Default::default()
                }),
            })
            .collect()
    };
    SubscribeUpdateTransaction {
        slot: r["slot"].as_u64().unwrap(),
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature: decode(&r["transaction"]["signatures"][0]),
            transaction: Some(wire::Transaction {
                signatures: r["transaction"]["signatures"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(decode)
                    .collect(),
                message: Some(wire::Message {
                    account_keys: m["accountKeys"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(decode)
                        .collect(),
                    instructions: m["instructions"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(instruction)
                        .collect(),
                    versioned: r["version"] != "legacy",
                    config: if r["version"] == 1 {
                        Some(wire::TransactionConfig::default())
                    } else {
                        None
                    },
                    ..Default::default()
                }),
            }),
            meta: Some(wire::TransactionStatusMeta {
                inner_instructions: meta["innerInstructions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|g| wire::InnerInstructions {
                        index: g["index"].as_u64().unwrap() as u32,
                        instructions: g["instructions"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|i| {
                                let x = instruction(i);
                                wire::InnerInstruction {
                                    program_id_index: x.program_id_index,
                                    accounts: x.accounts,
                                    data: x.data,
                                    stack_height: None,
                                }
                            })
                            .collect(),
                    })
                    .collect(),
                log_messages: meta["logMessages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().into())
                    .collect(),
                pre_token_balances: balances("preTokenBalances"),
                post_token_balances: balances("postTokenBalances"),
                loaded_writable_addresses: meta["loadedAddresses"]["writable"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(decode)
                    .collect(),
                loaded_readonly_addresses: meta["loadedAddresses"]["readonly"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(decode)
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    }
}
struct Source(Vec<u8>);
impl BlockSource for Source {
    fn block(&mut self, _: u64) -> Result<Vec<u8>> {
        Ok(self.0.clone())
    }
}
fn block(value: &Value, parent: u64, empty: bool) -> Source {
    let mut tx = value["result"].clone();
    tx.as_object_mut().unwrap().remove("slot");
    tx.as_object_mut().unwrap().remove("blockTime");
    Source(serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{
        "blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([6;32]).into_string(),"parentSlot":parent,
        "blockTime":value["result"]["blockTime"],"transactions":if empty{vec![]}else{vec![tx]}}})).unwrap())
}
#[test]
fn real_store_coalesces_both_source_orders_and_retries_before_queries() {
    for live_first in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.sqlite");
        let value = fixture();
        let tx = update(&value);
        let slot = tx.slot;
        let mut history = History::open(&path).unwrap();
        let mut intake = Intake::open(&path).unwrap();
        let run_history = |history: &mut History| {
            history
                .run(
                    MINT,
                    slot,
                    slot,
                    &mut block(&value, slot - 1, false),
                    || false,
                )
                .unwrap()
        };
        if live_first {
            let id = intake.retain(&tx).unwrap();
            assert_eq!(intake.process(id, &tx).unwrap(), 2);
        } else {
            assert!(run_history(&mut history).0.complete);
        }
        assert!(run_history(&mut history).0.complete);
        for _ in 0..3 {
            let id = intake.retain(&tx).unwrap();
            assert_eq!(intake.process(id, &tx).unwrap(), 0);
        }
        assert!(run_history(&mut history).0.complete);
        let expected = decoder::decode(
            &decoder::parse_capture(include_bytes!("fixtures/mainnet-transfer.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(Store::query(&path, Some(MINT), 1000).unwrap(), expected);
        let summary = Store::summary(&path, MINT).unwrap();
        assert_eq!(summary["transfer_count"], 2);
        assert_eq!(summary["known_volume_raw"], "46720000");
        assert_eq!(summary["known_fees_raw"], "0");
        let conn = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM capture_inputs", [], |r| r
                .get::<_, u32>(0))
                .unwrap(),
            2
        );
        assert!(Intake::report(&path).unwrap()["verified_boundary"].is_null());
    }
}
#[test]
fn conflicts_are_quarantined_and_cannot_change_existing_effects() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.sqlite");
    let mut value = fixture();
    persist(&path, &value).unwrap();
    let expected = Store::query(&path, None, 1000).unwrap();
    value["result"]["blockTime"] = json!(value["result"]["blockTime"].as_i64().unwrap() + 1);
    assert_eq!(persist(&path, &value).unwrap_err().0, "capture_conflict");
    value = fixture();
    let group = value["result"]["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|g| g["index"] == 2)
        .unwrap();
    let mut data = bs58::decode(group["instructions"][1]["data"].as_str().unwrap())
        .into_vec()
        .unwrap();
    data[1..9].copy_from_slice(&23360001u64.to_le_bytes());
    group["instructions"][1]["data"] = json!(bs58::encode(data).into_string());
    assert_eq!(persist(&path, &value).unwrap_err().0, "capture_conflict");
    assert_eq!(Store::query(&path, None, 1000).unwrap(), expected);
    let conn = rusqlite::Connection::open(path).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM capture_inputs WHERE status='capture_conflict'",
            [],
            |r| r.get::<_, u32>(0)
        )
        .unwrap(),
        2
    );
}
#[test]
fn metadata_enrichment_repairs_same_revision_failures_but_not_decoder_corrections() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.sqlite");
    let mut incomplete = fixture();
    incomplete["source"] = json!("helius-laserstream");
    incomplete["result"]["blockTime"] = Value::Null;
    incomplete["result"]["meta"]["innerInstructions"] = Value::Null;
    assert_eq!(
        persist(&path, &incomplete).unwrap_err().0,
        "missing_inner_instructions"
    );
    assert_eq!(persist(&path, &fixture()).unwrap(), 2);
    assert_eq!(Store::summary(&path, MINT).unwrap()["transfer_count"], 2);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute("UPDATE captures SET decoder_version='previous-decoder'", [])
        .unwrap();
    assert_eq!(
        persist(&path, &fixture()).unwrap_err().0,
        "decoder_revision_conflict"
    );
    assert_eq!(Store::summary(&path, MINT).unwrap()["transfer_count"], 2);
}
#[test]
fn legacy_raw_is_preserved_and_u128_summary_is_exact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.sqlite");
    let value = fixture();
    persist(&path, &value).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute("DROP TABLE capture_inputs", [])
        .unwrap();
    let normalized: Value =
        serde_json::from_slice(&live::normalize(&update(&value)).unwrap()).unwrap();
    assert_eq!(persist(&path, &normalized).unwrap(), 0);
    assert_eq!(
        rusqlite::Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM capture_inputs WHERE source='legacy'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
        1
    );
    let path = dir.path().join("large.sqlite");
    let mut large = fixture();
    for group in large["result"]["meta"]["innerInstructions"]
        .as_array_mut()
        .unwrap()
    {
        for ix in group["instructions"].as_array_mut().unwrap() {
            let mut bytes = bs58::decode(ix["data"].as_str().unwrap())
                .into_vec()
                .unwrap();
            if bytes.len() == 9 && bytes[0] == 3 {
                bytes[1..9].copy_from_slice(&u64::MAX.to_le_bytes());
                ix["data"] = json!(bs58::encode(bytes).into_string());
            }
        }
    }
    persist(&path, &large).unwrap();
    assert_eq!(
        Store::summary(&path, MINT).unwrap()["known_volume_raw"],
        (u128::from(u64::MAX) * 2).to_string()
    );
}
#[test]
fn later_finished_range_cannot_advance_a_missing_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.sqlite");
    let value = fixture();
    let slot = value["result"]["slot"].as_u64().unwrap();
    let mut history = History::open(&path).unwrap();
    assert!(
        history
            .run(
                MINT,
                slot + 1,
                slot + 1,
                &mut block(&value, slot, true),
                || false
            )
            .unwrap()
            .0
            .complete
    );
    assert_eq!(
        history
            .coverage(MINT, slot, slot + 1)
            .unwrap()
            .contiguous_through,
        None
    );
    history
        .run(
            MINT,
            slot,
            slot,
            &mut block(&value, slot - 1, false),
            || false,
        )
        .unwrap();
    assert_eq!(
        history
            .coverage(MINT, slot, slot + 1)
            .unwrap()
            .contiguous_through,
        Some(slot + 1)
    );
}
#[test]
fn downstream_slowdown_bounds_waiters_and_prioritizes_ready_live_work() {
    let gate = Arc::new(WriteGate::default());
    let held = gate.historical().unwrap();
    let (sender, receiver) = mpsc::channel();
    let g = Arc::clone(&gate);
    let s = sender.clone();
    let historical = std::thread::spawn(move || {
        let _permit = g.historical().unwrap();
        s.send("history").unwrap();
    });
    let g = Arc::clone(&gate);
    let live = std::thread::spawn(move || {
        let _permit = g.live().unwrap();
        sender.send("live").unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut ready = false;
    while Instant::now() < deadline {
        let snapshot = gate.snapshot().unwrap();
        if snapshot["live_waiting"] == 1 && snapshot["historical_waiting"] == 1 {
            ready = true;
            assert!(snapshot["oldest_pending_age_seconds"].as_f64().unwrap() > 0.0);
            break;
        }
        std::thread::yield_now();
    }
    drop(held);
    assert!(ready);
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
        "live"
    );
    assert_eq!(
        receiver.recv_timeout(Duration::from_secs(5)).unwrap(),
        "history"
    );
    live.join().unwrap();
    historical.join().unwrap();
    let snapshot = gate.snapshot().unwrap();
    assert_eq!(snapshot["live_waiting"], 0);
    assert_eq!(snapshot["historical_waiting"], 0);
    assert_eq!(snapshot["live_admissions"], 1);
    assert_eq!(snapshot["historical_admissions"], 2);
}
#[test]
fn coordinator_rejects_invalid_bounds_without_network() {
    let mut telemetry = helius_indexer::telemetry::Telemetry::default();
    assert_eq!(
        helius_indexer::coordinator::run(Path::new("unused"), 0, 0, 0, &mut telemetry).unwrap_err(),
        Error("invalid_live_bounds")
    );
}

#[test]
fn later_record_conflict_rolls_back_every_metadata_enrichment() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.sqlite");
    let value = fixture();
    let normalized: Value =
        serde_json::from_slice(&live::normalize(&update(&value)).unwrap()).unwrap();
    persist(&path, &normalized).unwrap();
    let before = Store::query(&path, None, 1000).unwrap();
    assert!(before.iter().all(|t| t.block_time.is_none()));
    let mut conflicting = value;
    for name in ["preTokenBalances", "postTokenBalances"] {
        for balance in conflicting["result"]["meta"][name].as_array_mut().unwrap() {
            if balance["accountIndex"] == 3 {
                balance["owner"] = json!(bs58::encode([33; 32]).into_string());
            }
        }
    }
    assert_eq!(
        persist(&path, &conflicting).unwrap_err().0,
        "capture_conflict"
    );
    assert_eq!(Store::query(&path, None, 1000).unwrap(), before);
}

#[test]
fn a_sparse_observation_cannot_erase_known_identity_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.sqlite");
    let value = fixture();
    persist(&path, &value).unwrap();
    let normalized: Value =
        serde_json::from_slice(&live::normalize(&update(&value)).unwrap()).unwrap();
    persist(&path, &normalized).unwrap();
    let mut conflicting = value;
    conflicting["result"]["transaction"]["message"]["recentBlockhash"] =
        json!(bs58::encode([33; 32]).into_string());
    assert_eq!(
        persist(&path, &conflicting).unwrap_err().0,
        "capture_conflict"
    );
    assert_eq!(Store::summary(&path, MINT).unwrap()["transfer_count"], 2);
}

#[test]
fn concurrent_source_cores_share_real_storage_without_losing_either_ack() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("index.sqlite");
    let value = fixture();
    let tx = update(&value);
    let slot = tx.slot;
    let gate = Arc::new(WriteGate::default());
    let mut intake = Intake::open(&path).unwrap();
    let mut history = History::open(&path).unwrap();
    history.set_gate(Arc::clone(&gate));
    let source = block(&value, slot - 1, false);
    let live_gate = Arc::clone(&gate);
    let (live_result, history_result) = std::thread::scope(|workers| {
        let a = workers.spawn(move || {
            for _ in 0..5 {
                let _permit = live_gate.live()?;
                let id = intake.retain(&tx)?;
                intake.process(id, &tx)?;
            }
            Ok::<_, Error>(())
        });
        let b = workers.spawn(move || history.run(MINT, slot, slot, &mut { source }, || false));
        (a.join().unwrap(), b.join().unwrap())
    });
    live_result.unwrap();
    assert!(history_result.unwrap().0.complete);
    assert_eq!(Store::summary(&path, MINT).unwrap()["transfer_count"], 2);
    assert_eq!(
        Store::summary(&path, MINT).unwrap()["known_volume_raw"],
        "46720000"
    );
    assert_eq!(Intake::report(&path).unwrap()["unresolved_items"], 0);
    assert!(
        gate.snapshot().unwrap()["live_admissions"]
            .as_u64()
            .unwrap()
            >= 5
    );
}

#[test]
fn mixed_token_transactions_have_the_same_complete_record_set_from_both_sources() {
    let value: Value =
        serde_json::from_slice(include_bytes!("fixtures/mainnet-mixed-transfer.json")).unwrap();
    let decoded =
        decoder::decode(&decoder::parse_capture(&serde_json::to_vec(&value).unwrap()).unwrap())
            .unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded.iter().filter(|t| t.mint == MINT).count(), 1);
    for live_first in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.sqlite");
        let tx = update(&value);
        let slot = tx.slot;
        let mut intake = Intake::open(&path).unwrap();
        let mut history = History::open(&path).unwrap();
        if live_first {
            let id = intake.retain(&tx).unwrap();
            assert_eq!(intake.process(id, &tx).unwrap(), 2);
        }
        assert!(
            history
                .run(
                    MINT,
                    slot,
                    slot,
                    &mut block(&value, slot - 1, false),
                    || false
                )
                .unwrap()
                .0
                .complete
        );
        let id = intake.retain(&tx).unwrap();
        assert_eq!(intake.process(id, &tx).unwrap(), 0);
        assert_eq!(Store::query(&path, None, 1000).unwrap(), decoded);
        assert_eq!(Store::summary(&path, MINT).unwrap()["transfer_count"], 1);
    }
}

/// Local captured-input replay; explicitly opted in, never opens a network or reads API credentials.
#[test]
#[ignore = "requires locally retained validation database and a fresh destination"]
fn replay_retained_network_observations() {
    use prost::Message;
    use std::collections::BTreeMap;
    let source = std::env::var("INDEXER_VALIDATION_SOURCE").expect("set source database path");
    let destination =
        std::env::var("INDEXER_VALIDATION_DESTINATION").expect("set fresh destination path");
    let path = Path::new(&destination);
    assert!(!path.exists(), "destination must be new");
    let input =
        rusqlite::Connection::open_with_flags(source, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let mut intake = Intake::open(path).unwrap();
    let mut attempts = 0;
    let mut failures = 0;
    let mut statement = input
        .prepare("SELECT protobuf,proto_schema FROM live_raw ORDER BY id LIMIT 1001")
        .unwrap();
    for row in statement
        .query_map([], |r| {
            Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, String>(1)?))
        })
        .unwrap()
    {
        let (bytes, schema) = row.unwrap();
        attempts += 1;
        assert!(attempts <= 1000);
        assert_eq!(schema, "laserstream-core-proto/14.0.0");
        let tx = SubscribeUpdateTransaction::decode(bytes.as_slice()).unwrap();
        let id = intake.retain(&tx).unwrap();
        failures += usize::from(intake.process(id, &tx).is_err());
    }
    let mut blocks = BTreeMap::new();
    let mut bytes = 0;
    let mut statement=input.prepare("SELECT slot,response FROM historical_inputs WHERE mint=?1 ORDER BY slot,attempt LIMIT 33").unwrap();
    for row in statement
        .query_map([MINT], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?))
        })
        .unwrap()
    {
        let (slot, data) = row.unwrap();
        let slot = u64::try_from(slot).expect("nonnegative slot");
        bytes += data.len();
        assert!(bytes <= 64 * 1024 * 1024);
        blocks.insert(slot, data);
        assert!(blocks.len() <= 16);
    }
    let start = *blocks.keys().next().expect("captured block required");
    let end = *blocks.keys().next_back().unwrap();
    struct Retained(BTreeMap<u64, Vec<u8>>);
    impl BlockSource for Retained {
        fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
            self.0
                .get(&slot)
                .cloned()
                .ok_or(Error("validation_block_missing"))
        }
    }
    let mut history = History::open(path).unwrap();
    let (coverage, metrics) = history
        .run(MINT, start, end, &mut Retained(blocks), || false)
        .unwrap();
    let output = rusqlite::Connection::open(path).unwrap();
    let conflicts:i64=output.query_row("SELECT COUNT(*) FROM capture_inputs WHERE status IN ('capture_conflict','decoder_revision_conflict')",[],|r|r.get(0)).unwrap();
    let shared:i64=output.query_row("SELECT COUNT(*) FROM (SELECT signature FROM capture_inputs WHERE status='decoded' GROUP BY signature HAVING COUNT(DISTINCT source)>=2)",[],|r|r.get(0)).unwrap();
    assert_eq!(conflicts, 0);
    assert!(shared > 0);
    let before = Store::summary(path, MINT).unwrap();
    // Repeat captured successful inputs: immediate logical counts and volume stay unchanged.
    for row in input
        .prepare("SELECT capture_json FROM capture_inputs WHERE status='decoded'")
        .unwrap()
        .query_map([], |r| r.get::<_, Vec<u8>>(0))
        .unwrap()
    {
        let value: Value = serde_json::from_slice(&row.unwrap()).unwrap();
        assert_eq!(persist(path, &value).unwrap(), 0);
    }
    assert_eq!(Store::summary(path, MINT).unwrap(), before);
    println!(
        "{}",
        json!({"live_attempts":attempts,"live_decode_failures":failures,"shared_decoded_transactions":shared,"quarantined_conflicts":conflicts,"historical_complete":coverage.complete,"historical_decode_failures":metrics.decode_failures,"summary":before})
    );
}
