//! Captured RPC effects plus a synthetic protobuf view; no credentialed stream claims.
use helius_indexer::{
    Error, Result,
    backfill::{BlockSource, History},
    coordinator::WriteGate,
    decoder,
    live::{self, Intake},
    storage::{Database, Store, pg::TestDatabase},
};
use laserstream_core_proto::{geyser::*, solana::storage::confirmed_block as wire};
use serde_json::{Value, json};
use std::{
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

const MINT: &str = live::USDC;
fn fixture() -> Value {
    serde_json::from_slice(include_bytes!("fixtures/mainnet-transfer.json")).unwrap()
}
fn persist(path: &Database, value: &Value) -> Result<usize> {
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
        "blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([7;32]).into_string(),"parentSlot":parent,
        "blockTime":value["result"]["blockTime"],"transactions":if empty{vec![]}else{vec![tx]}}})).unwrap())
}
#[test]
fn real_store_coalesces_both_source_orders_and_retries_before_queries() {
    for live_first in [true, false] {
        let path = TestDatabase::new();
        let path = &path.0;
        let value = fixture();
        let tx = update(&value);
        let slot = tx.slot;
        let mut history = History::open(path).unwrap();
        let mut intake = Intake::open(path).unwrap();
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
        assert_eq!(Store::query(path, Some(MINT), 1000).unwrap(), expected);
        let summary = Store::summary(path, MINT).unwrap();
        assert_eq!(summary["transfer_count"], 2);
        assert_eq!(summary["known_volume_raw"], "46720000");
        assert_eq!(summary["known_fees_raw"], "0");
        let mut conn = path.test_client();
        assert_eq!(
            conn.query_one("SELECT COUNT(*) FROM capture_inputs", &[])
                .unwrap()
                .get::<_, i64>(0),
            2
        );
        assert!(Intake::report(path).unwrap()["verified_boundary"].is_null());
    }
}
#[test]
fn conflicts_are_quarantined_and_cannot_change_existing_effects() {
    let path = TestDatabase::new();
    let path = &path.0;
    let mut value = fixture();
    persist(path, &value).unwrap();
    let expected = Store::query(path, None, 1000).unwrap();
    value["result"]["blockTime"] = json!(value["result"]["blockTime"].as_i64().unwrap() + 1);
    assert_eq!(persist(path, &value).unwrap_err().0, "capture_conflict");
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
    assert_eq!(persist(path, &value).unwrap_err().0, "capture_conflict");
    assert_eq!(Store::query(path, None, 1000).unwrap(), expected);
    let mut conn = path.test_client();
    assert_eq!(
        conn.query_one(
            "SELECT COUNT(*) FROM capture_inputs WHERE status='capture_conflict'",
            &[]
        )
        .unwrap()
        .get::<_, i64>(0),
        2
    );
}
#[test]
fn metadata_enrichment_repairs_same_revision_failures_but_not_decoder_corrections() {
    let path = TestDatabase::new();
    let path = &path.0;
    let mut incomplete = fixture();
    incomplete["source"] = json!("helius-laserstream");
    incomplete["result"]["blockTime"] = Value::Null;
    incomplete["result"]["meta"]["innerInstructions"] = Value::Null;
    assert_eq!(
        persist(path, &incomplete).unwrap_err().0,
        "missing_inner_instructions"
    );
    assert_eq!(persist(path, &fixture()).unwrap(), 2);
    assert_eq!(Store::summary(path, MINT).unwrap()["transfer_count"], 2);
    let mut conn = path.test_client();
    conn.execute(
        "UPDATE captures SET decoder_version='previous-decoder'",
        &[],
    )
    .unwrap();
    assert_eq!(
        persist(path, &fixture()).unwrap_err().0,
        "decoder_revision_conflict"
    );
    assert_eq!(Store::summary(path, MINT).unwrap()["transfer_count"], 2);
}
#[test]
fn unresolved_observations_retain_evidence_through_sparse_repair() {
    for accumulate in [false, true] {
        let path = TestDatabase::new();
        let path = &path.0;
        let known = fixture();
        let mut pending = known.clone();
        pending["result"]["meta"]["innerInstructions"] = Value::Null;
        if accumulate {
            let mut sparse = pending.clone();
            sparse["result"]["blockTime"] = Value::Null;
            sparse["result"]["transaction"]["message"]
                .as_object_mut()
                .unwrap()
                .remove("recentBlockhash");
            assert!(persist(path, &sparse).is_err());
        }
        assert!(persist(path, &pending).is_err());
        let mut contradictory_pending = pending.clone();
        contradictory_pending["result"]["blockTime"] = json!(1);
        assert_eq!(
            persist(path, &contradictory_pending).unwrap_err().0,
            "capture_conflict"
        );
        let mut repaired = known.clone();
        repaired["result"]["blockTime"] = Value::Null;
        repaired["result"]["transaction"]["message"]
            .as_object_mut()
            .unwrap()
            .remove("recentBlockhash");
        assert_eq!(persist(path, &repaired).unwrap(), 2);
        let mut conn = path.test_client();
        let record: String = conn
            .query_one("SELECT record_json FROM transfers LIMIT 1", &[])
            .unwrap()
            .get(0);
        let record: Value = serde_json::from_str(&record).unwrap();
        assert_eq!(record["block_time"], known["result"]["blockTime"]);
        let mut conflict = known.clone();
        conflict["result"]["transaction"]["message"]["recentBlockhash"] = json!("contradictory");
        assert_eq!(persist(path, &conflict).unwrap_err().0, "capture_conflict");
    }
}
#[test]
fn complementary_decodable_owner_evidence_enriches_existing_rows() {
    let path = TestDatabase::new();
    let path = &path.0;
    let mut first = fixture();
    let mut second = fixture();
    for balance in first["result"]["meta"]["postTokenBalances"]
        .as_array_mut()
        .unwrap()
    {
        balance.as_object_mut().unwrap().remove("owner");
    }
    for balance in second["result"]["meta"]["preTokenBalances"]
        .as_array_mut()
        .unwrap()
    {
        balance.as_object_mut().unwrap().remove("owner");
    }
    assert_eq!(persist(path, &first).unwrap(), 2);
    assert_eq!(persist(path, &second).unwrap(), 0);
    let expected =
        decoder::decode(&decoder::parse_capture(&serde_json::to_vec(&fixture()).unwrap()).unwrap())
            .unwrap();
    let mut conn = path.test_client();
    for transfer in expected {
        let actual: String = conn
            .query_one(
                "SELECT record_json FROM transfers WHERE transfer_id=$1",
                &[&transfer.transfer_id],
            )
            .unwrap()
            .get(0);
        assert_eq!(
            serde_json::from_str::<Value>(&actual).unwrap(),
            serde_json::to_value(transfer).unwrap()
        );
    }
}
#[test]
fn u128_summary_is_exact_at_u64_max() {
    let path = TestDatabase::new();
    let path = &path.0;
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
    persist(path, &large).unwrap();
    assert_eq!(
        Store::summary(path, MINT).unwrap()["known_volume_raw"],
        (u128::from(u64::MAX) * 2).to_string()
    );
}
#[test]
fn later_finished_range_cannot_advance_a_missing_prefix() {
    let path = TestDatabase::new();
    let path = &path.0;
    let value = fixture();
    let slot = value["result"]["slot"].as_u64().unwrap();
    let mut history = History::open(path).unwrap();
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
        helius_indexer::coordinator::run(
            &Database::new("postgres://unused@127.0.0.1/x", "unused").unwrap(),
            0,
            0,
            0,
            &mut telemetry
        )
        .unwrap_err(),
        Error("invalid_live_bounds")
    );
}

#[test]
fn later_record_conflict_rolls_back_every_metadata_enrichment() {
    let path = TestDatabase::new();
    let path = &path.0;
    let value = fixture();
    let normalized: Value =
        serde_json::from_slice(&live::normalize(&update(&value)).unwrap()).unwrap();
    persist(path, &normalized).unwrap();
    let before = Store::query(path, None, 1000).unwrap();
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
        persist(path, &conflicting).unwrap_err().0,
        "capture_conflict"
    );
    assert_eq!(Store::query(path, None, 1000).unwrap(), before);
}

#[test]
fn a_sparse_observation_cannot_erase_known_identity_evidence() {
    let path = TestDatabase::new();
    let path = &path.0;
    let value = fixture();
    persist(path, &value).unwrap();
    let normalized: Value =
        serde_json::from_slice(&live::normalize(&update(&value)).unwrap()).unwrap();
    persist(path, &normalized).unwrap();
    let mut conflicting = value;
    conflicting["result"]["transaction"]["message"]["recentBlockhash"] =
        json!(bs58::encode([33; 32]).into_string());
    assert_eq!(
        persist(path, &conflicting).unwrap_err().0,
        "capture_conflict"
    );
    assert_eq!(Store::summary(path, MINT).unwrap()["transfer_count"], 2);
}

#[test]
fn concurrent_source_cores_share_real_storage_without_losing_either_ack() {
    let path = TestDatabase::new();
    let path = &path.0;
    let value = fixture();
    let tx = update(&value);
    let slot = tx.slot;
    let gate = Arc::new(WriteGate::default());
    let mut intake = Intake::open(path).unwrap();
    let mut history = History::open(path).unwrap();
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
    assert_eq!(Store::summary(path, MINT).unwrap()["transfer_count"], 2);
    assert_eq!(
        Store::summary(path, MINT).unwrap()["known_volume_raw"],
        "46720000"
    );
    assert_eq!(Intake::report(path).unwrap()["unresolved_items"], 0);
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
        let path = TestDatabase::new();
        let path = &path.0;
        let tx = update(&value);
        let slot = tx.slot;
        let mut intake = Intake::open(path).unwrap();
        let mut history = History::open(path).unwrap();
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
        assert_eq!(Store::query(path, None, 1000).unwrap(), decoded);
        assert_eq!(Store::summary(path, MINT).unwrap()["transfer_count"], 1);
    }
}
