use super::*;
use crate::{decoder, storage::Store};
use jetstreamer_firehose::{
    dataframe::DataFrame, node::NodesWithCids, transaction::Transaction, utils::Buffer,
};
use laserstream_core_proto::solana::storage::confirmed_block as wire;
use prost::Message;
use serde_cbor::Value as C;
use sha2::{Digest, Sha256};
use std::fs;

fn fixture() -> Value {
    serde_json::from_slice(include_bytes!("../../tests/fixtures/mainnet-transfer.json")).unwrap()
}
fn b58(value: &Value) -> Vec<u8> {
    bs58::decode(value.as_str().unwrap()).into_vec().unwrap()
}
fn short(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let b = (n & 127) as u8;
        n >>= 7;
        out.push(if n > 0 { b | 128 } else { b });
        if n == 0 {
            break;
        }
    }
}
fn wire_bytes(value: &Value) -> Vec<u8> {
    let tx = &value["result"]["transaction"];
    let message = &tx["message"];
    let mut bytes = Vec::new();
    short(tx["signatures"].as_array().unwrap().len(), &mut bytes);
    for sig in tx["signatures"].as_array().unwrap() {
        bytes.extend(b58(sig));
    }
    for field in [
        "numRequiredSignatures",
        "numReadonlySignedAccounts",
        "numReadonlyUnsignedAccounts",
    ] {
        bytes.push(message["header"][field].as_u64().unwrap() as u8);
    }
    short(message["accountKeys"].as_array().unwrap().len(), &mut bytes);
    for key in message["accountKeys"].as_array().unwrap() {
        bytes.extend(b58(key));
    }
    bytes.extend(b58(&message["recentBlockhash"]));
    short(
        message["instructions"].as_array().unwrap().len(),
        &mut bytes,
    );
    for ix in message["instructions"].as_array().unwrap() {
        bytes.push(ix["programIdIndex"].as_u64().unwrap() as u8);
        let a = ix["accounts"].as_array().unwrap();
        short(a.len(), &mut bytes);
        bytes.extend(a.iter().map(|v| v.as_u64().unwrap() as u8));
        let data = b58(&ix["data"]);
        short(data.len(), &mut bytes);
        bytes.extend(data);
    }
    bytes
}
fn metadata(value: &Value) -> wire::TransactionStatusMeta {
    let meta = &value["result"]["meta"];
    let balances = |key: &str| {
        meta[key]
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
                    amount: b["uiTokenAmount"]["amount"].as_str().unwrap_or("0").into(),
                    ..Default::default()
                }),
            })
            .collect()
    };
    wire::TransactionStatusMeta {
        pre_balances: meta["preBalances"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b.as_u64().unwrap())
            .collect(),
        post_balances: meta["postBalances"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| b.as_u64().unwrap())
            .collect(),
        log_messages: meta["logMessages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| l.as_str().unwrap().into())
            .collect(),
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
                    .map(|i| wire::InnerInstruction {
                        program_id_index: i["programIdIndex"].as_u64().unwrap() as u32,
                        accounts: i["accounts"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|a| a.as_u64().unwrap() as u8)
                            .collect(),
                        data: b58(&i["data"]),
                        stack_height: i["stackHeight"].as_u64().map(|n| n as u32),
                    })
                    .collect(),
            })
            .collect(),
        pre_token_balances: balances("preTokenBalances"),
        post_token_balances: balances("postTokenBalances"),
        ..Default::default()
    }
}
fn transaction(value: &Value) -> Transaction {
    let frame = |data| DataFrame {
        kind: 6,
        hash: None,
        index: None,
        total: None,
        data: Buffer::from_vec(data),
        next: None,
    };
    Transaction {
        kind: 0,
        slot: value["result"]["slot"].as_u64().unwrap(),
        index: Some(0),
        data: frame(wire_bytes(value)),
        metadata: frame(metadata(value).encode_to_vec()),
    }
}
fn cbor_frame(data: Vec<u8>) -> C {
    C::Array(vec![
        C::Integer(6),
        C::Null,
        C::Null,
        C::Null,
        C::Bytes(data),
    ])
}
fn node(value: C, raw: &mut Vec<u8>) -> Vec<u8> {
    let data = serde_cbor::to_vec(&value).unwrap();
    let mut cid = vec![1, 0x71, 0x12, 32];
    cid.extend(Sha256::digest(&data));
    short(cid.len() + data.len(), raw);
    raw.extend(&cid);
    raw.extend(data);
    let mut link = vec![0];
    link.extend(cid);
    link
}
// The CAR containers/entry hashes are synthetic; the legacy transaction and runtime metadata are captured.
fn slot_range(
    value: &Value,
    slot: u64,
    parent: u64,
    hash: u8,
    include_transaction: bool,
) -> Vec<u8> {
    let mut raw = Vec::new();
    let reward = node(C::Array(vec![C::Integer(5), cbor_frame(vec![])]), &mut raw);
    let transactions = if include_transaction {
        let tx = transaction(value);
        vec![C::Bytes(node(
            C::Array(vec![
                C::Integer(0),
                cbor_frame(tx.data.data.as_slice().to_vec()),
                cbor_frame(tx.metadata.data.as_slice().to_vec()),
                C::Integer(slot as i128),
                C::Integer(0),
            ]),
            &mut raw,
        ))]
    } else {
        vec![]
    };
    let entry = node(
        C::Array(vec![
            C::Integer(1),
            C::Integer(1),
            C::Bytes(vec![hash; 32]),
            C::Array(transactions),
        ]),
        &mut raw,
    );
    node(
        C::Array(vec![
            C::Integer(2),
            C::Integer(slot as i128),
            C::Array(vec![]),
            C::Array(vec![C::Bytes(entry)]),
            C::Array(vec![
                C::Integer(parent as i128),
                C::Integer(value["result"]["blockTime"].as_i64().unwrap() as i128),
            ]),
            C::Bytes(reward),
        ]),
        &mut raw,
    );
    raw
}
fn put_cache(dir: &Path, slot: u64, raw: &[u8]) {
    let epoch = slot / 432000;
    let stem = format!("epoch-{epoch}-slot-{slot}");
    let mut record = 100u64.to_le_bytes().to_vec();
    record.extend((raw.len() as u32).to_le_bytes());
    fs::write(dir.join(format!("{stem}.car-range")), raw).unwrap();
    fs::write(dir.join(format!("{stem}.json")),serde_json::to_vec(&json!({"schema_version":1,"jetstreamer_revision":JETSTREAMER_REVISION,"epoch":epoch,"slot":slot,"offset":100,"length":raw.len(),"index_record":record,"sha256":format!("{:x}",Sha256::digest(raw))})).unwrap()).unwrap();
}

#[test]
fn maintained_wire_reader_normalizes_captured_legacy_and_synthetic_v0_v1() {
    let value = fixture();
    let tx = transaction(&value);
    let nodes = NodesWithCids::new();
    let normalized =
        normalize_transaction(&tx, &nodes, value["result"]["blockTime"].as_i64()).unwrap();
    let expected =
        decoder::decode(&decoder::parse_capture(&serde_json::to_vec(&value).unwrap()).unwrap())
            .unwrap();
    let actual = decoder::decode(
        &decoder::parse_capture(&serde_json::to_vec(&normalized).unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(actual, expected);
    let original = tx.as_parsed().unwrap();
    let solana_message::VersionedMessage::Legacy(m) = original.message.clone() else {
        panic!()
    };
    for message in [
        solana_message::VersionedMessage::V0(solana_message::v0::Message {
            header: m.header,
            account_keys: m.account_keys.clone(),
            recent_blockhash: m.recent_blockhash,
            instructions: m.instructions.clone(),
            address_table_lookups: vec![],
        }),
        solana_message::VersionedMessage::V1(solana_message::v1::Message {
            header: m.header,
            account_keys: m.account_keys.clone(),
            lifetime_specifier: m.recent_blockhash,
            instructions: m.instructions.clone(),
            config: Default::default(),
        }),
    ] {
        let mut native = original.clone();
        native.message = message;
        let mut variant = tx.clone();
        variant.data.data = Buffer::from_vec(wincode::serialize(&native).unwrap());
        let normalized =
            normalize_transaction(&variant, &nodes, value["result"]["blockTime"].as_i64()).unwrap();
        assert!(normalized["result"]["version"] == 0 || normalized["result"]["version"] == 1);
        let actual = decoder::decode(
            &decoder::parse_capture(&serde_json::to_vec(&normalized).unwrap()).unwrap(),
        )
        .unwrap();
        assert_eq!(actual, expected);
    }
}

#[test]
fn cached_car_source_replays_through_real_storage_and_both_source_orders() {
    let value = fixture();
    let slot = value["result"]["slot"].as_u64().unwrap();
    for reverse in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = crate::storage::pg::TestDatabase::new();
        let path = &path.0;
        let cache = dir.path().join("archive");
        fs::create_dir(&cache).unwrap();
        put_cache(&cache, slot, &slot_range(&value, slot, slot - 1, 7, true));
        put_cache(
            &cache,
            slot - 1,
            &slot_range(&value, slot - 1, slot - 2, 6, false),
        );
        let mut source = ArchiveSource::new(&cache, false, Limits::default()).unwrap();
        let archive_raw = source.block(slot).unwrap();
        assert_eq!(source.stats.requests, 0);
        assert_eq!(source.stats.cached_chunks, 2);
        let rpc_raw=serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":{"blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([6;32]).into_string(),"parentSlot":slot-1,"blockTime":value["result"]["blockTime"],"transactions":[value["result"]]}})).unwrap();
        compare_blocks(&archive_raw, &rpc_raw, slot).unwrap();
        let mint = &crate::live::USDC;
        let mut history = History::open(path).unwrap();
        let mut a = Observation {
            slot,
            raw: archive_raw,
            archive: true,
        };
        let mut b = Observation {
            slot,
            raw: rpc_raw,
            archive: false,
        };
        let observations: Vec<&mut Observation> = if reverse {
            vec![&mut b, &mut a]
        } else {
            vec![&mut a, &mut b]
        };
        for observation in observations {
            let (coverage, _) = history
                .run(mint, slot, slot, observation, || false)
                .unwrap();
            assert!(coverage.complete);
        }
        let last = if reverse { &mut a } else { &mut b };
        assert_eq!(
            history
                .run(mint, slot, slot, last, || false)
                .unwrap()
                .1
                .source_attempts,
            0
        );
        history.reverify(mint, slot, slot).unwrap();
        assert!(
            history
                .run(mint, slot, slot, last, || false)
                .unwrap()
                .0
                .complete
        );
        let expected =
            decoder::decode(&decoder::parse_capture(&serde_json::to_vec(&value).unwrap()).unwrap())
                .unwrap();
        assert_eq!(Store::query(path, None, 1000).unwrap(), expected);
        assert_eq!(Store::summary(path, mint).unwrap()["transfer_count"], 2);
        let mut db = path.test_client();
        assert_eq!(
            db.query_one("SELECT COUNT(DISTINCT source) FROM capture_inputs", &[])
                .unwrap()
                .get::<_, i64>(0),
            2
        );
    }
}

#[test]
fn corrupt_truncated_or_incomplete_archives_cannot_prove_empty_coverage() {
    let value = fixture();
    let slot = value["result"]["slot"].as_u64().unwrap();
    let raw = slot_range(&value, slot, slot - 1, 7, true);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut corrupt = raw.clone();
    corrupt[45] ^= 1;
    assert!(runtime.block_on(decode_slot(&corrupt, slot)).is_err());
    assert!(
        runtime
            .block_on(decode_slot(&raw[..raw.len() - 1], slot))
            .is_err()
    );
    assert!(runtime.block_on(decode_slot(&raw, slot + 1)).is_err());
    let mut missing = transaction(&value);
    missing.metadata.data = Buffer::from_vec(vec![]);
    assert_eq!(
        normalize_transaction(&missing, &NodesWithCids::new(), None)
            .unwrap_err()
            .0,
        "archive_missing_execution_metadata"
    );
}

#[test]
fn immutable_block_identity_rejects_conflicts_and_warms_existing_raw_inputs() {
    let path = crate::storage::pg::TestDatabase::new();
    let path = &path.0;
    let _history = History::open(path).unwrap();
    let slot = 12345u64;
    let block = json!({"blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([6;32]).into_string(),"parentSlot":slot-1,"blockTime":100,"transactions":[{"transaction":{"signatures":[bs58::encode([8;64]).into_string()]}}]});
    let mut db = path.test_client();
    db.execute(
        "INSERT INTO historical_inputs(mint,decoder,slot,attempt,source_kind,received_at,produced_block,response) VALUES('mint','old',$1,1,'finalized-mainnet-rpc',1,true,$2)",
        &[&(slot as i64), &(serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":block})).unwrap())],
    )
    .unwrap();
    let mut store = Store::open(path).unwrap();
    let mut conflict = block.clone();
    conflict["blockhash"] = json!(bs58::encode([9; 32]).into_string());
    assert_eq!(
        store
            .observe_block_identity(
                slot,
                &conflict,
                conflict["transactions"].as_array().unwrap()
            )
            .unwrap_err()
            .0,
        "block_identity_conflict"
    );
    store
        .observe_block_identity(slot, &block, block["transactions"].as_array().unwrap())
        .unwrap();
    conflict = block.clone();
    conflict["transactions"][0]["transaction"]["signatures"][0] =
        json!(bs58::encode([10; 64]).into_string());
    assert_eq!(
        store
            .observe_block_identity(
                slot,
                &conflict,
                conflict["transactions"].as_array().unwrap()
            )
            .unwrap_err()
            .0,
        "block_identity_conflict"
    );
    assert_eq!(
        db.query_one("SELECT COUNT(*) FROM transfers", &[])
            .unwrap()
            .get::<_, i64>(0),
        0
    );
}

#[test]
fn failed_legacy_identity_migration_cannot_be_bypassed_on_retry() {
    let path = crate::storage::pg::TestDatabase::new();
    let path = &path.0;
    let _history = History::open(path).unwrap();
    let mut db = path.test_client();
    let block = json!({"blockhash":bs58::encode([7;32]).into_string(),"previousBlockhash":bs58::encode([6;32]).into_string(),"parentSlot":10,"blockTime":100,"transactions":[]});
    for attempt in 1..=2i64 {
        let mut prior = block.clone();
        if attempt == 2 {
            prior["blockhash"] = json!(bs58::encode([9; 32]).into_string());
        }
        db.execute(
            "INSERT INTO historical_inputs(mint,decoder,slot,attempt,source_kind,received_at,produced_block,response) VALUES('mint','old',11,$1,'finalized-mainnet-rpc',1,true,$2)",
            &[&attempt, &(serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":prior})).unwrap())],
        )
        .unwrap();
    }
    let mut store = Store::open(path).unwrap();
    for _ in 0..2 {
        assert_eq!(
            store.observe_block_identity(11, &block, &[]).unwrap_err().0,
            "block_identity_conflict"
        );
        assert_eq!(
            db.query_one("SELECT COUNT(*) FROM historical_block_identity", &[])
                .unwrap()
                .get::<_, i64>(0),
            0
        );
    }
}

#[test]
fn acknowledged_cache_corruption_fails_closed_without_network() {
    let value = fixture();
    let slot = value["result"]["slot"].as_u64().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let raw = slot_range(&value, slot, slot - 1, 7, true);
    put_cache(dir.path(), slot, &raw);
    fs::write(
        dir.path()
            .join(format!("epoch-{}-slot-{slot}.car-range", slot / 432000)),
        b"broken",
    )
    .unwrap();
    let mut source = ArchiveSource::new(dir.path(), false, Limits::default()).unwrap();
    assert!(source.block(slot).is_err());
    assert_eq!(source.stats.requests, 0);
}
