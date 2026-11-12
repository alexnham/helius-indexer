use crate::{Error, Result, decoder::MAX_CAPTURE_BYTES};
use jetstreamer_firehose::{
    node::{Node, NodeWithCid, NodesWithCids, parse_any_from_cbordata},
    node_reader::{RawNode, read_uvarint},
    transaction::Transaction,
};
use laserstream_core_proto::{
    geyser::{SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo},
    solana::storage::confirmed_block as wire,
};
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    io::{Cursor, Read},
};

/// A slot's range contains CAR sections without the file's header.
pub async fn decode_slot(raw: &[u8], expected_slot: u64) -> Result<(Value, u64, String)> {
    if raw.is_empty() || raw.len() > 16 * 1024 * 1024 {
        return Err(Error("archive_slot_size_limit"));
    }
    let nodes = parse_nodes(raw).await?;
    let block = nodes
        .get_block()
        .map_err(|_| Error("archive_missing_block"))?;
    if block.slot != expected_slot || block.meta.parent_slot >= expected_slot {
        return Err(Error("archive_block_identity"));
    }
    let mut transactions = Vec::new();
    let mut bytes = 0;
    let mut last_hash = None;
    let mut used = HashSet::new();
    for entry_cid in &block.entries {
        if !used.insert(*entry_cid) {
            return Err(Error("archive_duplicate_entry"));
        }
        let Node::Entry(entry) = nodes
            .get_by_cid(entry_cid)
            .ok_or(Error("archive_missing_entry"))?
            .get_node()
        else {
            return Err(Error("archive_invalid_entry"));
        };
        if entry.hash.to_bytes().len() != 32 {
            return Err(Error("archive_invalid_entry_hash"));
        }
        last_hash = Some(bs58::encode(entry.hash.to_bytes()).into_string());
        for tx_cid in &entry.transactions {
            if !used.insert(*tx_cid) {
                return Err(Error("archive_duplicate_transaction"));
            }
            let Node::Transaction(tx) = nodes
                .get_by_cid(tx_cid)
                .ok_or(Error("archive_missing_transaction"))?
                .get_node()
            else {
                return Err(Error("archive_invalid_transaction"));
            };
            if tx.slot != block.slot || tx.index.is_some_and(|i| i != transactions.len() as u64) {
                return Err(Error("archive_transaction_order"));
            }
            let capture = normalize_transaction(
                tx,
                &nodes,
                Some(
                    i64::try_from(block.meta.blocktime)
                        .map_err(|_| Error("archive_invalid_block_time"))?,
                ),
            )?;
            let result = capture["result"].clone();
            bytes += serde_json::to_vec(&result)
                .map_err(|_| Error("serialization_failed"))?
                .len();
            if bytes > crate::backfill::MAX_BLOCK_BYTES - 4096 {
                return Err(Error("archive_normalized_block_limit"));
            }
            transactions.push(result);
        }
    }
    if nodes
        .0
        .iter()
        .filter(|n| matches!(n.get_node(), Node::Entry(_)))
        .count()
        != block.entries.len()
    {
        return Err(Error("archive_unreferenced_entry"));
    }
    let all_transactions = nodes
        .0
        .iter()
        .filter(|n| matches!(n.get_node(), Node::Transaction(_)))
        .count();
    if all_transactions != transactions.len() {
        return Err(Error("archive_unreferenced_transaction"));
    }
    let hash = last_hash.ok_or(Error("archive_missing_entry_hash"))?;
    Ok((
        json!({"blockhash":hash,"parentSlot":block.meta.parent_slot,"blockTime":block.meta.blocktime,"transactions":transactions}),
        block.meta.parent_slot,
        hash,
    ))
}

/// Normalize the maintained Jetstreamer wire/metadata representation through the existing live contract.
pub fn normalize_transaction(
    tx: &Transaction,
    nodes: &NodesWithCids,
    block_time: Option<i64>,
) -> Result<Value> {
    let mut complete = tx.clone();
    let data = nodes
        .reassemble_dataframes(&tx.data)
        .map_err(|_| Error("archive_incomplete_transaction"))?;
    if data.len() > 4096 {
        return Err(Error("archive_transaction_limit"));
    }
    complete.data.data = jetstreamer_firehose::utils::Buffer::from_vec(data);
    complete.data.next = None;
    let native = complete
        .as_parsed()
        .map_err(|_| Error("archive_transaction_format"))?;
    native
        .sanitize()
        .map_err(|_| Error("archive_invalid_transaction"))?;
    let metadata = nodes
        .reassemble_dataframes(&tx.metadata)
        .map_err(|_| Error("archive_incomplete_metadata"))?;
    if metadata.is_empty() {
        return Err(Error("archive_missing_execution_metadata"));
    }
    let mut decoded = Vec::new();
    if metadata.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        zstd::stream::read::Decoder::new(metadata.as_slice())
            .map_err(|_| Error("archive_invalid_metadata"))?
            .take(MAX_CAPTURE_BYTES as u64 + 1)
            .read_to_end(&mut decoded)
            .map_err(|_| Error("archive_invalid_metadata"))?;
    } else {
        decoded = metadata;
    }
    if decoded.is_empty() || decoded.len() > MAX_CAPTURE_BYTES {
        return Err(Error("archive_metadata_limit"));
    }
    let meta = wire::TransactionStatusMeta::decode(decoded.as_slice())
        .map_err(|_| Error("archive_metadata_format"))?;
    let msg = &native.message;
    let account_count = msg.static_account_keys().len()
        + meta.loaded_writable_addresses.len()
        + meta.loaded_readonly_addresses.len();
    if meta.pre_balances.len() != account_count || meta.post_balances.len() != account_count {
        return Err(Error("archive_incomplete_metadata"));
    }
    let mut message = wire::Message {
        account_keys: msg
            .static_account_keys()
            .iter()
            .map(|k| k.as_ref().to_vec())
            .collect(),
        recent_blockhash: msg.recent_blockhash().as_ref().to_vec(),
        instructions: msg
            .instructions()
            .iter()
            .map(|i| wire::CompiledInstruction {
                program_id_index: i.program_id_index as u32,
                accounts: i.accounts.clone(),
                data: i.data.clone(),
            })
            .collect(),
        address_table_lookups: msg
            .address_table_lookups()
            .unwrap_or(&[])
            .iter()
            .map(|l| wire::MessageAddressTableLookup {
                account_key: l.account_key.as_ref().to_vec(),
                writable_indexes: l.writable_indexes.clone(),
                readonly_indexes: l.readonly_indexes.clone(),
            })
            .collect(),
        ..Default::default()
    };
    match msg {
        solana_message::VersionedMessage::Legacy(_) => {}
        solana_message::VersionedMessage::V0(_) => message.versioned = true,
        solana_message::VersionedMessage::V1(_) => message.config = Some(Default::default()),
    }
    let signatures: Vec<Vec<u8>> = native
        .signatures
        .iter()
        .map(|s| s.as_ref().to_vec())
        .collect();
    let update = SubscribeUpdateTransaction {
        slot: tx.slot,
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature: signatures
                .first()
                .cloned()
                .ok_or(Error("missing_signature"))?,
            transaction: Some(wire::Transaction {
                signatures,
                message: Some(message),
            }),
            meta: Some(meta),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut capture: Value = serde_json::from_slice(&crate::live::normalize(&update)?)
        .map_err(|_| Error("serialization_failed"))?;
    capture["source"] = json!(super::SOURCE);
    capture["result"]["blockTime"] = json!(block_time);
    let header = msg.header();
    capture["result"]["transaction"]["message"]["header"] = json!({"numRequiredSignatures":header.num_required_signatures,"numReadonlySignedAccounts":header.num_readonly_signed_accounts,"numReadonlyUnsignedAccounts":header.num_readonly_unsigned_accounts});
    capture["result"]["transaction"]["message"]["recentBlockhash"] =
        json!(msg.recent_blockhash().to_string());
    if let solana_message::VersionedMessage::V1(v1) = msg {
        capture["result"]["transaction"]["message"]["config"] =
            serde_json::to_value(v1.config).map_err(|_| Error("serialization_failed"))?;
    }
    Ok(capture)
}

async fn parse_nodes(raw: &[u8]) -> Result<NodesWithCids> {
    if raw.is_empty() || raw.len() > 16 * 1024 * 1024 {
        return Err(Error("archive_slot_size_limit"));
    }
    let mut cursor = Cursor::new(raw.to_vec());
    let mut nodes = NodesWithCids::new();
    let mut seen = HashSet::new();
    while (cursor.position() as usize) < raw.len() {
        let len = read_uvarint(&mut cursor)
            .await
            .map_err(|_| Error("archive_invalid_section"))?;
        if len == 0 || len > MAX_CAPTURE_BYTES as u64 || len > raw.len() as u64 - cursor.position()
        {
            return Err(Error("archive_invalid_section"));
        }
        let end = cursor.position() + len;
        let section = raw[cursor.position() as usize..end as usize].to_vec();
        cursor.set_position(end);
        let node = RawNode::from_cursor(&mut Cursor::new(section))
            .await
            .map_err(|_| Error("archive_invalid_cid"))?;
        if node.cid.codec() != 0x71
            || node.cid.hash().code() != 0x12
            || node.cid.hash().digest() != Sha256::digest(&node.data).as_slice()
        {
            return Err(Error("archive_cid_mismatch"));
        }
        if !seen.insert(node.cid) || nodes.len() >= 100_000 {
            return Err(Error("archive_node_limit"));
        }
        // Validate CBOR before calling the upstream parser, which assumes valid CBOR at this boundary.
        let cbor: serde_cbor::Value =
            serde_cbor::from_slice(&node.data).map_err(|_| Error("archive_invalid_cbor"))?;
        validate_node(&cbor)?;
        let typed = std::panic::catch_unwind(|| parse_any_from_cbordata(node.data))
            .map_err(|_| Error("archive_invalid_node"))?
            .map_err(|_| Error("archive_invalid_node"))?;
        nodes.push(NodeWithCid::new(node.cid, typed));
    }
    if nodes
        .0
        .iter()
        .filter(|n| matches!(n.get_node(), Node::Block(_)))
        .count()
        != 1
    {
        return Err(Error("archive_block_count"));
    }
    Ok(nodes)
}

pub(super) async fn parent_hash(raw: &[u8], slot: u64) -> Result<String> {
    let nodes = parse_nodes(raw).await?;
    let block = nodes
        .get_block()
        .map_err(|_| Error("archive_missing_block"))?;
    if block.slot != slot {
        return Err(Error("archive_block_identity"));
    }
    let last = block
        .entries
        .last()
        .ok_or(Error("archive_missing_entry_hash"))?;
    let Node::Entry(entry) = nodes
        .get_by_cid(last)
        .ok_or(Error("archive_missing_entry"))?
        .get_node()
    else {
        return Err(Error("archive_invalid_entry"));
    };
    Ok(bs58::encode(entry.hash.to_bytes()).into_string())
}
fn validate_node(value: &serde_cbor::Value) -> Result<()> {
    use serde_cbor::Value::{Array, Bytes, Integer, Null};
    let Array(a) = value else {
        return Err(Error("archive_invalid_node"));
    };
    let number = |v: Option<&serde_cbor::Value>| matches!(v,Some(Integer(n)) if *n>=0 && *n<=i64::MAX as i128);
    match a.first() {
        Some(Integer(0)) => {
            if !number(a.get(3)) {
                return Err(Error("archive_invalid_transaction"));
            }
            validate_frame(a.get(1).ok_or(Error("archive_invalid_transaction"))?)?;
            validate_frame(a.get(2).ok_or(Error("archive_invalid_metadata"))?)?;
            if a.get(4)
                .is_some_and(|v| !matches!(v, Null) && !number(Some(v)))
            {
                return Err(Error("archive_transaction_order"));
            }
        }
        Some(Integer(1)) => {
            if !number(a.get(1))
                || !matches!(a.get(2),Some(Bytes(b)) if b.len()==32)
                || !matches!(a.get(3), Some(Array(_)))
            {
                return Err(Error("archive_invalid_entry"));
            }
        }
        Some(Integer(2)) => {
            if !number(a.get(1))
                || !matches!(a.get(2), Some(Array(_)))
                || !matches!(a.get(3), Some(Array(_)))
            {
                return Err(Error("archive_invalid_block"));
            }
            let Some(Array(meta)) = a.get(4) else {
                return Err(Error("archive_invalid_block"));
            };
            if !number(meta.first()) || !number(meta.get(1)) {
                return Err(Error("archive_invalid_block_time"));
            }
        }
        Some(Integer(6)) => validate_frame(value)?,
        Some(Integer(5)) => {}
        _ => return Err(Error("archive_unexpected_node")),
    }
    Ok(())
}
fn validate_frame(value: &serde_cbor::Value) -> Result<()> {
    use serde_cbor::Value::{Array, Bytes, Integer, Null};
    let Array(a) = value else {
        return Err(Error("archive_invalid_frame"));
    };
    if a.first() != Some(&Integer(6))
        || !matches!(a.get(4),Some(Bytes(b)) if b.len()<=MAX_CAPTURE_BYTES)
    {
        return Err(Error("archive_invalid_frame"));
    }
    for value in a.iter().take(4).skip(1) {
        if !matches!(value, Null) && !matches!(value,Integer(n) if *n>=0 && *n<=u64::MAX as i128) {
            return Err(Error("archive_invalid_frame"));
        }
    }
    Ok(())
}

pub(super) async fn block_time(raw: &[u8], slot: u64) -> Result<i64> {
    let nodes = parse_nodes(raw).await?;
    let block = nodes
        .get_block()
        .map_err(|_| Error("archive_missing_block"))?;
    if block.slot != slot {
        return Err(Error("archive_block_identity"));
    }
    i64::try_from(block.meta.blocktime).map_err(|_| Error("archive_invalid_block_time"))
}
