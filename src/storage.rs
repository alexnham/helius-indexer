pub(crate) mod repair;
use crate::{
    Error, Result,
    decoder::DECODER_VERSION,
    model::{Capture, Transfer},
    reconcile,
};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use std::{collections::BTreeMap, path::Path, time::Duration};

pub struct Store(Connection);

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path).map_err(|_| Error("storage_open_failed"))?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(|_| Error("storage_configuration_failed"))?;
        db.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS captures (
                signature TEXT PRIMARY KEY, result_json TEXT NOT NULL,
                capture_json BLOB NOT NULL, status TEXT NOT NULL, decoder_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS transfers (
                transfer_id TEXT PRIMARY KEY, signature TEXT NOT NULL,
                slot INTEGER NOT NULL, outer_index INTEGER NOT NULL,
                inner_index INTEGER NOT NULL, mint TEXT NOT NULL, record_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS transfers_mint ON transfers(mint);
            CREATE TABLE IF NOT EXISTS capture_inputs (
                signature TEXT NOT NULL, source TEXT NOT NULL, result_json TEXT NOT NULL,
                capture_json BLOB NOT NULL, status TEXT NOT NULL,
                PRIMARY KEY(signature,source,result_json));
            CREATE TABLE IF NOT EXISTS decoder_repairs (
                signature TEXT NOT NULL, target_decoder TEXT NOT NULL, slot INTEGER NOT NULL,
                previous_decoder TEXT NOT NULL, status TEXT NOT NULL, error_class TEXT,
                attempts INTEGER NOT NULL, first_pending INTEGER NOT NULL,
                PRIMARY KEY(signature,target_decoder));",
        )
        .map_err(|_| Error("storage_configuration_failed"))?;
        Ok(Self(db))
    }

    /// Stores a complete transaction's output atomically. Replay of identical input is a no-op.
    pub fn persist(
        &mut self,
        capture: &Capture,
        raw: &[u8],
        decoded: &Result<Vec<Transfer>>,
    ) -> Result<usize> {
        let signature = capture
            .result
            .transaction
            .signatures
            .first()
            .ok_or(Error("missing_signature"))?;
        let value: serde_json::Value =
            serde_json::from_slice(raw).map_err(|_| Error("invalid_json"))?;
        let result_json =
            serde_json::to_string(&value["result"]).map_err(|_| Error("invalid_json"))?;
        let transaction = self
            .0
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|_| Error("storage_write_failed"))?;
        let prior: Option<(String, String, String)> = transaction
            .query_row(
                "SELECT result_json,decoder_version,status FROM captures WHERE signature=?1",
                [signature],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(|_| Error("storage_read_failed"))?;
        // Lazy additive migration preserves the original raw input before any enrichment.
        transaction.execute("INSERT OR IGNORE INTO capture_inputs SELECT signature,'legacy',result_json,capture_json,status FROM captures WHERE signature=?1 AND NOT EXISTS(SELECT 1 FROM capture_inputs WHERE signature=?1)",[signature]).map_err(|_|Error("storage_write_failed"))?;
        transaction.execute("INSERT INTO capture_inputs VALUES(?1,?2,?3,?4,'received') ON CONFLICT(signature,source,result_json) DO NOTHING",params![signature,capture.source,result_json,raw]).map_err(|_|Error("storage_write_failed"))?;
        let status = match decoded {
            Ok(_) if !capture.result.meta.err.is_null() => "failed_transaction",
            Ok(_) => "decoded",
            Err(e) => e.0,
        };
        let mut repaired = None;
        if let Some((prior, version, prior_status)) = prior {
            let prior_value: serde_json::Value =
                serde_json::from_str(&prior).map_err(|_| Error("stored_record_invalid"))?;
            let mut outcome = if version != DECODER_VERSION {
                Err(Error("decoder_revision_conflict"))
            } else {
                reconcile::compatible_results(&prior_value, &value["result"])
            };
            let mut reference = None;
            if outcome.is_ok() {
                outcome = (|| {
                    let mut combined = reconcile::reference(&prior_value, &value["result"])?;
                    if decoded.is_err() && !prior_value["meta"]["logMessages"].is_null() {
                        combined["meta"]["logMessages"] =
                            prior_value["meta"]["logMessages"].clone();
                    }
                    let mut envelope = value.clone();
                    envelope["result"] = combined.clone();
                    let raw =
                        serde_json::to_vec(&envelope).map_err(|_| Error("serialization_failed"))?;
                    let combined_capture = crate::decoder::parse_capture(&raw)?;
                    if decoded.is_ok() {
                        repaired = Some(crate::decoder::decode(&combined_capture)?);
                    }
                    reference = Some(
                        serde_json::to_string(&combined)
                            .map_err(|_| Error("serialization_failed"))?,
                    );
                    Ok(())
                })();
            }
            let mut updates = Vec::new();
            if outcome.is_ok()
                && prior_status == "decoded"
                && let Some(incoming) = repaired.as_ref()
            {
                let mut statement = transaction
                    .prepare("SELECT transfer_id,record_json FROM transfers WHERE signature=?1")
                    .map_err(|_| Error("storage_read_failed"))?;
                let old = statement
                    .query_map([signature], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                    })
                    .map_err(|_| Error("storage_read_failed"))?
                    .collect::<std::result::Result<BTreeMap<_, _>, _>>()
                    .map_err(|_| Error("storage_read_failed"))?;
                if old.len() != incoming.len() {
                    outcome = Err(Error("capture_conflict"));
                }
                for new in incoming {
                    let Some(old) = old.get(&new.transfer_id) else {
                        outcome = Err(Error("capture_conflict"));
                        break;
                    };
                    let old: Transfer =
                        serde_json::from_str(old).map_err(|_| Error("stored_record_invalid"))?;
                    match reconcile::transfer(&old, new) {
                        Ok(merged) => updates.push(merged),
                        Err(e) => {
                            outcome = Err(e);
                            break;
                        }
                    }
                }
            }
            let receipt_status = outcome.as_ref().map_or_else(|e| e.0, |_| status);
            transaction.execute("UPDATE capture_inputs SET status=?1 WHERE signature=?2 AND source=?3 AND result_json=?4",params![receipt_status,signature,capture.source,result_json]).map_err(|_|Error("storage_write_failed"))?;
            if let Err(error) = outcome {
                // Quarantine is durable, but existing logical query results remain unchanged.
                transaction
                    .commit()
                    .map_err(|_| Error("storage_commit_failed"))?;
                return Err(error);
            }
            transaction
                .execute(
                    "UPDATE captures SET result_json=?1 WHERE signature=?2",
                    params![reference, signature],
                )
                .map_err(|_| Error("storage_write_failed"))?;
            if prior_status == "decoded" || prior_status == "failed_transaction" {
                for merged in updates {
                    let text = serde_json::to_string(&merged)
                        .map_err(|_| Error("serialization_failed"))?;
                    transaction
                        .execute(
                            "UPDATE transfers SET record_json=?1 WHERE transfer_id=?2",
                            params![text, merged.transfer_id],
                        )
                        .map_err(|_| Error("storage_write_failed"))?;
                }
                transaction
                    .commit()
                    .map_err(|_| Error("storage_commit_failed"))?;
                return Ok(0);
            }
            // A same-revision unresolved observation may become decodable after metadata arrives.
            if decoded.is_err() {
                transaction
                    .commit()
                    .map_err(|_| Error("storage_commit_failed"))?;
                return Ok(0);
            }
            transaction
                .execute(
                    "UPDATE captures SET capture_json=?1,status=?2 WHERE signature=?3",
                    params![raw, status, signature],
                )
                .map_err(|_| Error("storage_write_failed"))?;
        } else {
            transaction
                .execute(
                    "INSERT INTO captures VALUES (?1,?2,?3,?4,?5)",
                    params![signature, result_json, raw, status, DECODER_VERSION],
                )
                .map_err(|_| Error("storage_write_failed"))?;
            transaction.execute("UPDATE capture_inputs SET status=?1 WHERE signature=?2 AND source=?3 AND result_json=?4",params![status,signature,capture.source,result_json]).map_err(|_|Error("storage_write_failed"))?;
        }
        let mut inserted = 0;
        if let Some(transfers) = repaired.as_ref().or_else(|| decoded.as_ref().ok()) {
            for transfer in transfers {
                let json =
                    serde_json::to_string(transfer).map_err(|_| Error("serialization_failed"))?;
                transaction
                    .execute(
                        "INSERT INTO transfers VALUES (?1,?2,?3,?4,?5,?6,?7)",
                        params![
                            transfer.transfer_id,
                            signature,
                            transfer.slot as i64,
                            transfer.instruction_index as i64,
                            transfer.inner_instruction_index.map_or(-1, |n| n as i64),
                            transfer.mint,
                            json,
                        ],
                    )
                    .map_err(|_| Error("storage_write_failed"))?;
                inserted += 1;
            }
        }
        transaction
            .commit()
            .map_err(|_| Error("storage_commit_failed"))?;
        Ok(inserted)
    }

    pub fn summary(path: &Path, mint: &str) -> Result<serde_json::Value> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|_| Error("storage_open_failed"))?;
        let mut statement = db
            .prepare(&format!(
                "SELECT record_json FROM transfers WHERE mint=?1 {}",
                repair_filter(&db, "?2")?
            ))
            .map_err(|_| Error("storage_read_failed"))?;
        let rows = statement
            .query_map(params![mint, DECODER_VERSION], |r| r.get::<_, String>(0))
            .map_err(|_| Error("storage_read_failed"))?;
        let (mut count, mut volume, mut fees, mut unknown_fees) = (0u64, 0u128, 0u128, 0u64);
        let mut other_decoder_transfers = 0u64;
        for row in rows {
            let transfer: Transfer =
                serde_json::from_str(&row.map_err(|_| Error("storage_read_failed"))?)
                    .map_err(|_| Error("stored_record_invalid"))?;
            if transfer.decoder_version != DECODER_VERSION {
                other_decoder_transfers = other_decoder_transfers
                    .checked_add(1)
                    .ok_or(Error("aggregate_overflow"))?;
            }
            count = count.checked_add(1).ok_or(Error("aggregate_overflow"))?;
            volume = volume
                .checked_add(u128::from(transfer.amount_raw))
                .ok_or(Error("aggregate_overflow"))?;
            if let Some(fee) = transfer.fee_raw {
                fees = fees
                    .checked_add(u128::from(fee))
                    .ok_or(Error("aggregate_overflow"))?;
            } else {
                unknown_fees = unknown_fees
                    .checked_add(1)
                    .ok_or(Error("aggregate_overflow"))?;
            }
        }
        Ok(
            serde_json::json!({"mint":mint,"transfer_count":count,"target_decoder":DECODER_VERSION,"other_decoder_transfer_count":other_decoder_transfers,"known_volume_raw":volume.to_string(),"known_fees_raw":fees.to_string(),"unknown_fee_count":unknown_fees,"coverage":"unknown","window":"all_stored"}),
        )
    }

    pub fn query(path: &Path, mint: Option<&str>, limit: usize) -> Result<Vec<Transfer>> {
        if !(1..=1000).contains(&limit) {
            return Err(Error("invalid_query_limit"));
        }
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|_| Error("storage_open_failed"))?;
        let mut stmt = db
            .prepare(&format!(
                "SELECT record_json FROM transfers
            WHERE (?1 IS NULL OR mint=?1) {}
            ORDER BY slot, signature, outer_index, inner_index LIMIT ?2",
                repair_filter(&db, "?3")?
            ))
            .map_err(|_| Error("storage_read_failed"))?;
        let rows = stmt
            .query_map(params![mint, limit as i64, DECODER_VERSION], |r| {
                r.get::<_, String>(0)
            })
            .map_err(|_| Error("storage_read_failed"))?;
        rows.map(|row| {
            let text = row.map_err(|_| Error("storage_read_failed"))?;
            serde_json::from_str(&text).map_err(|_| Error("stored_record_invalid"))
        })
        .collect()
    }
}

// Older read-only databases need no migration just to inspect their existing records.
fn repair_filter(db: &Connection, parameter: &str) -> Result<String> {
    let exists: bool = db
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='decoder_repairs')",
            [],
            |r| r.get(0),
        )
        .map_err(|_| Error("storage_read_failed"))?;
    Ok(if exists {
        format!(
            "AND {parameter} IS NOT NULL AND NOT EXISTS(SELECT 1 FROM decoder_repairs r WHERE r.signature=transfers.signature AND r.status='unresolved')"
        )
    } else {
        format!("AND {parameter} IS NOT NULL")
    })
}

impl Store {
    pub(crate) fn observe_block_identity(
        &mut self,
        slot: u64,
        block: &serde_json::Value,
        transactions: &[serde_json::Value],
    ) -> Result<()> {
        self.0
            .execute_batch("SAVEPOINT block_identity_observation")
            .map_err(|_| Error("storage_write_failed"))?;
        let result = self.observe_block_identity_scan(slot, block, transactions);
        if result.is_ok() {
            self.0
                .execute_batch("RELEASE block_identity_observation")
                .map_err(|_| Error("storage_commit_failed"))?;
        } else {
            self.0
                .execute_batch(
                    "ROLLBACK TO block_identity_observation; RELEASE block_identity_observation",
                )
                .map_err(|_| Error("storage_write_failed"))?;
        }
        result
    }
    fn observe_block_identity_scan(
        &mut self,
        slot: u64,
        block: &serde_json::Value,
        transactions: &[serde_json::Value],
    ) -> Result<()> {
        let known: bool = self
            .0
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM historical_block_identity WHERE slot=?1)",
                [slot as i64],
                |r| r.get(0),
            )
            .map_err(|_| Error("storage_read_failed"))?;
        if !known {
            let mut total = 0;
            for index in 0..33 {
                let raw:Option<Option<Vec<u8>>>=self.0.query_row("SELECT CASE WHEN length(response)<=67108864 THEN response END FROM historical_inputs WHERE slot=?1 ORDER BY received_at,attempt LIMIT 1 OFFSET ?2",params![slot as i64,index],|r|r.get(0)).optional().map_err(|_|Error("storage_read_failed"))?;
                let Some(raw) = raw else {
                    break;
                };
                let raw = raw.ok_or(Error("block_identity_observation_limit"))?;
                total += raw.len();
                if index == 32
                    || total > 128 * 1024 * 1024
                    || raw.len() > crate::backfill::MAX_BLOCK_BYTES
                {
                    return Err(Error("block_identity_observation_limit"));
                }
                let Ok(value) = serde_json::from_slice::<serde_json::Value>(&raw) else {
                    continue;
                };
                if value["jsonrpc"] != "2.0" || value["id"] != 1 || value.get("error").is_some() {
                    continue;
                }
                let prior = &value["result"];
                let Some(txs) = prior["transactions"].as_array() else {
                    continue;
                };
                if txs.len() > 100_000
                    || !prior["parentSlot"].as_u64().is_some_and(|p| p < slot)
                    || !prior
                        .get("blockTime")
                        .is_some_and(|v| v.is_null() || v.is_i64())
                {
                    continue;
                }
                if ["blockhash", "previousBlockhash"].iter().any(|field| {
                    prior[*field].as_str().is_none_or(|s| {
                        s.len() > 44 || bs58::decode(s).into_vec().map_or(true, |b| b.len() != 32)
                    })
                }) {
                    continue;
                }
                self.observe_block_identity_inner(slot, prior, txs)?;
            }
        }
        self.observe_block_identity_inner(slot, block, transactions)
    }
    fn observe_block_identity_inner(
        &mut self,
        slot: u64,
        block: &serde_json::Value,
        transactions: &[serde_json::Value],
    ) -> Result<()> {
        use sha2::{Digest, Sha256};
        let mut ids = Vec::with_capacity(transactions.len());
        let mut seen = std::collections::HashSet::new();
        for tx in transactions {
            let signatures = tx["transaction"]["signatures"]
                .as_array()
                .filter(|s| !s.is_empty())
                .ok_or(Error("missing_signature"))?;
            for signature in signatures {
                let signature = signature
                    .as_str()
                    .filter(|s| s.len() <= 100)
                    .ok_or(Error("invalid_base58_identifier"))?;
                if bs58::decode(signature)
                    .into_vec()
                    .map_or(true, |s| s.len() != 64)
                {
                    return Err(Error("invalid_base58_identifier"));
                }
            }
            if !seen.insert(&signatures[0]) {
                return Err(Error("duplicate_block_transaction"));
            }
            ids.push(signatures);
        }
        let digest =
            Sha256::digest(serde_json::to_vec(&ids).map_err(|_| Error("serialization_failed"))?)
                .to_vec();
        let hash = block["blockhash"]
            .as_str()
            .ok_or(Error("invalid_block_hash"))?;
        let previous = block["previousBlockhash"]
            .as_str()
            .ok_or(Error("invalid_block_hash"))?;
        let parent = block["parentSlot"]
            .as_i64()
            .ok_or(Error("missing_block_parent"))?;
        let time = block["blockTime"].as_i64();
        let transaction = &self.0;
        type StoredBlockIdentity = (String, i64, String, Vec<u8>, Option<i64>);
        let prior:Option<StoredBlockIdentity>=transaction.query_row("SELECT blockhash,parent_slot,previous_blockhash,transaction_ids_hash,block_time FROM historical_block_identity WHERE slot=?1",[slot as i64],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional().map_err(|_|Error("storage_read_failed"))?;
        if let Some((old_hash, old_parent, old_previous, old_ids, old_time)) = prior {
            if old_hash != hash
                || old_parent != parent
                || old_previous != previous
                || old_ids != digest
                || matches!((old_time,time),(Some(a),Some(b)) if a!=b)
            {
                return Err(Error("block_identity_conflict"));
            }
            transaction.execute("UPDATE historical_block_identity SET block_time=COALESCE(block_time,?2) WHERE slot=?1",params![slot as i64,time]).map_err(|_|Error("storage_write_failed"))?;
        } else {
            transaction
                .execute(
                    "INSERT INTO historical_block_identity VALUES(?1,?2,?3,?4,?5,?6)",
                    params![slot as i64, hash, parent, previous, digest, time],
                )
                .map_err(|_| Error("storage_write_failed"))?;
        }
        Ok(())
    }
}
