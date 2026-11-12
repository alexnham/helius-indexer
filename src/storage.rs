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
                PRIMARY KEY(signature,source,result_json));",
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
        if let Some((prior, version, prior_status)) = prior {
            let prior_value: serde_json::Value =
                serde_json::from_str(&prior).map_err(|_| Error("stored_record_invalid"))?;
            let mut outcome = if version != DECODER_VERSION {
                Err(Error("decoder_revision_conflict"))
            } else {
                reconcile::compatible_results(&prior_value, &value["result"])
            };
            let mut updates = Vec::new();
            if outcome.is_ok()
                && prior_status == "decoded"
                && let Ok(incoming) = decoded
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
            if prior_status == "decoded" || prior_status == "failed_transaction" {
                if decoded.is_ok() {
                    let reference = serde_json::to_string(&reconcile::reference(
                        &prior_value,
                        &value["result"],
                    ))
                    .map_err(|_| Error("serialization_failed"))?;
                    transaction
                        .execute(
                            "UPDATE captures SET result_json=?1 WHERE signature=?2",
                            params![reference, signature],
                        )
                        .map_err(|_| Error("storage_write_failed"))?;
                }
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
            transaction.execute("UPDATE captures SET result_json=?1,capture_json=?2,status=?3 WHERE signature=?4",params![result_json,raw,status,signature]).map_err(|_|Error("storage_write_failed"))?;
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
        if let Ok(transfers) = decoded {
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
            .prepare("SELECT record_json FROM transfers WHERE mint=?1")
            .map_err(|_| Error("storage_read_failed"))?;
        let rows = statement
            .query_map([mint], |r| r.get::<_, String>(0))
            .map_err(|_| Error("storage_read_failed"))?;
        let (mut count, mut volume, mut fees, mut unknown_fees) = (0u64, 0u128, 0u128, 0u64);
        for row in rows {
            let transfer: Transfer =
                serde_json::from_str(&row.map_err(|_| Error("storage_read_failed"))?)
                    .map_err(|_| Error("stored_record_invalid"))?;
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
            serde_json::json!({"mint":mint,"transfer_count":count,"known_volume_raw":volume.to_string(),"known_fees_raw":fees.to_string(),"unknown_fee_count":unknown_fees,"coverage":"unknown","window":"all_stored"}),
        )
    }

    pub fn query(path: &Path, mint: Option<&str>, limit: usize) -> Result<Vec<Transfer>> {
        if !(1..=1000).contains(&limit) {
            return Err(Error("invalid_query_limit"));
        }
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|_| Error("storage_open_failed"))?;
        let mut stmt = db
            .prepare(
                "SELECT record_json FROM transfers
            WHERE (?1 IS NULL OR mint=?1)
            ORDER BY slot, signature, outer_index, inner_index LIMIT ?2",
            )
            .map_err(|_| Error("storage_read_failed"))?;
        let rows = stmt
            .query_map(params![mint, limit as i64], |r| r.get::<_, String>(0))
            .map_err(|_| Error("storage_read_failed"))?;
        rows.map(|row| {
            let text = row.map_err(|_| Error("storage_read_failed"))?;
            serde_json::from_str(&text).map_err(|_| Error("stored_record_invalid"))
        })
        .collect()
    }
}
