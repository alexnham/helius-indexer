//! Transaction-scoped replacement; scheduling and range diagnostics live in crate::repair.
use super::Store;
use crate::{Error, Result, decoder};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde_json::Value;
use std::path::Path;

/// Retained-capture repair progress for one slot range and the current decoder revision.
pub(crate) struct RepairCounts {
    pub total: i64,
    pub remaining_other_revision: i64,
    pub corrected: i64,
    pub unresolved: i64,
    pub first_unresolved_at: Option<i64>,
    /// Most frequent unresolved error classes, at most 16.
    pub failure_classes: Vec<(String, i64)>,
}

pub(crate) struct Correction {
    pub slot: u64,
    pub error: Option<Error>,
    pub old_rows: usize,
    pub new_rows: usize,
}

impl Store {
    pub(crate) fn correct(&mut self, signature: &str) -> Result<Correction> {
        let tx = self
            .0
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|_| Error("storage_write_failed"))?;
        let (raw_len, result_len, slot, previous): (i64, i64, i64, String) = tx.query_row(
            "SELECT length(capture_json),length(CAST(result_json AS BLOB)),json_extract(result_json,'$.slot'),decoder_version FROM captures WHERE signature=?1", [signature],
            |r| Ok((r.get(0)?, r.get(1)?,r.get(2)?,r.get(3)?))).map_err(|_| Error("storage_read_failed"))?;
        let revision_number = |value: &str| {
            value
                .strip_prefix("token-transfers-v")
                .and_then(|n| n.parse::<u32>().ok())
        };
        if let (Some(previous), Some(target)) = (
            revision_number(&previous),
            revision_number(decoder::DECODER_VERSION),
        ) && previous > target
        {
            return Err(Error("decoder_downgrade_not_supported"));
        }
        let slot = u64::try_from(slot).map_err(|_| Error("stored_record_invalid"))?;
        let bounded = raw_len <= decoder::MAX_CAPTURE_BYTES as i64
            && result_len <= decoder::MAX_CAPTURE_BYTES as i64;
        let mut transaction_failed = false;
        let mut merged_reference = None;
        let decoded = (|| {
            if !bounded {
                return Err(Error("capture_too_large"));
            }
            let (raw, reference): (Vec<u8>, String) = tx
                .query_row(
                    "SELECT capture_json,result_json FROM captures WHERE signature=?1",
                    [signature],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(|_| Error("storage_read_failed"))?;
            let mut reference: Value =
                serde_json::from_str(&reference).map_err(|_| Error("stored_record_invalid"))?;
            // Audit receipts may contain compatible execution metadata received after the old decoder failed.
            let mut statement=tx.prepare("SELECT CASE WHEN length(CAST(result_json AS BLOB))<=16777216 THEN result_json END FROM capture_inputs WHERE signature=?1 AND status!='capture_conflict' ORDER BY source,result_json LIMIT 33")
                .map_err(|_|Error("storage_read_failed"))?;
            let mut total = 0;
            for (count, observation) in statement
                .query_map([signature], |r| r.get::<_, Option<String>>(0))
                .map_err(|_| Error("storage_read_failed"))?
                .enumerate()
            {
                let observation = observation
                    .map_err(|_| Error("storage_read_failed"))?
                    .ok_or(Error("capture_too_large"))?;
                total += observation.len();
                if count >= 32 || total > 64 * 1024 * 1024 {
                    return Err(Error("repair_observation_limit"));
                }
                let observation: Value = serde_json::from_str(&observation)
                    .map_err(|_| Error("stored_record_invalid"))?;
                crate::reconcile::compatible_results(&reference, &observation)?;
                let prior_logs = reference["meta"]["logMessages"].clone();
                let incoming_logs = observation["meta"]["logMessages"].clone();
                if reference["meta"]["err"].is_null()
                    && [&prior_logs, &incoming_logs]
                        .iter()
                        .filter_map(|v| v.as_array())
                        .flatten()
                        .filter_map(Value::as_str)
                        .any(decoder::runtime_failure)
                {
                    return Err(Error("unsupported_caught_token_failure"));
                }
                reference = crate::reconcile::reference(&reference, &observation)?;
                if prior_logs.as_array().map_or(0, Vec::len)
                    > incoming_logs.as_array().map_or(0, Vec::len)
                {
                    reference["meta"]["logMessages"] = prior_logs;
                }
            }
            let mut envelope: Value =
                serde_json::from_slice(&raw).map_err(|_| Error("stored_record_invalid"))?;
            merged_reference =
                Some(serde_json::to_string(&reference).map_err(|_| Error("serialization_failed"))?);
            envelope["result"] = reference;
            let combined =
                serde_json::to_vec(&envelope).map_err(|_| Error("serialization_failed"))?;
            let capture = decoder::parse_capture(&combined)?;
            if capture
                .result
                .transaction
                .signatures
                .first()
                .map(String::as_str)
                != Some(signature)
            {
                return Err(Error("stored_capture_identity_conflict"));
            }
            transaction_failed = !capture.result.meta.err.is_null();
            decoder::decode(&capture)
        })();
        if let Err(error) = decoded.as_ref()
            && error.0.starts_with("storage_")
        {
            return Err(*error);
        }
        let old_rows: i64 = tx
            .query_row(
                "SELECT COUNT(*) FROM transfers WHERE signature=?1",
                [signature],
                |r| r.get(0),
            )
            .map_err(|_| Error("storage_read_failed"))?;
        let old_rows = usize::try_from(old_rows).map_err(|_| Error("stored_record_invalid"))?;
        let error = decoded.as_ref().err().copied();
        if bounded {
            tx.execute("INSERT OR IGNORE INTO capture_inputs SELECT signature,'legacy',result_json,capture_json,status FROM captures WHERE signature=?1 AND NOT EXISTS(SELECT 1 FROM capture_inputs WHERE signature=?1)", [signature])
            .map_err(|_| Error("storage_write_failed"))?;
        }
        tx.execute("INSERT INTO decoder_repairs VALUES(?1,?2,?3,?4,?5,?6,1,unixepoch()) ON CONFLICT(signature,target_decoder) DO UPDATE SET status=excluded.status,error_class=excluded.error_class,attempts=decoder_repairs.attempts+1",
            params![signature,decoder::DECODER_VERSION,slot as i64,previous,if error.is_some(){"unresolved"}else{"complete"},error.map(|e|e.0)])
            .map_err(|_| Error("storage_write_failed"))?;
        // Retained captures can be filtered or incomplete; only a full block replay can verify coverage.
        let history: Option<String> = tx
            .query_row(
                "SELECT name FROM sqlite_master WHERE name='historical_slots'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(|_| Error("storage_read_failed"))?;
        if history.is_some() {
            tx.execute("UPDATE historical_slots SET status='unknown',error_class='decoder_repair_requires_verification' WHERE slot=?1 AND decoder=?2 AND status='complete'",params![slot as i64,decoder::DECODER_VERSION])
                .map_err(|_| Error("storage_write_failed"))?;
        }
        let mut new_rows = 0;
        if let Ok(transfers) = decoded {
            tx.execute("DELETE FROM transfers WHERE signature=?1", [signature])
                .map_err(|_| Error("storage_write_failed"))?;
            for transfer in &transfers {
                let json =
                    serde_json::to_string(transfer).map_err(|_| Error("serialization_failed"))?;
                tx.execute(
                    "INSERT INTO transfers VALUES(?1,?2,?3,?4,?5,?6,?7)",
                    params![
                        transfer.transfer_id,
                        signature,
                        transfer.slot as i64,
                        transfer.instruction_index as i64,
                        transfer.inner_instruction_index.map_or(-1, |n| n as i64),
                        transfer.mint,
                        json
                    ],
                )
                .map_err(|_| Error("storage_write_failed"))?;
            }
            new_rows = transfers.len();
            tx.execute("UPDATE capture_inputs SET status='revision_reconciled' WHERE signature=?1 AND status='decoder_revision_conflict'",[signature])
                .map_err(|_|Error("storage_write_failed"))?;
            tx.execute("UPDATE decoder_repairs SET status='superseded',error_class=NULL WHERE signature=?1 AND target_decoder!=?2", params![signature,decoder::DECODER_VERSION])
                .map_err(|_| Error("storage_write_failed"))?;
            tx.execute(
                "UPDATE captures SET decoder_version=?1,status=?2,result_json=?4 WHERE signature=?3",
                params![
                    decoder::DECODER_VERSION,
                    if transaction_failed {
                        "failed_transaction"
                    } else {
                        "decoded"
                    },
                    signature,
                    merged_reference
                ],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        }
        tx.commit().map_err(|_| Error("storage_commit_failed"))?;
        Ok(Correction {
            slot,
            error,
            old_rows,
            new_rows,
        })
    }
}

impl Store {
    /// Read-only and consistent: all counts come from one snapshot.
    pub(crate) fn repair_counts(path: &Path, start: u64, end: u64) -> Result<RepairCounts> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|_| Error("storage_open_failed"))?;
        let _snapshot = db
            .unchecked_transaction()
            .map_err(|_| Error("storage_read_failed"))?;
        let exists: bool = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='decoder_repairs')",
                [],
                |r| r.get(0),
            )
            .map_err(|_| Error("storage_read_failed"))?;
        let (total, remaining_other_revision): (i64,i64) = db.query_row("SELECT COUNT(*),COALESCE(SUM(decoder_version!=?3),0) FROM captures WHERE json_extract(result_json,'$.slot') BETWEEN ?1 AND ?2",
            params![start as i64,end as i64,decoder::DECODER_VERSION],|r|Ok((r.get(0)?,r.get(1)?))).map_err(|_|Error("storage_read_failed"))?;
        let (corrected, unresolved, first_unresolved_at): (i64, i64, Option<i64>) = if exists {
            db.query_row("SELECT COALESCE(SUM(status='complete'),0),COALESCE(SUM(status='unresolved'),0),MIN(CASE WHEN status='unresolved' THEN first_pending END) FROM decoder_repairs WHERE slot BETWEEN ?1 AND ?2 AND target_decoder=?3",
                params![start as i64,end as i64,decoder::DECODER_VERSION],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|_|Error("storage_read_failed"))?
        } else {
            (0, 0, None)
        };
        let mut failure_classes = Vec::new();
        if exists {
            let mut statement=db.prepare("SELECT error_class,COUNT(*) FROM decoder_repairs WHERE target_decoder=?3 AND slot BETWEEN ?1 AND ?2 AND status='unresolved' GROUP BY error_class ORDER BY COUNT(*) DESC,error_class LIMIT 16")
                .map_err(|_|Error("storage_read_failed"))?;
            let classes = statement
                .query_map(
                    params![start as i64, end as i64, decoder::DECODER_VERSION],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
                )
                .map_err(|_| Error("storage_read_failed"))?;
            for class in classes {
                failure_classes.push(class.map_err(|_| Error("storage_read_failed"))?);
            }
        }
        Ok(RepairCounts {
            total,
            remaining_other_revision,
            corrected,
            unresolved,
            first_unresolved_at,
            failure_classes,
        })
    }

    /// Captures in range that are on another revision or unresolved, fewest prior attempts first.
    pub(crate) fn repair_candidates(
        path: &Path,
        start: u64,
        end: u64,
        limit: usize,
    ) -> Result<Vec<String>> {
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|_| Error("storage_open_failed"))?;
        let mut statement = db.prepare("SELECT c.signature FROM captures c LEFT JOIN decoder_repairs r ON r.signature=c.signature AND r.target_decoder=?3 WHERE (c.decoder_version!=?3 OR r.status='unresolved') AND json_extract(c.result_json,'$.slot') BETWEEN ?1 AND ?2 ORDER BY COALESCE(r.attempts,0),c.signature LIMIT ?4")
            .map_err(|_|Error("storage_read_failed"))?;
        statement
            .query_map(
                params![
                    start as i64,
                    end as i64,
                    decoder::DECODER_VERSION,
                    limit as i64
                ],
                |r| r.get(0),
            )
            .map_err(|_| Error("storage_read_failed"))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|_| Error("storage_read_failed"))
    }
}
