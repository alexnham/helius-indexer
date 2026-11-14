//! Transaction-scoped replacement; scheduling and range diagnostics live in crate::repair.
use super::{Database, Store, insert_transfers, read_failed, write_failed};
use crate::{Error, Result, decoder};
use serde_json::Value;

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
        let slot: Option<i64> = self
            .db
            .query_opt(
                "SELECT slot FROM captures WHERE signature=$1",
                &[&signature],
            )
            .map_err(read_failed)?
            .map(|r| r.get(0));
        let slot = u64::try_from(slot.ok_or(Error("storage_read_failed"))?)
            .map_err(|_| Error("stored_record_invalid"))?;
        self.ensure_partitions([slot])?;
        let mut tx = self.db.transaction().map_err(write_failed)?;
        // Locks the capture so a concurrent observation or repair waits for this replacement.
        let row = tx.query_one(
            "SELECT octet_length(capture_json),octet_length(result_json),decoder_version FROM captures WHERE signature=$1 FOR UPDATE", &[&signature])
            .map_err(read_failed)?;
        let (raw_len, result_len, previous): (i32, i32, String) =
            (row.get(0), row.get(1), row.get(2));
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
        let bounded = raw_len as usize <= decoder::MAX_CAPTURE_BYTES
            && result_len as usize <= decoder::MAX_CAPTURE_BYTES;
        let mut transaction_failed = false;
        let mut merged_reference = None;
        let decoded = (|| {
            if !bounded {
                return Err(Error("capture_too_large"));
            }
            let row = tx
                .query_one(
                    "SELECT capture_json,result_json FROM captures WHERE signature=$1",
                    &[&signature],
                )
                .map_err(read_failed)?;
            let (raw, reference): (Vec<u8>, String) = (row.get(0), row.get(1));
            let mut reference: Value =
                serde_json::from_str(&reference).map_err(|_| Error("stored_record_invalid"))?;
            // Audit receipts may contain compatible execution metadata received after the old decoder failed.
            let observations = tx.query("SELECT CASE WHEN octet_length(result_json)<=16777216 THEN result_json END FROM capture_inputs WHERE signature=$1 AND status<>'capture_conflict' ORDER BY source,result_digest LIMIT 33", &[&signature])
                .map_err(read_failed)?;
            let mut total = 0;
            for (count, observation) in observations.iter().enumerate() {
                let observation: String = observation
                    .get::<_, Option<String>>(0)
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
            .query_one(
                "SELECT COUNT(*) FROM transfers WHERE signature=$1",
                &[&signature],
            )
            .map_err(read_failed)?
            .get(0);
        let old_rows = usize::try_from(old_rows).map_err(|_| Error("stored_record_invalid"))?;
        let error = decoded.as_ref().err().copied();
        let slot_key = slot as i64;
        tx.execute("INSERT INTO decoder_repairs VALUES($1,$2,$3,$4,$5,$6,1,extract(epoch from now())::bigint) ON CONFLICT(signature,target_decoder) DO UPDATE SET status=excluded.status,error_class=excluded.error_class,attempts=decoder_repairs.attempts+1",
            &[&signature,&decoder::DECODER_VERSION,&slot_key,&previous,&if error.is_some(){"unresolved"}else{"complete"},&error.map(|e|e.0)])
            .map_err(write_failed)?;
        // Retained captures can be filtered or incomplete; only a full block replay can verify coverage.
        tx.execute("UPDATE historical_slots SET status='unknown',error_class='decoder_repair_requires_verification' WHERE slot=$1 AND decoder=$2 AND status='complete'",&[&slot_key,&decoder::DECODER_VERSION])
            .map_err(write_failed)?;
        let mut new_rows = 0;
        if let Ok(transfers) = decoded {
            tx.execute("DELETE FROM transfers WHERE signature=$1", &[&signature])
                .map_err(write_failed)?;
            new_rows = insert_transfers(&mut tx, signature, Some(&transfers))?;
            tx.execute("UPDATE capture_inputs SET status='revision_reconciled' WHERE signature=$1 AND status='decoder_revision_conflict'",&[&signature])
                .map_err(write_failed)?;
            tx.execute("UPDATE decoder_repairs SET status='superseded',error_class=NULL WHERE signature=$1 AND target_decoder<>$2", &[&signature,&decoder::DECODER_VERSION])
                .map_err(write_failed)?;
            tx.execute(
                "UPDATE captures SET decoder_version=$1,status=$2,result_json=$4 WHERE signature=$3",
                &[
                    &decoder::DECODER_VERSION,
                    &if transaction_failed {
                        "failed_transaction"
                    } else {
                        "decoded"
                    },
                    &signature,
                    &merged_reference,
                ],
            )
            .map_err(write_failed)?;
        }
        tx.commit().map_err(|_| Error("storage_commit_failed"))?;
        Ok(Correction {
            slot,
            error,
            old_rows,
            new_rows,
        })
    }

    /// Read-only and consistent: all counts come from one snapshot.
    pub(crate) fn repair_counts(db: &Database, start: u64, end: u64) -> Result<RepairCounts> {
        let mut client = db.connect()?;
        let mut tx = client
            .build_transaction()
            .isolation_level(postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(read_failed)?;
        let (start, end) = (start as i64, end as i64);
        let row = tx.query_one("SELECT COUNT(*),COUNT(*) FILTER (WHERE decoder_version<>$3) FROM captures WHERE slot BETWEEN $1 AND $2",
            &[&start,&end,&decoder::DECODER_VERSION]).map_err(read_failed)?;
        let (total, remaining_other_revision) = (row.get(0), row.get(1));
        let row = tx.query_one("SELECT COUNT(*) FILTER (WHERE status='complete'),COUNT(*) FILTER (WHERE status='unresolved'),MIN(CASE WHEN status='unresolved' THEN first_pending END) FROM decoder_repairs WHERE slot BETWEEN $1 AND $2 AND target_decoder=$3",
            &[&start,&end,&decoder::DECODER_VERSION]).map_err(read_failed)?;
        let (corrected, unresolved, first_unresolved_at) = (row.get(0), row.get(1), row.get(2));
        let failure_classes = tx.query("SELECT error_class,COUNT(*) FROM decoder_repairs WHERE target_decoder=$3 AND slot BETWEEN $1 AND $2 AND status='unresolved' GROUP BY error_class ORDER BY COUNT(*) DESC,error_class LIMIT 16",
            &[&start,&end,&decoder::DECODER_VERSION])
            .map_err(read_failed)?
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, i64>(1)))
            .collect();
        tx.commit().map_err(read_failed)?;
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
        db: &Database,
        start: u64,
        end: u64,
        limit: usize,
    ) -> Result<Vec<String>> {
        Ok(db.connect()?
            .query("SELECT c.signature FROM captures c LEFT JOIN decoder_repairs r ON r.signature=c.signature AND r.target_decoder=$3 WHERE (c.decoder_version<>$3 OR r.status='unresolved') AND c.slot BETWEEN $1 AND $2 ORDER BY COALESCE(r.attempts,0),c.signature LIMIT $4",
                &[&(start as i64), &(end as i64), &decoder::DECODER_VERSION, &(limit as i64)])
            .map_err(read_failed)?
            .iter()
            .map(|r| r.get(0))
            .collect())
    }
}
