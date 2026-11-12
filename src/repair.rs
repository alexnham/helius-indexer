//! Bounded offline revision repair. A repaired capture is not proof of full slot coverage.
use crate::{
    Error, Result, backfill::MAX_SLOTS, decoder::DECODER_VERSION, storage::Store,
    telemetry::Telemetry,
};
use rusqlite::{Connection, OpenFlags, params};
use serde_json::{Value, json};
use std::path::Path;

fn scope(start: u64, end: u64) -> Result<()> {
    if start > end || end > i64::MAX as u64 || end - start >= MAX_SLOTS {
        return Err(Error("invalid_repair_range"));
    }
    Ok(())
}

/// The completion flag concerns retained captures only; historical blocks must be reverified.
pub fn status(path: &Path, start: u64, end: u64) -> Result<Value> {
    scope(start, end)?;
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
    let (total, remaining): (i64,i64) = db.query_row("SELECT COUNT(*),COALESCE(SUM(decoder_version!=?3),0) FROM captures WHERE json_extract(result_json,'$.slot') BETWEEN ?1 AND ?2",
        params![start as i64,end as i64,DECODER_VERSION],|r|Ok((r.get(0)?,r.get(1)?))).map_err(|_|Error("storage_read_failed"))?;
    let (corrected, unresolved, oldest): (i64, i64, Option<i64>) = if exists {
        db.query_row("SELECT COALESCE(SUM(status='complete'),0),COALESCE(SUM(status='unresolved'),0),MIN(CASE WHEN status='unresolved' THEN first_pending END) FROM decoder_repairs WHERE slot BETWEEN ?1 AND ?2 AND target_decoder=?3",
            params![start as i64,end as i64,DECODER_VERSION],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).map_err(|_|Error("storage_read_failed"))?
    } else {
        (0, 0, None)
    };
    let mut failure_classes = Vec::new();
    if exists {
        let mut statement=db.prepare("SELECT error_class,COUNT(*) FROM decoder_repairs WHERE target_decoder=?3 AND slot BETWEEN ?1 AND ?2 AND status='unresolved' GROUP BY error_class ORDER BY COUNT(*) DESC,error_class LIMIT 16")
            .map_err(|_|Error("storage_read_failed"))?;
        let classes = statement
            .query_map(params![start as i64, end as i64, DECODER_VERSION], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })
            .map_err(|_| Error("storage_read_failed"))?;
        for class in classes {
            let (class, count) = class.map_err(|_| Error("storage_read_failed"))?;
            failure_classes.push(json!({"error_class":class,"count":count}));
        }
    }
    let age = if unresolved == 0 {
        Some(0)
    } else {
        oldest.and_then(|first| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| i64::try_from(d.as_secs()).ok())
                .and_then(|now| now.checked_sub(first))
                .filter(|age| *age >= 0)
        })
    };
    Ok(
        json!({"start":start,"end":end,"target_decoder":DECODER_VERSION,"retained_captures":total,"remaining_other_revision":remaining,"corrected":corrected,"unresolved":unresolved,
        "first_unresolved_at":oldest,"oldest_unresolved_age_seconds":age,"failure_classes":failure_classes,"failure_classes_limit":16,"repair_complete":total>0 && remaining==0 && unresolved==0,"coverage":"unknown","coverage_reason":"retained_captures_do_not_certify_full_blocks"}),
    )
}

/// Each call schedules at most 100 captures, preferring those with fewer prior attempts.
pub fn run(
    path: &Path,
    start: u64,
    end: u64,
    telemetry: &mut Telemetry,
    cancelled: impl Fn() -> bool,
) -> Result<Value> {
    scope(start, end)?;
    telemetry.set_context("decoder_repair", "retained_capture", "repair");
    let mut store = Store::open(path)?;
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| Error("storage_open_failed"))?;
    let selected: Vec<String> = {
        let mut statement = db.prepare("SELECT c.signature FROM captures c LEFT JOIN decoder_repairs r ON r.signature=c.signature AND r.target_decoder=?3 WHERE (c.decoder_version!=?3 OR r.status='unresolved') AND json_extract(c.result_json,'$.slot') BETWEEN ?1 AND ?2 ORDER BY COALESCE(r.attempts,0),c.signature LIMIT 100")
            .map_err(|_|Error("storage_read_failed"))?;
        statement
            .query_map(params![start as i64, end as i64, DECODER_VERSION], |r| {
                r.get(0)
            })
            .map_err(|_| Error("storage_read_failed"))?
            .collect::<std::result::Result<_, _>>()
            .map_err(|_| Error("storage_read_failed"))?
    };
    let (mut attempted, mut corrected, mut unresolved, mut replaced_rows, mut written_rows) =
        (0, 0, 0, 0, 0);
    let mut stopped = false;
    for signature in selected {
        if cancelled() {
            stopped = true;
            break;
        }
        let result = store.correct(&signature)?;
        attempted += 1;
        corrected += usize::from(result.error.is_none());
        unresolved += usize::from(result.error.is_some());
        replaced_rows += if result.error.is_none() {
            result.old_rows
        } else {
            0
        };
        written_rows += result.new_rows;
        telemetry.slot = Some(result.slot);
        telemetry.emit(
            "decoder_correction",
            "replace_transaction",
            if result.error.is_none() {
                "success"
            } else {
                "failed"
            },
            result.error.map(|e| e.0),
        );
    }
    telemetry.slot = None;
    let progress = status(path, start, end)?;
    telemetry.emit(
        "decoder_repair_batch_finished",
        "repair",
        if progress["repair_complete"] == true {
            "success"
        } else {
            "partial"
        },
        None,
    );
    Ok(
        json!({"run_id":telemetry.run_id,"attempted":attempted,"corrected":corrected,"unresolved":unresolved,"replaced_rows":replaced_rows,"written_rows":written_rows,"cancelled":stopped,"batch_limit":100,"progress":progress}),
    )
}

/// Reverify whole retained RPC blocks offline, including previously rejected candidates.
pub fn history(
    path: &Path,
    telemetry: &mut Telemetry,
    mint: &str,
    start: u64,
    end: u64,
    cancelled: impl Fn() -> bool,
) -> Result<Value> {
    history_from(path, telemetry, mint, start, end, cancelled, false)
}

/// Reverify normalized archive blocks without fetching CAR files or the network.
pub fn archive_history(
    path: &Path,
    telemetry: &mut Telemetry,
    mint: &str,
    start: u64,
    end: u64,
    cancelled: impl Fn() -> bool,
) -> Result<Value> {
    history_from(path, telemetry, mint, start, end, cancelled, true)
}
fn history_from(
    path: &Path,
    telemetry: &mut Telemetry,
    mint: &str,
    start: u64,
    end: u64,
    cancelled: impl Fn() -> bool,
    archive: bool,
) -> Result<Value> {
    telemetry.set_context("decoder_repair", "retained_block", "repair");
    crate::backfill::scope(mint, start, end)?;
    struct NoNetwork(bool);
    impl crate::backfill::BlockSource for NoNetwork {
        fn provenance(&self) -> &'static str {
            if self.0 {
                "old-faithful-jetstreamer"
            } else {
                "finalized-mainnet-rpc"
            }
        }
        fn capture_source(&self) -> &'static str {
            if self.0 {
                "old-faithful-jetstreamer"
            } else {
                "solana-mainnet-rpc"
            }
        }
        fn block(&mut self, _slot: u64) -> Result<Vec<u8>> {
            Err(Error("retained_block_unavailable"))
        }
    }
    let mut history = crate::backfill::History::open(path)?;
    history.enable_revision_repair(telemetry.run_id.clone());
    let run_id = history.run_id().to_owned();
    let (coverage, metrics) = history.run(mint, start, end, &mut NoNetwork(archive), cancelled)?;
    Ok(
        json!({"run_id":run_id,"source":"retained_blocks_only","source_kind":if archive {"archive"} else {"rpc"},"coverage":coverage,"metrics":metrics}),
    )
}
