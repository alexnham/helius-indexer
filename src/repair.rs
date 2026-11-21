//! Bounded offline revision repair. A repaired capture is not proof of full slot coverage.
use crate::storage::Database;
use crate::{
    Error, Result, backfill::MAX_SLOTS, decoder::DECODER_VERSION, storage::Store,
    telemetry::Telemetry,
};
use serde_json::{Value, json};

fn scope(start: u64, end: u64) -> Result<()> {
    if start > end || end > i64::MAX as u64 || end - start >= MAX_SLOTS {
        return Err(Error("invalid_repair_range"));
    }
    Ok(())
}

/// The completion flag concerns retained captures only; historical blocks must be reverified.
pub fn status(database: &Database, start: u64, end: u64) -> Result<Value> {
    scope(start, end)?;
    let counts = Store::repair_counts(database, start, end)?;
    let (total, remaining, corrected, unresolved, oldest) = (
        counts.total,
        counts.remaining_other_revision,
        counts.corrected,
        counts.unresolved,
        counts.first_unresolved_at,
    );
    let failure_classes: Vec<Value> = counts
        .failure_classes
        .iter()
        .map(|(class, count)| json!({"error_class":class,"count":count}))
        .collect();
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
    database: &Database,
    start: u64,
    end: u64,
    telemetry: &mut Telemetry,
    cancelled: impl Fn() -> bool,
) -> Result<Value> {
    scope(start, end)?;
    telemetry.set_context("decoder_repair", "retained_capture", "repair");
    let mut store = Store::open(database)?;
    let selected = Store::repair_candidates(database, start, end, 100)?;
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
    let progress = status(database, start, end)?;
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
    database: &Database,
    telemetry: &mut Telemetry,
    mint: &str,
    start: u64,
    end: u64,
    cancelled: impl Fn() -> bool,
) -> Result<Value> {
    history_from(database, telemetry, mint, start, end, cancelled, false)
}

/// Reverify retained archive blocks. Without `cache` nothing is fetched; with it (archive
/// feature, `INDEXER_ARCHIVE_OPT_IN=1`), a slot retained only as a CAR receipt is read
/// again from Old Faithful into `cache`.
pub fn archive_history(
    database: &Database,
    telemetry: &mut Telemetry,
    mint: &str,
    start: u64,
    end: u64,
    cache: Option<&std::path::Path>,
    cancelled: impl Fn() -> bool,
) -> Result<Value> {
    #[cfg(feature = "archive")]
    if let Some(cache) = cache {
        crate::backfill::scope(mint, start, end)?;
        let limits = crate::archive::Limits::bridge(end - start + 1, 120);
        let mut source = crate::archive::ArchiveSource::new(cache, true, limits)?;
        return history_with(
            database,
            telemetry,
            mint,
            start,
            end,
            cancelled,
            &mut source,
            "retained_blocks_or_archive",
        );
    }
    #[cfg(not(feature = "archive"))]
    if cache.is_some() {
        return Err(Error("archive_feature_required"));
    }
    history_from(database, telemetry, mint, start, end, cancelled, true)
}
fn history_from(
    database: &Database,
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
    history_with(
        database,
        telemetry,
        mint,
        start,
        end,
        cancelled,
        &mut NoNetwork(archive),
        "retained_blocks_only",
    )
}
#[allow(clippy::too_many_arguments)]
fn history_with(
    database: &Database,
    telemetry: &mut Telemetry,
    mint: &str,
    start: u64,
    end: u64,
    cancelled: impl Fn() -> bool,
    source: &mut impl crate::backfill::BlockSource,
    label: &str,
) -> Result<Value> {
    telemetry.set_context("decoder_repair", "retained_block", "repair");
    let mut history = crate::backfill::History::open(database)?;
    history.enable_revision_repair(telemetry.run_id.clone());
    let run_id = history.run_id().to_owned();
    let archive = source.provenance() == "old-faithful-jetstreamer";
    let (coverage, metrics) = history.run(mint, start, end, source, cancelled)?;
    Ok(
        json!({"run_id":run_id,"source":label,"source_kind":if archive {"archive"} else {"rpc"},"coverage":coverage,"metrics":metrics}),
    )
}
