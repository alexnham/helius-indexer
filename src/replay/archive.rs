//! Archive work for a routed job: one charged batch of at most eight slots per call,
//! through the shared History path. Full blocks give per-slot status for these slots
//! only; routed replay or overlap evidence is not reconciled here.
use crate::{
    Error, Result,
    backfill::{BlockSource, History},
    storage::{
        Database,
        replay::{JobLog, valid_id},
    },
    telemetry::Telemetry,
};
use serde_json::{Value, json};

// Only the archive-feature command drives a batch; tests drive it with a fake source.
#[cfg_attr(not(feature = "archive"), allow(dead_code))]
const BATCH_SLOTS: u64 = 8;

#[cfg_attr(not(feature = "archive"), allow(dead_code))]
/// Next up-to-eight incomplete slots of the latest route's archive or overlap segments.
fn next_range(database: &Database, id: &str) -> Result<Option<(i64, u64, u64)>> {
    let mut log = JobLog::open(database)?;
    let mint = log.scope(id)?.mint;
    let (revision, segments) = log.latest_route(id)?.ok_or(Error("replay_route_missing"))?;
    let history = History::open(database)?;
    for (start, end, kind) in segments {
        if kind != "archive" && kind != "overlap" {
            continue;
        }
        let coverage = history.coverage(&mint, start, end)?;
        if let Some(slot) = coverage
            .slots
            .iter()
            .find(|s| s.status != "complete" && s.status != "skipped")
        {
            return Ok(Some((
                revision,
                slot.slot,
                end.min(slot.slot + BATCH_SLOTS - 1),
            )));
        }
    }
    Ok(None)
}

/// Per routed archive/overlap segment, full-block slot status counts. Replay segments get none.
pub(crate) fn segment_status(database: &Database, id: &str) -> Result<Value> {
    let mut log = JobLog::open(database)?;
    let mint = log.scope(id)?.mint;
    let Some((revision, segments)) = log.latest_route(id)? else {
        return Ok(Value::Null);
    };
    let history = History::open(database)?;
    let mut out = Vec::new();
    for (start, end, kind) in segments {
        if kind != "archive" && kind != "overlap" {
            continue;
        }
        let coverage = history.coverage(&mint, start, end)?;
        let count = |status: &str| coverage.slots.iter().filter(|s| s.status == status).count();
        out.push(json!({"start":start,"end":end,"kind":kind,"complete":count("complete"),"skipped":count("skipped"),
            "pending":coverage.pending,"oldest_pending_age_seconds":coverage.oldest_pending_age_seconds}));
    }
    Ok(json!({"route_revision":revision,"evidence":"full_block_slot_status","segments":out}))
}

#[cfg_attr(not(feature = "archive"), allow(dead_code))]
/// One charged batch. `observed` reads the source's (requests, response bytes, cached chunks).
pub(crate) fn batch<S: BlockSource>(
    database: &Database,
    id: &str,
    gate: std::sync::Arc<crate::coordinator::WriteGate>,
    source: &mut S,
    observed: impl Fn(&S) -> (u64, u64, u64),
    cancelled: impl Fn() -> bool,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    valid_id(id)?;
    let mint = JobLog::open(database)?.scope(id)?.mint;
    telemetry.set_context("replay_archive", "archive", "historical");
    let Some((revision, start, end)) = next_range(database, id)? else {
        return Ok(
            json!({"attempt":null,"reason":"no_pending_archive_slots","job":super::status(database, id)?}),
        );
    };
    let reservation = {
        let _permit = gate.historical()?;
        JobLog::archive_reserve(database, id, revision, start, end)?
    };
    let attempt = reservation.attempt;
    let result = History::open(database).and_then(|mut history| {
        history.set_gate(std::sync::Arc::clone(&gate));
        history.run(&mint, start, end, source, &cancelled)
    });
    let status = match &result {
        Err(e) => e.0,
        Ok((_, metrics)) if metrics.cancelled => "cancelled",
        Ok((coverage, _)) if coverage.complete => "finished",
        Ok(_) => "incomplete",
    };
    {
        let _permit = gate.historical()?;
        reservation.finish(status, observed(source))?;
    }
    let mut event = telemetry.event(
        "archive_attempt_finished",
        "archive",
        match status {
            "finished" => "success",
            "incomplete" => "incomplete",
            other => other,
        },
        result.as_ref().err().map(|e| e.0),
    );
    let (requests, bytes, cached) = observed(source);
    event["requests"] = json!(requests);
    event["response_bytes"] = json!(bytes);
    event["cached_chunks"] = json!(cached);
    use std::io::Write;
    let _ = writeln!(std::io::stderr().lock(), "{event}");
    let (coverage, metrics) = result?;
    Ok(
        json!({"attempt":attempt,"route_revision":revision,"start":start,"end":end,"status":status,
        "coverage":coverage,"metrics":metrics,"job":super::status(database, id)?}),
    )
}

/// Opt-in archive batch: requires the archive feature and INDEXER_ARCHIVE_OPT_IN=1.
#[cfg(feature = "archive")]
pub fn run(
    database: &Database,
    id: &str,
    cache: &std::path::Path,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    run_admitted(database, id, cache, Default::default(), telemetry)
}

/// One archive batch whose writes yield to live intake through `gate`.
#[cfg(feature = "archive")]
pub(crate) fn run_admitted(
    database: &Database,
    id: &str,
    cache: &std::path::Path,
    gate: std::sync::Arc<crate::coordinator::WriteGate>,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    use crate::{archive, storage::replay::ARCHIVE_BATCH_LIMITS};
    use std::time::{Duration, Instant};
    let (requests, bytes, seconds) = ARCHIVE_BATCH_LIMITS;
    let mut source = archive::ArchiveSource::new(
        cache,
        true,
        archive::Limits {
            requests: requests as u32,
            bytes: bytes as u64,
            seconds: seconds as u64,
        },
    )?;
    let began = Instant::now();
    let cancel = std::env::var_os("BACKFILL_CANCEL_FILE");
    batch(
        database,
        id,
        gate,
        &mut source,
        |s| {
            (
                u64::from(s.stats.requests),
                s.stats.response_body_bytes,
                u64::from(s.stats.cached_chunks),
            )
        },
        || {
            began.elapsed() >= Duration::from_secs(seconds as u64)
                || cancel
                    .as_ref()
                    .is_some_and(|p| std::path::Path::new(p).exists())
        },
        telemetry,
    )
}

pub fn grant(database: &Database, id: &str, telemetry: &mut Telemetry) -> Result<Value> {
    valid_id(id)?;
    JobLog::open(database)?.archive_grant(id)?;
    telemetry.set_context("replay_archive", "archive", "historical");
    telemetry.emit("archive_budget_granted", "budget", "success", None);
    super::status(database, id)
}

#[cfg(test)]
mod tests;
