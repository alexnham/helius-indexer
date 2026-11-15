//! Full-block work for a routed job: one charged batch of at most eight slots per call,
//! through the shared History path. The archive lane serves archive and overlap
//! segments; the gap lane fills, with RPC `getBlock`, only the measured interval
//! between the archive tip and the replay window. Replay evidence is not reconciled here.
use crate::{
    Error, Result,
    backfill::{BlockSource, History},
    storage::{
        Database,
        replay::{JobLog, Lane, RouteBounds, valid_id},
    },
    telemetry::Telemetry,
};
use serde_json::{Value, json};

const BATCH_SLOTS: u64 = 8;

/// Slots of `segments` a lane may serve. The gap lane takes only uncovered, finalized
/// slots strictly between a measured archive tip and a measured replay start; unknown
/// availability is never filled by RPC.
pub(crate) fn lane_segments(
    lane: Lane,
    segments: &[(u64, u64, String)],
    bounds: RouteBounds,
) -> Vec<(u64, u64, String)> {
    match lane {
        Lane::Archive => segments
            .iter()
            .filter(|s| s.2 == "archive" || s.2 == "overlap")
            .cloned()
            .collect(),
        Lane::Gap => {
            let (Some(last), Some(first), reference) = bounds else {
                return vec![];
            };
            segments
                .iter()
                .filter(|s| s.2 == "uncovered")
                .filter_map(|s| {
                    let (a, b) = (
                        s.0.max(last.saturating_add(1)),
                        s.1.min(first.saturating_sub(1)).min(reference),
                    );
                    (a <= b && first > 0).then(|| (a, b, "gap".to_owned()))
                })
                .collect()
        }
    }
}

/// Next up-to-eight incomplete slots a lane may serve under the latest route.
fn next_range(database: &Database, id: &str, lane: Lane) -> Result<Option<(i64, u64, u64)>> {
    let mut log = JobLog::open(database)?;
    let mint = log.scope(id)?.mint;
    let (revision, segments) = log.latest_route(id)?.ok_or(Error("replay_route_missing"))?;
    let bounds = log.route_bounds(id, revision)?;
    let history = History::open(database)?;
    for (start, end, _) in lane_segments(lane, &segments, bounds) {
        let coverage = history.coverage(&mint, start, end)?;
        if let Some(slot) = coverage
            .slots
            .iter()
            .find(|s| s.status != "complete" && s.status != "skipped")
        {
            let mut last = end.min(slot.slot + BATCH_SLOTS - 1);
            // A skipped slot is proven only by a later block's parent link, so a batch
            // reaching the gap's end also fetches the replay start's block (finalized only).
            // It settles a skipped tail; it is not counted as gap coverage.
            if lane == Lane::Gap
                && last == end
                && bounds.1 == Some(end + 1)
                && end < bounds.2
                && last - slot.slot + 1 < BATCH_SLOTS
            {
                last = end + 1;
            }
            return Ok(Some((revision, slot.slot, last)));
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

/// One charged archive batch. `observed` reads the source's (requests, response bytes, cached chunks).
#[cfg_attr(not(feature = "archive"), allow(dead_code))]
pub(crate) fn batch<S: BlockSource>(
    database: &Database,
    id: &str,
    gate: std::sync::Arc<crate::coordinator::WriteGate>,
    source: &mut S,
    observed: impl Fn(&S) -> (u64, u64, u64),
    cancelled: impl Fn() -> bool,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    lane_batch(
        database,
        id,
        Lane::Archive,
        gate,
        source,
        observed,
        cancelled,
        telemetry,
    )
}

/// One charged batch of `lane`, through the shared History path.
#[allow(clippy::too_many_arguments)]
pub(crate) fn lane_batch<S: BlockSource>(
    database: &Database,
    id: &str,
    lane: Lane,
    gate: std::sync::Arc<crate::coordinator::WriteGate>,
    source: &mut S,
    observed: impl Fn(&S) -> (u64, u64, u64),
    cancelled: impl Fn() -> bool,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    valid_id(id)?;
    let mint = JobLog::open(database)?.scope(id)?.mint;
    let (component, source_kind, event_name, idle) = match lane {
        Lane::Archive => (
            "replay_archive",
            "archive",
            "archive_attempt_finished",
            "no_pending_archive_slots",
        ),
        Lane::Gap => (
            "replay_gap",
            "rpc",
            "gap_attempt_finished",
            "no_pending_gap_slots",
        ),
    };
    telemetry.set_context(component, source_kind, "historical");
    let Some((revision, start, end)) = next_range(database, id, lane)? else {
        return Ok(json!({"attempt":null,"reason":idle,"job":super::status(database, id)?}));
    };
    let reservation = {
        let _permit = gate.historical()?;
        JobLog::reserve_batch(database, id, lane, revision, start, end)?
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
        event_name,
        lane.name(),
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
    lane_grant(database, id, Lane::Archive, telemetry)
}

pub fn lane_grant(
    database: &Database,
    id: &str,
    lane: Lane,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    valid_id(id)?;
    JobLog::open(database)?.lane_grant(id, lane)?;
    match lane {
        Lane::Archive => telemetry.set_context("replay_archive", "archive", "historical"),
        Lane::Gap => telemetry.set_context("replay_gap", "rpc", "historical"),
    }
    telemetry.emit(
        match lane {
            Lane::Archive => "archive_budget_granted",
            Lane::Gap => "gap_budget_granted",
        },
        "budget",
        "success",
        None,
    );
    super::status(database, id)
}

/// Opt-in RPC fill of the measured archive-to-replay gap: one charged batch of at most
/// eight `getBlock` requests plus a mainnet check, from `SOLANA_RPC_URL`.
pub fn gap(database: &Database, id: &str, telemetry: &mut Telemetry) -> Result<Value> {
    gap_admitted(database, id, Default::default(), telemetry)
}

pub(crate) fn gap_admitted(
    database: &Database,
    id: &str,
    gate: std::sync::Arc<crate::coordinator::WriteGate>,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    use std::time::{Duration, Instant};
    if std::env::var("INDEXER_LIVE_OPT_IN").as_deref() != Ok("1") {
        return Err(Error("live_opt_in_required"));
    }
    let (_, _, seconds) = Lane::Gap.limits();
    let began = Instant::now();
    let mut source = crate::backfill::RpcSource::bounded(
        std::env::var("SOLANA_RPC_URL")
            .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into()),
        began + Duration::from_secs(seconds as u64),
    )?;
    let cancel = std::env::var_os("BACKFILL_CANCEL_FILE");
    lane_batch(
        database,
        id,
        Lane::Gap,
        gate,
        &mut source,
        |s| {
            let d = s.diagnostics();
            (
                d["requests"].as_u64().unwrap_or(0),
                d["response_body_bytes_read"].as_u64().unwrap_or(0),
                0,
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

#[cfg(test)]
mod tests;
