//! Frozen, measured source boundaries with bounded resumable work.
use super::WriteGate;
use crate::storage::Database;
use crate::{
    Error, Result, archive,
    backfill::{BlockSource, History, Metrics, RpcSource},
    decoder,
    storage::bridge::BridgeLog,
    telemetry::Telemetry,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const BATCH_SLOTS: u64 = 8;
const MAX_PLAN_SPAN: u64 = 16_000_000;
const REPORT_RANGES: usize = 32;

#[derive(Clone)]
struct Plan {
    mint: String,
    start: u64,
    tip: u64,
    end: u64,
    snapshot: Value,
}
impl Plan {
    fn validate(&self) -> Result<()> {
        crate::backfill::scope(&self.mint, self.start, self.start)?;
        if self.tip == 0
            || self.tip > self.end
            || self.start > self.tip
            || self.end > i64::MAX as u64
            || self.end - self.start >= MAX_PLAN_SPAN
        {
            return Err(Error("invalid_bridge_plan"));
        }
        if self.snapshot["last_observed_archive_slot"].as_u64() != Some(self.tip)
            || self.snapshot["reference_slot"].as_u64() != Some(self.end)
            || self.snapshot["tip_exact_within_probed_epochs"] != true
        {
            return Err(Error("unverified_bridge_boundary"));
        }
        Ok(())
    }
}
fn load(db: &BridgeLog, mint: &str, start: u64) -> Result<Option<Plan>> {
    db.plan(mint, start)?
        .map(|(tip, end, snapshot)| {
            if snapshot.len() > 16384 {
                return Err(Error("invalid_bridge_plan"));
            }
            let plan = Plan {
                mint: mint.into(),
                start,
                tip: u64::try_from(tip).map_err(|_| Error("invalid_bridge_plan"))?,
                end: u64::try_from(end).map_err(|_| Error("invalid_bridge_plan"))?,
                snapshot: serde_json::from_str(&snapshot)
                    .map_err(|_| Error("invalid_bridge_plan"))?,
            };
            plan.validate()?;
            Ok(plan)
        })
        .transpose()
}
fn save(db: &BridgeLog, plan: &Plan) -> Result<()> {
    plan.validate()?;
    db.freeze_plan(
        &plan.mint,
        plan.start,
        plan.tip,
        plan.end,
        &plan.snapshot.to_string(),
    )
}
fn prepare(
    database: &Database,
    cache: &Path,
    mint: &str,
    start: u64,
    telemetry: &mut Telemetry,
) -> Result<Plan> {
    crate::backfill::scope(mint, start, start)?;
    let db = BridgeLog::open(database)?;
    if let Some(plan) = load(&db, mint, start)? {
        return Ok(plan);
    }
    let mut snapshot = archive::tip(cache, telemetry)?;
    snapshot["measured_at_unix_seconds"] = json!(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error("system_clock_invalid"))?
            .as_secs()
    );
    let plan = Plan {
        mint: mint.into(),
        start,
        tip: snapshot["last_observed_archive_slot"]
            .as_u64()
            .ok_or(Error("archive_tip_unknown"))?,
        end: snapshot["reference_slot"]
            .as_u64()
            .ok_or(Error("invalid_rpc_reference"))?,
        snapshot,
    };
    save(&db, &plan)?;
    load(&db, mint, start)?.ok_or(Error("bridge_plan_missing"))
}
#[derive(Debug, Serialize)]
struct Interval {
    start: u64,
    end: u64,
    source_kind: &'static str,
}
#[derive(Serialize)]
struct Progress {
    next_pending_slot: Option<u64>,
    contiguous_through: Option<u64>,
    complete: bool,
    pending_slots: u64,
    missing_ranges: u64,
    unfinished: Vec<Interval>,
    ranges_truncated: bool,
    overlap_verified: bool,
    overlap_recovered: bool,
    boundary_ready: bool,
    oldest_pending_age_seconds: Option<u64>,
}
fn progress(db: &BridgeLog, plan: &Plan) -> Result<Progress> {
    // Tie overlap proof to the exact durable attempt generation; retries invalidate it.
    let proof = db.overlap_proven(&plan.mint, plan.tip)?;
    let completed = |source: &str| db.source_complete(&plan.mint, plan.tip, source);
    let archive_complete = completed(archive::SOURCE)?;
    let overlap = proof && archive_complete && completed("finalized-mainnet-rpc")?;
    let recovered = proof && archive_complete && !overlap && completed(super::evidence::SOURCE)?;
    let ready = overlap || recovered;
    // The immutable identity ledger protects anchors; capture reconciliation
    // protects execution context. The boundary worker compares before either ack.
    let rows = db.verified_slots(&plan.mint, plan.start, plan.end, plan.tip)?;
    let cursor = db.next_slot(&plan.mint, plan.start)?;
    let cursor = u64::try_from(cursor).map_err(|_| Error("invalid_bridge_plan"))?;
    if cursor < plan.start || cursor > plan.end + 1 {
        return Err(Error("invalid_bridge_plan"));
    }
    let mut result = Progress {
        next_pending_slot: None,
        contiguous_through: None,
        complete: false,
        pending_slots: 0,
        missing_ranges: 0,
        unfinished: vec![],
        ranges_truncated: false,
        overlap_verified: overlap,
        overlap_recovered: recovered,
        boundary_ready: ready,
        oldest_pending_age_seconds: None,
    };
    let mut next = plan.start;
    let mut gap = |start: u64, end: u64| {
        for (a, b, source) in [
            (start, end.min(plan.tip), "archive"),
            (start.max(plan.tip + 1), end, "rpc"),
        ] {
            if a > b {
                continue;
            }
            if result.next_pending_slot.is_none() && b >= cursor {
                result.next_pending_slot = Some(a.max(cursor));
            }
            result.pending_slots += b - a + 1;
            result.missing_ranges += 1;
            if result.unfinished.len() < REPORT_RANGES {
                result.unfinished.push(Interval {
                    start: a,
                    end: b,
                    source_kind: source,
                });
            } else {
                result.ranges_truncated = true;
            }
        }
    };
    for row in rows {
        let slot = u64::try_from(row).map_err(|_| Error("invalid_bridge_plan"))?;
        if slot == plan.tip && !ready {
            continue;
        }
        if next < slot {
            gap(next, slot - 1);
        }
        next = slot + 1;
    }
    if next <= plan.end {
        gap(next, plan.end);
    }
    result.next_pending_slot = result
        .next_pending_slot
        .or_else(|| result.unfinished.first().map(|r| r.start));
    result.complete = result.pending_slots == 0;
    result.contiguous_through = result
        .unfinished
        .first()
        .and_then(|r| r.start.checked_sub(1))
        .filter(|s| *s >= plan.start)
        .or_else(|| result.complete.then_some(plan.end));
    // Age includes unscheduled work from the frozen plan, not merely fetched slots.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error("system_clock_invalid"))?
        .as_secs();
    result.oldest_pending_age_seconds = if result.complete {
        Some(0)
    } else {
        plan.snapshot["measured_at_unix_seconds"]
            .as_u64()
            .and_then(|t| now.checked_sub(t))
    };
    Ok(result)
}
fn report(db: &BridgeLog, plan: &Plan) -> Result<Value> {
    Ok(
        json!({"mint":plan.mint,"decoder_version":decoder::DECODER_VERSION,"start":plan.start,"archive_tip":plan.tip,"finalized_reference":plan.end,"boundary_snapshot":plan.snapshot,"historical":progress(db,plan)?,"live_tail":{"start":plan.end+1,"end":null,"coverage":"unknown"}}),
    )
}
pub fn plan(
    database: &Database,
    cache: &Path,
    mint: &str,
    start: u64,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    let plan = prepare(database, cache, mint, start, telemetry)?;
    report(&BridgeLog::open(database)?, &plan)
}
pub fn status(database: &Database, mint: &str, start: u64) -> Result<Value> {
    crate::backfill::scope(mint, start, start)?;
    let db = BridgeLog::open(database)?;
    report(
        &db,
        &load(&db, mint, start)?.ok_or(Error("bridge_plan_missing"))?,
    )
}

fn batch<A: BlockSource, R: BlockSource>(
    database: &Database,
    plan: &Plan,
    archive: &mut A,
    rpc: &mut R,
    gate: Arc<WriteGate>,
    telemetry: &mut Telemetry,
    cancelled: impl Fn() -> bool,
) -> Result<Value> {
    let db = BridgeLog::open(database)?;
    let before = progress(&db, plan)?;
    let mut history = History::open(database)?;
    history.set_gate(Arc::clone(&gate));
    let mut attempts: Vec<Value> = vec![];
    if let Some(start) = before.next_pending_slot.filter(|_| !cancelled()) {
        let end = (start + BATCH_SLOTS - 1).min(plan.end);
        history.verify_parent_gaps(&plan.mint, start, end)?;
        if start <= plan.tip {
            let archive_end = end.min(plan.tip.saturating_sub(1));
            if start <= archive_end {
                let (_, metrics) =
                    history.run(&plan.mint, start, archive_end, archive, &cancelled)?;
                attempts.push(json!({"source_kind":"archive","metrics":metrics}));
            }
        }
        if start <= plan.tip && plan.tip <= end && !cancelled() {
            let verified = boundary(&mut history, plan, archive, rpc, &cancelled);
            match verified {
                Ok(metrics) => {
                    attempts.push(json!({"source_kind":"archive_rpc_overlap","metrics":metrics}))
                }
                Err(error) => attempts
                    .push(json!({"source_kind":"archive_rpc_overlap","error_class":error.0})),
            }
        }
        let rpc_start = start.max(plan.tip + 1);
        if rpc_start <= end && !cancelled() {
            let (_, metrics) = history.run(&plan.mint, rpc_start, end, rpc, &cancelled)?;
            attempts.push(json!({"source_kind":"rpc","metrics":metrics}));
        }
        history.verify_parent_gaps(&plan.mint, start, end)?;
        {
            let _permit = gate.historical()?;
            db.advance(&plan.mint, plan.start, end + 1)?;
        }
    }
    let mut output = report(&db, plan)?;
    output["run_id"] = json!(telemetry.run_id);
    output["attempts"] = json!(attempts);
    output["cancelled"] = json!(cancelled());
    output["source_diagnostics"] = json!({"archive":archive.diagnostics(),"rpc":rpc.diagnostics()});
    output["summary"] = crate::storage::Store::summary(database, &plan.mint)?;
    output["batch_limits"] = json!({"unique_slots":BATCH_SLOTS,"archive_response_bytes":128*1024*1024,"archive_requests":32,"rpc_block_responses":9,"rpc_block_response_bytes_each":crate::backfill::MAX_BLOCK_BYTES,"request_seconds":20});
    telemetry.set_context("coordinator", "archive_rpc", "historical");
    let mut event = telemetry.event(
        "bridge_batch_finished",
        "bridge",
        if output["historical"]["complete"] == true {
            "success"
        } else {
            "incomplete"
        },
        None,
    );
    event["mint"] = json!(plan.mint);
    event["start"] = json!(plan.start);
    event["end"] = json!(plan.end);
    event["archive_tip"] = json!(plan.tip);
    event["pending_slots"] = output["historical"]["pending_slots"].clone();
    use std::io::Write;
    let _ = writeln!(std::io::stderr().lock(), "{event}");
    Ok(output)
}
fn boundary<A: BlockSource, R: BlockSource>(
    history: &mut History,
    plan: &Plan,
    archive: &mut A,
    rpc: &mut R,
    cancelled: &impl Fn() -> bool,
) -> Result<Vec<Metrics>> {
    history.reverify(&plan.mint, plan.tip, plan.tip)?;
    let a = archive.block(plan.tip)?;
    if cancelled() {
        return Err(Error("cancelled"));
    }
    let b = rpc.block(plan.tip)?;
    let result = super::evidence::ingest(history, &plan.mint, plan.tip, a, b, cancelled)?;
    if !result.ready() {
        return Err(Error("bridge_boundary_incomplete"));
    }
    Ok(result.metrics)
}

pub fn run(
    database: &Database,
    cache: &Path,
    mint: &str,
    start: u64,
    live_seconds: Option<u64>,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    if let Some(seconds) = live_seconds {
        if mint != crate::live::USDC {
            return Err(Error("invalid_live_mint"));
        }
        crate::live::validate_options(start, seconds)?;
    }
    crate::backfill::scope(mint, start, start)?;
    let plan =
        load(&BridgeLog::open(database)?, mint, start)?.ok_or(Error("bridge_plan_missing"))?;
    let job = |gate: Arc<WriteGate>| {
        let began = Instant::now();
        let cancel = std::env::var_os("BACKFILL_CANCEL_FILE");
        let duration = Duration::from_secs(live_seconds.unwrap_or(120).min(120));
        let mut archive = archive::ArchiveSource::new(
            cache,
            true,
            archive::Limits {
                seconds: duration.as_secs(),
                ..archive::Limits::default()
            },
        )?;
        let mut rpc = RpcSource::bounded(
            std::env::var("SOLANA_RPC_URL")
                .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into()),
            began + duration,
        )?;
        let mut context = Telemetry::default();
        batch(
            database,
            &plan,
            &mut archive,
            &mut rpc,
            gate,
            &mut context,
            || {
                began.elapsed() >= Duration::from_secs(live_seconds.unwrap_or(120).min(120))
                    || cancel.as_ref().is_some_and(|p| Path::new(p).exists())
            },
        )
    };
    if let Some(seconds) = live_seconds {
        super::run_with_job(database, plan.end, seconds, telemetry, job)
    } else {
        job(Arc::new(WriteGate::default()))
    }
}
#[cfg(test)]
mod tests;
