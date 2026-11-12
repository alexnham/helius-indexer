//! Frozen, measured source boundaries with bounded resumable work.
use super::WriteGate;
use crate::{
    Error, Result, archive,
    backfill::{BlockSource, History, Metrics, RpcSource},
    decoder,
    telemetry::Telemetry,
};
use rusqlite::{Connection, OptionalExtension, params};
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
fn database(path: &Path) -> Result<Connection> {
    History::open(path)?;
    let db = Connection::open(path).map_err(|_| Error("storage_open_failed"))?;
    db.busy_timeout(Duration::from_secs(5))
        .map_err(|_| Error("storage_configuration_failed"))?;
    db.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS bridge_plans(mint TEXT NOT NULL,decoder TEXT NOT NULL,start INTEGER NOT NULL,archive_tip INTEGER NOT NULL,reference_slot INTEGER NOT NULL,snapshot TEXT NOT NULL,next_slot INTEGER NOT NULL,PRIMARY KEY(mint,decoder,start)); CREATE TABLE IF NOT EXISTS bridge_overlap_proofs(mint TEXT NOT NULL,decoder TEXT NOT NULL,slot INTEGER NOT NULL,verified_attempt INTEGER NOT NULL,PRIMARY KEY(mint,decoder,slot));").map_err(|_|Error("storage_write_failed"))?;
    Ok(db)
}
fn load(db: &Connection, mint: &str, start: u64) -> Result<Option<Plan>> {
    let row:Option<(i64,i64,String)>=db.query_row("SELECT archive_tip,reference_slot,substr(snapshot,1,16385) FROM bridge_plans WHERE mint=?1 AND decoder=?2 AND start=?3",params![mint,decoder::DECODER_VERSION,start as i64],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional().map_err(|_|Error("storage_read_failed"))?;
    row.map(|(tip, end, snapshot)| {
        if snapshot.len() > 16384 {
            return Err(Error("invalid_bridge_plan"));
        }
        let plan = Plan {
            mint: mint.into(),
            start,
            tip: u64::try_from(tip).map_err(|_| Error("invalid_bridge_plan"))?,
            end: u64::try_from(end).map_err(|_| Error("invalid_bridge_plan"))?,
            snapshot: serde_json::from_str(&snapshot).map_err(|_| Error("invalid_bridge_plan"))?,
        };
        plan.validate()?;
        Ok(plan)
    })
    .transpose()
}
fn save(db: &Connection, plan: &Plan) -> Result<()> {
    plan.validate()?;
    db.execute(
        "INSERT OR IGNORE INTO bridge_plans VALUES(?1,?2,?3,?4,?5,?6,?3)",
        params![
            plan.mint,
            decoder::DECODER_VERSION,
            plan.start as i64,
            plan.tip as i64,
            plan.end as i64,
            plan.snapshot.to_string()
        ],
    )
    .map_err(|_| Error("storage_write_failed"))?;
    Ok(())
}
fn prepare(
    path: &Path,
    cache: &Path,
    mint: &str,
    start: u64,
    telemetry: &mut Telemetry,
) -> Result<Plan> {
    crate::backfill::scope(mint, start, start)?;
    let db = database(path)?;
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
    oldest_pending_age_seconds: Option<u64>,
}
fn progress(db: &Connection, plan: &Plan) -> Result<Progress> {
    // Tie overlap proof to the exact durable attempt generation; retries invalidate it.
    let overlap:bool=db.query_row("SELECT COUNT(DISTINCT source_kind)=2 FROM historical_source_slots s JOIN historical_slots h USING(mint,decoder,slot) WHERE s.mint=?1 AND s.decoder=?2 AND s.slot=?3 AND h.status='complete' AND EXISTS(SELECT 1 FROM bridge_overlap_proofs p WHERE p.mint=h.mint AND p.decoder=h.decoder AND p.slot=h.slot AND p.verified_attempt=h.attempts) AND s.status='complete' AND s.source_kind IN ('old-faithful-jetstreamer','finalized-mainnet-rpc')",params![plan.mint,decoder::DECODER_VERSION,plan.tip as i64],|r|r.get(0)).map_err(|_|Error("storage_read_failed"))?;
    // The immutable identity ledger protects anchors; capture reconciliation
    // protects execution context. The boundary worker compares before either ack.
    let mut query=db.prepare("SELECT h.slot FROM historical_slots h WHERE h.mint=?1 AND h.decoder=?2 AND h.slot BETWEEN ?3 AND ?4 AND (h.status='skipped' OR (h.status='complete' AND EXISTS(SELECT 1 FROM historical_source_slots s WHERE s.mint=h.mint AND s.decoder=h.decoder AND s.slot=h.slot AND s.status='complete' AND s.source_kind=CASE WHEN h.slot<=?5 THEN 'old-faithful-jetstreamer' ELSE 'finalized-mainnet-rpc' END))) ORDER BY h.slot").map_err(|_|Error("storage_read_failed"))?;
    let rows = query
        .query_map(
            params![
                plan.mint,
                decoder::DECODER_VERSION,
                plan.start as i64,
                plan.end as i64,
                plan.tip as i64
            ],
            |r| r.get::<_, i64>(0),
        )
        .map_err(|_| Error("storage_read_failed"))?;
    let cursor: i64 = db
        .query_row(
            "SELECT next_slot FROM bridge_plans WHERE mint=?1 AND decoder=?2 AND start=?3",
            params![plan.mint, decoder::DECODER_VERSION, plan.start as i64],
            |r| r.get(0),
        )
        .map_err(|_| Error("storage_read_failed"))?;
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
        let slot = u64::try_from(row.map_err(|_| Error("storage_read_failed"))?)
            .map_err(|_| Error("invalid_bridge_plan"))?;
        if slot == plan.tip && !overlap {
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
fn report(db: &Connection, plan: &Plan) -> Result<Value> {
    Ok(
        json!({"mint":plan.mint,"decoder_version":decoder::DECODER_VERSION,"start":plan.start,"archive_tip":plan.tip,"finalized_reference":plan.end,"boundary_snapshot":plan.snapshot,"historical":progress(db,plan)?,"live_tail":{"start":plan.end+1,"end":null,"coverage":"unknown"}}),
    )
}
pub fn plan(
    path: &Path,
    cache: &Path,
    mint: &str,
    start: u64,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    let plan = prepare(path, cache, mint, start, telemetry)?;
    report(&database(path)?, &plan)
}
pub fn status(path: &Path, mint: &str, start: u64) -> Result<Value> {
    crate::backfill::scope(mint, start, start)?;
    let db = database(path)?;
    report(
        &db,
        &load(&db, mint, start)?.ok_or(Error("bridge_plan_missing"))?,
    )
}

fn batch<A: BlockSource, R: BlockSource>(
    path: &Path,
    plan: &Plan,
    archive: &mut A,
    rpc: &mut R,
    gate: Arc<WriteGate>,
    telemetry: &mut Telemetry,
    cancelled: impl Fn() -> bool,
) -> Result<Value> {
    let db = database(path)?;
    let before = progress(&db, plan)?;
    let mut history = History::open(path)?;
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
            db.execute(
                "UPDATE bridge_plans SET next_slot=?1 WHERE mint=?2 AND decoder=?3 AND start=?4",
                params![
                    end as i64 + 1,
                    plan.mint,
                    decoder::DECODER_VERSION,
                    plan.start as i64
                ],
            )
            .map_err(|_| Error("storage_write_failed"))?;
        }
    }
    let mut output = report(&db, plan)?;
    output["run_id"] = json!(telemetry.run_id);
    output["attempts"] = json!(attempts);
    output["cancelled"] = json!(cancelled());
    output["source_diagnostics"] = json!({"archive":archive.diagnostics(),"rpc":rpc.diagnostics()});
    output["summary"] = crate::storage::Store::summary(path, &plan.mint)?;
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
    // Invalidate first: a failed comparison or crash must not preserve readiness.
    history.reverify(&plan.mint, plan.tip, plan.tip)?;
    let a = archive.block(plan.tip)?;
    if cancelled() {
        return Err(Error("cancelled"));
    }
    let b = rpc.block(plan.tip)?;
    archive::compare_blocks(&a, &b, plan.tip)?;
    let mut outcomes = vec![];
    for (raw, is_archive) in [(a, true), (b, false)] {
        history.reverify(&plan.mint, plan.tip, plan.tip)?;
        let mut source = archive::Observation {
            slot: plan.tip,
            raw,
            archive: is_archive,
        };
        let (coverage, metrics) =
            history.run(&plan.mint, plan.tip, plan.tip, &mut source, cancelled)?;
        outcomes.push(metrics);
        if !coverage.complete {
            return Err(Error("bridge_boundary_incomplete"));
        }
    }
    history.acknowledge_bridge_overlap(&plan.mint, plan.tip)?;
    Ok(outcomes)
}
pub fn run(
    path: &Path,
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
    let plan = load(&database(path)?, mint, start)?.ok_or(Error("bridge_plan_missing"))?;
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
            path,
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
        super::run_with_job(path, plan.end, seconds, telemetry, job)
    } else {
        job(Arc::new(WriteGate::default()))
    }
}
#[cfg(test)]
mod tests;
