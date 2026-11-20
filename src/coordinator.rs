//! Two bounded workers share a write admission gate; live work has priority.
use crate::storage::Database;
#[cfg(feature = "archive")]
pub mod bridge;
#[cfg(feature = "archive")]
pub(crate) mod evidence;
use crate::{
    Error, Result,
    backfill::{BlockSource, History, RpcSource},
    live,
    storage::Store,
    telemetry::Telemetry,
};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

#[derive(Default)]
struct State {
    /// Writes holding a permit; at most `WriteGate::limit`.
    active: usize,
    live_started: bool,
    live_finished: bool,
    live_waiting: usize,
    history_waiting: usize,
    live_since: Option<Instant>,
    history_since: Option<Instant>,
    live_admissions: u64,
    history_admissions: u64,
    history_timeouts: u64,
    live_wait_seconds: f64,
    history_wait_seconds: f64,
}
pub struct WriteGate {
    state: Mutex<State>,
    changed: Condvar,
    limit: usize,
}
/// One permit per configured historical worker (`INDEXER_HISTORICAL_WORKERS`, default 1),
/// so a single worker keeps the one-write-at-a-time schedule.
impl Default for WriteGate {
    fn default() -> Self {
        Self::with_limit(crate::backfill::historical_workers().unwrap_or(1))
    }
}
pub struct Permit<'a> {
    gate: &'a WriteGate,
}
impl WriteGate {
    pub fn with_limit(limit: usize) -> Self {
        Self {
            state: Mutex::default(),
            changed: Condvar::new(),
            limit: limit.max(1),
        }
    }
    pub fn live(&self) -> Result<Permit<'_>> {
        self.enter(true, None)
    }
    pub fn historical(&self) -> Result<Permit<'_>> {
        self.enter(false, None)
    }
    pub fn historical_until(&self, deadline: Instant) -> Result<Permit<'_>> {
        if deadline <= Instant::now() {
            return Err(Error("historical_admission_timeout"));
        }
        self.enter(false, Some(deadline))
    }
    pub(crate) fn live_started(&self) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| Error("scheduling_failed"))?
            .live_started = true;
        self.changed.notify_all();
        Ok(())
    }
    pub(crate) fn wait_live_start(&self) -> Result<bool> {
        let state = self.state.lock().map_err(|_| Error("scheduling_failed"))?;
        Ok(self
            .changed
            .wait_timeout_while(state, Duration::from_secs(5), |s| !s.live_started)
            .map_err(|_| Error("scheduling_failed"))?
            .0
            .live_started)
    }
    pub(crate) fn live_finished(&self) -> Result<()> {
        self.state
            .lock()
            .map_err(|_| Error("scheduling_failed"))?
            .live_finished = true;
        self.changed.notify_all();
        Ok(())
    }
    /// Whether the live worker has finished, waiting up to `timeout` for it to.
    pub(crate) fn wait_live_finish(&self, timeout: Duration) -> Result<bool> {
        let state = self.state.lock().map_err(|_| Error("scheduling_failed"))?;
        Ok(self
            .changed
            .wait_timeout_while(state, timeout, |s| !s.live_finished)
            .map_err(|_| Error("scheduling_failed"))?
            .0
            .live_finished)
    }
    fn enter(&self, live: bool, deadline: Option<Instant>) -> Result<Permit<'_>> {
        let began = Instant::now();
        let mut state = self.state.lock().map_err(|_| Error("scheduling_failed"))?;
        if live {
            state.live_waiting += 1;
            state.live_since.get_or_insert(began);
        } else {
            state.history_waiting += 1;
            state.history_since.get_or_insert(began);
        }
        while state.active >= self.limit || (!live && state.live_waiting > 0) {
            if let Some(deadline) = deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    state.history_waiting -= 1;
                    state.history_timeouts += 1;
                    state.history_wait_seconds += began.elapsed().as_secs_f64();
                    if state.history_waiting == 0 {
                        state.history_since = None;
                    }
                    return Err(Error("historical_admission_timeout"));
                }
                state = self
                    .changed
                    .wait_timeout(state, remaining)
                    .map_err(|_| Error("scheduling_failed"))?
                    .0;
            } else {
                state = self
                    .changed
                    .wait(state)
                    .map_err(|_| Error("scheduling_failed"))?;
            }
        }
        if !live && deadline.is_some_and(|t| t <= Instant::now()) {
            state.history_waiting -= 1;
            state.history_timeouts += 1;
            state.history_wait_seconds += began.elapsed().as_secs_f64();
            if state.history_waiting == 0 {
                state.history_since = None;
            }
            return Err(Error("historical_admission_timeout"));
        }
        if live {
            state.live_waiting -= 1;
            if state.live_waiting == 0 {
                state.live_since = None;
            }
            state.live_admissions += 1;
            state.live_wait_seconds += began.elapsed().as_secs_f64();
        } else {
            state.history_waiting -= 1;
            if state.history_waiting == 0 {
                state.history_since = None;
            }
            state.history_admissions += 1;
            state.history_wait_seconds += began.elapsed().as_secs_f64();
        }
        state.active += 1;
        Ok(Permit { gate: self })
    }
    pub fn snapshot(&self) -> Result<Value> {
        let s = self.state.lock().map_err(|_| Error("scheduling_failed"))?;
        Ok(
            json!({"active":s.active > 0,"active_writes":s.active,"write_limit":self.limit,"live_waiting":s.live_waiting,"historical_waiting":s.history_waiting,
            "oldest_pending_age_seconds":s.live_since.into_iter().chain(s.history_since).map(|t|t.elapsed().as_secs_f64()).reduce(f64::max).unwrap_or(0.0),
            "historical_admission_timeouts":s.history_timeouts,"live_admissions":s.live_admissions,"historical_admissions":s.history_admissions,
            "live_wait_seconds_sum":s.live_wait_seconds,"historical_wait_seconds_sum":s.history_wait_seconds}),
        )
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.active -= 1;
            self.gate.changed.notify_all();
        }
    }
}

/// Opt-in bounded validation, not a production scheduler or a full-window coverage claim.
pub fn run(
    database: &Database,
    start: u64,
    end: u64,
    seconds: u64,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    telemetry.set_context("coordinator", "combined", "validation");
    crate::backfill::scope(live::USDC, start, end)?;
    // History here is bounded by the same seconds, so a continuous run is not allowed.
    if seconds == live::CONTINUOUS {
        return Err(Error("invalid_live_bounds"));
    }
    live::validate_options(start, seconds)?;
    let endpoint = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());
    run_with_source(
        database,
        start,
        end,
        seconds,
        telemetry,
        RpcSource::new(endpoint)?,
    )
}

pub(crate) fn run_with_source<S: BlockSource + Send>(
    database: &Database,
    start: u64,
    end: u64,
    seconds: u64,
    telemetry: &mut Telemetry,
    mut source: S,
) -> Result<Value> {
    crate::backfill::scope(live::USDC, start, end)?;
    if seconds == live::CONTINUOUS {
        return Err(Error("invalid_live_bounds"));
    }
    live::validate_options(start, seconds)?;
    run_with_job(database, start, seconds, telemetry, |gate| {
        let mut history = History::open(database)?;
        history.set_gate(gate);
        let began = Instant::now();
        let cancel = std::env::var_os("BACKFILL_CANCEL_FILE");
        let (coverage, metrics) = history.run(live::USDC, start, end, &mut source, || {
            began.elapsed() >= Duration::from_secs(seconds)
                || cancel.as_ref().is_some_and(|p| Path::new(p).exists())
        })?;
        Ok(json!({"run_id":history.run_id(),"coverage":coverage,"metrics":metrics}))
    })
}

pub struct MonitorConfig<'a> {
    pub mint: &'a str,
    pub live_from: u64,
    pub start: u64,
    pub end: u64,
    pub seconds: u64,
}

/// Configurable bounded live intake and a separate historical range, sharing admission.
pub fn monitor(
    database: &Database,
    config: MonitorConfig<'_>,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    crate::backfill::scope(config.mint, config.start, config.end)?;
    crate::backfill::scope(config.mint, config.live_from, config.live_from)?;
    if config.seconds == live::CONTINUOUS {
        return Err(Error("invalid_live_bounds"));
    }
    live::validate_options(config.live_from, config.seconds)?;
    let endpoint = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());
    let mut source = RpcSource::new(endpoint)?;
    telemetry.set_context("coordinator", "combined", "validation");
    run_with_mint_job(
        database,
        config.live_from,
        config.seconds,
        config.mint,
        telemetry,
        |gate| {
            let mut history = History::open(database)?;
            history.set_gate(gate);
            let began = Instant::now();
            let cancel = std::env::var_os("BACKFILL_CANCEL_FILE");
            let (coverage, metrics) =
                history.run(config.mint, config.start, config.end, &mut source, || {
                    began.elapsed() >= Duration::from_secs(config.seconds)
                        || cancel.as_ref().is_some_and(|p| Path::new(p).exists())
                })?;
            Ok(json!({"run_id":history.run_id(),"coverage":coverage,"metrics":metrics}))
        },
    )
}

pub(crate) fn run_with_job(
    database: &Database,
    live_from: u64,
    seconds: u64,
    telemetry: &mut Telemetry,
    job: impl FnOnce(Arc<WriteGate>) -> Result<Value> + Send,
) -> Result<Value> {
    run_with_mint_job(database, live_from, seconds, live::USDC, telemetry, job)
}

pub(crate) fn run_with_mint_job(
    database: &Database,
    live_from: u64,
    seconds: u64,
    mint: &str,
    telemetry: &mut Telemetry,
    job: impl FnOnce(Arc<WriteGate>) -> Result<Value> + Send,
) -> Result<Value> {
    live::validate_options(live_from, seconds)?;
    let gate = Arc::new(WriteGate::default());
    let began = Instant::now();
    let parent_id = telemetry.run_id.clone();
    let (live_result, history_result) = paired_workers(
        &gate,
        move |live_gate| {
            let mut context = Telemetry::default();
            context.run_id = format!("{parent_id}:live");
            live::run_mint_admitted(database, live_from, seconds, mint, &mut context, &live_gate)
        },
        job,
    );
    let wrap = |result: Result<Value>| match result {
        Ok(v) => json!({"outcome":"reported","report":v}),
        Err(e) => json!({"outcome":"failed","error_class":e.0}),
    };
    let history_id = history_result
        .as_ref()
        .ok()
        .and_then(|v| v.get("run_id"))
        .cloned();
    let failed = live_result.is_err() || history_result.is_err();
    telemetry.emit(
        "workers_finished",
        "overlap",
        if failed { "failed" } else { "success" },
        if failed {
            Some("source_incomplete")
        } else {
            None
        },
    );
    Ok(
        json!({"run_id":telemetry.run_id,"historical_run_id":history_id,"live":wrap(live_result),"historical":wrap(history_result),"writes":gate.snapshot()?,
        "summary":Store::summary(database,mint)?,"coverage":"inspect_scoped_historical_report","elapsed_seconds":began.elapsed().as_secs_f64(),
        "note":"reported workers do not imply complete coverage; stream tail remains unverified"}),
    )
}

fn paired_workers(
    gate: &Arc<WriteGate>,
    live: impl FnOnce(Arc<WriteGate>) -> Result<Value> + Send,
    history: impl FnOnce(Arc<WriteGate>) -> Result<Value> + Send,
) -> (Result<Value>, Result<Value>) {
    std::thread::scope(|threads| {
        let live_gate = Arc::clone(gate);
        let live_worker = threads.spawn(move || {
            let result = live(Arc::clone(&live_gate));
            // A failed live startup must not prevent independent historical repair.
            let _ = live_gate.live_started();
            let _ = live_gate.live_finished();
            result
        });
        let historical_gate = Arc::clone(gate);
        let historical_worker = threads.spawn(move || {
            historical_gate.wait_live_start()?;
            history(historical_gate)
        });
        (
            live_worker
                .join()
                .unwrap_or(Err(Error("live_worker_failed"))),
            historical_worker
                .join()
                .unwrap_or(Err(Error("historical_worker_failed"))),
        )
    })
}

#[cfg(test)]
mod replay_gate_tests {
    use super::*;
    #[test]
    fn replay_expiry_and_provider_outage_cannot_stop_the_independent_live_worker() {
        use crate::storage::{pg::TestDatabase, replay::JobLog};
        for failure in ["outside_replay_window", "provider_unavailable"] {
            let db = TestDatabase::new();
            let mut jobs = JobLog::open(&db.0).unwrap();
            jobs.create("recent", live::USDC, 100, 149).unwrap();
            let gate = Arc::new(WriteGate::default());
            let (failed, observed) = std::sync::mpsc::channel();
            let database = &db.0;
            let (live_result, history) = paired_workers(
                &gate,
                move |gate| {
                    let mut intake = live::Intake::open(database)?;
                    gate.live_started()?;
                    // Prove live writes continue after historical failure, independent of thread order.
                    observed.recv_timeout(Duration::from_secs(10)).unwrap();
                    for signature in [9u8, 10] {
                        let _permit = gate.live()?;
                        let mut update = live::tests::update();
                        let tx = update.transaction.as_mut().unwrap();
                        tx.signature = vec![signature; 64];
                        tx.transaction.as_mut().unwrap().signatures = vec![vec![signature; 64]];
                        let raw = intake.retain(&update)?;
                        intake.process(raw, &update)?;
                    }
                    Ok(json!({"received":2}))
                },
                |gate| {
                    let _permit = gate.historical()?;
                    let scope = JobLog::open(&db.0)?.scope("recent")?;
                    let mut attempt = JobLog::reserve(&db.0, "recent", 60)?;
                    let error = if failure == "outside_replay_window" {
                        attempt.availability(&scope, Some(101), 150).unwrap_err()
                    } else {
                        attempt.connection()?;
                        Error("provider_unavailable")
                    };
                    attempt.finish(error.0)?;
                    failed.send(()).unwrap();
                    Err(error)
                },
            );
            assert_eq!(live_result.unwrap()["received"], 2);
            assert_eq!(history.unwrap_err().0, failure);
            assert_eq!(Store::query(&db.0, Some(live::USDC), 100).unwrap().len(), 2);
            let status = jobs.status("recent").unwrap();
            assert_eq!(status["attempts"][0]["status"], failure);
            assert_eq!(status["coverage"], "unknown");
        }
    }

    #[test]
    fn waiting_history_can_time_out_without_leaking_admission_state() {
        let gate = Arc::new(WriteGate::default());
        let active = gate.live().unwrap();
        let history = Arc::clone(&gate);
        let worker = std::thread::spawn(move || {
            matches!(
                history.historical_until(Instant::now() + Duration::from_millis(30)),
                Err(Error("historical_admission_timeout"))
            )
        });
        assert!(worker.join().unwrap());
        assert_eq!(gate.snapshot().unwrap()["historical_waiting"], 0);
        drop(active);
        assert!(gate.live().is_ok());
    }
    #[test]
    fn history_can_start_after_live_initialization_without_waiting_for_its_cycle() {
        let gate = Arc::new(WriteGate::default());
        let live = Arc::clone(&gate);
        let (ready, rx) = std::sync::mpsc::channel();
        let (stop, finish) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            live.live_started().unwrap();
            ready.send(()).unwrap();
            finish.recv().unwrap();
        });
        rx.recv().unwrap();
        assert!(gate.wait_live_start().unwrap());
        assert!(gate.historical().is_ok()); // live worker is still running.
        stop.send(()).unwrap();
        worker.join().unwrap();
    }
}
