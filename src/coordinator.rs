//! Two bounded workers share a write admission gate; live work has priority.
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
    active: bool,
    live_waiting: usize,
    history_waiting: usize,
    live_since: Option<Instant>,
    history_since: Option<Instant>,
    live_admissions: u64,
    history_admissions: u64,
    live_wait_seconds: f64,
    history_wait_seconds: f64,
}
#[derive(Default)]
pub struct WriteGate {
    state: Mutex<State>,
    changed: Condvar,
}
pub struct Permit<'a> {
    gate: &'a WriteGate,
}
impl WriteGate {
    pub fn live(&self) -> Result<Permit<'_>> {
        self.enter(true)
    }
    pub fn historical(&self) -> Result<Permit<'_>> {
        self.enter(false)
    }
    fn enter(&self, live: bool) -> Result<Permit<'_>> {
        let began = Instant::now();
        let mut state = self.state.lock().map_err(|_| Error("scheduling_failed"))?;
        if live {
            state.live_waiting += 1;
            state.live_since.get_or_insert(began);
        } else {
            state.history_waiting += 1;
            state.history_since.get_or_insert(began);
        }
        while state.active || (!live && state.live_waiting > 0) {
            state = self
                .changed
                .wait(state)
                .map_err(|_| Error("scheduling_failed"))?;
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
        state.active = true;
        Ok(Permit { gate: self })
    }
    pub fn snapshot(&self) -> Result<Value> {
        let s = self.state.lock().map_err(|_| Error("scheduling_failed"))?;
        Ok(
            json!({"active":s.active,"live_waiting":s.live_waiting,"historical_waiting":s.history_waiting,
            "oldest_pending_age_seconds":s.live_since.into_iter().chain(s.history_since).map(|t|t.elapsed().as_secs_f64()).reduce(f64::max).unwrap_or(0.0),
            "live_admissions":s.live_admissions,"historical_admissions":s.history_admissions,
            "live_wait_seconds_sum":s.live_wait_seconds,"historical_wait_seconds_sum":s.history_wait_seconds}),
        )
    }
}
impl Drop for Permit<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.gate.state.lock() {
            state.active = false;
            self.gate.changed.notify_all();
        }
    }
}

/// Opt-in bounded validation, not a production scheduler or a full-window coverage claim.
pub fn run(
    path: &Path,
    start: u64,
    end: u64,
    seconds: u64,
    telemetry: &mut Telemetry,
) -> Result<Value> {
    telemetry.set_context("coordinator", "combined", "validation");
    crate::backfill::scope(live::USDC, start, end)?;
    live::validate_options(start, seconds)?;
    let endpoint = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".into());
    run_with_source(
        path,
        start,
        end,
        seconds,
        telemetry,
        RpcSource::new(endpoint)?,
    )
}

pub(crate) fn run_with_source<S: BlockSource + Send>(
    path: &Path,
    start: u64,
    end: u64,
    seconds: u64,
    telemetry: &mut Telemetry,
    mut source: S,
) -> Result<Value> {
    crate::backfill::scope(live::USDC, start, end)?;
    live::validate_options(start, seconds)?;
    run_with_job(path, start, seconds, telemetry, |gate| {
        let mut history = History::open(path)?;
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

pub(crate) fn run_with_job(
    path: &Path,
    live_from: u64,
    seconds: u64,
    telemetry: &mut Telemetry,
    job: impl FnOnce(Arc<WriteGate>) -> Result<Value> + Send,
) -> Result<Value> {
    live::validate_options(live_from, seconds)?;
    let gate = Arc::new(WriteGate::default());
    let began = Instant::now();
    let parent_id = telemetry.run_id.clone();
    let (live_result, history_result) = std::thread::scope(|threads| {
        let live_gate = Arc::clone(&gate);
        let live_worker = threads.spawn(move || {
            let mut context = Telemetry::default();
            context.run_id = format!("{parent_id}:live");
            live::run_admitted(path, live_from, seconds, &mut context, &live_gate)
        });
        let history_gate = Arc::clone(&gate);
        let historical_worker = threads.spawn(move || job(history_gate));
        let live = live_worker
            .join()
            .unwrap_or(Err(Error("live_worker_failed")));
        let historical = historical_worker
            .join()
            .unwrap_or(Err(Error("historical_worker_failed")));
        (live, historical)
    });
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
        "summary":Store::summary(path,live::USDC)?,"coverage":"inspect_scoped_historical_report","elapsed_seconds":began.elapsed().as_secs_f64(),
        "note":"reported workers do not imply complete coverage; stream tail remains unverified"}),
    )
}
