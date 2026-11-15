//! Bounded finalized block ingestion. Coverage is a separate durable acknowledgment.
use crate::storage::Database;
use crate::{
    Error, Result, decoder,
    storage::{Store, history::HistoryLog},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    io::Read,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub const MAX_SLOTS: u64 = 128;
pub const MAX_BLOCK_BYTES: usize = 64 * 1024 * 1024;

/// Implementations must request full, finalized mainnet blocks; errors are never skip evidence.
pub trait BlockSource {
    fn provenance(&self) -> &'static str {
        "finalized-mainnet-rpc"
    }
    fn diagnostics(&self) -> Value {
        Value::Null
    }
    fn capture_source(&self) -> &'static str {
        "solana-mainnet-rpc"
    }
    fn block(&mut self, slot: u64) -> Result<Vec<u8>>;
}

#[derive(Default)]
struct RpcDiagnostics {
    requests: u64,
    request_failures: u64,
    read_failures: u64,
    oversized_responses: u64,
    response_body_bytes_read: u64,
}
fn read_rpc_body(
    reader: impl Read,
    max: usize,
    too_large: Error,
    stats: &mut RpcDiagnostics,
) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let read = reader.take(max as u64 + 1).read_to_end(&mut bytes);
    stats.response_body_bytes_read += bytes.len() as u64;
    if read.is_err() {
        stats.read_failures += 1;
        return Err(Error("rpc_read_failed"));
    }
    if bytes.len() > max {
        stats.oversized_responses += 1;
        return Err(too_large);
    }
    Ok(bytes)
}

pub struct RpcSource {
    endpoint: String,
    agent: ureq::Agent,
    verified: bool,
    deadline: Option<Instant>,
    diagnostics: RpcDiagnostics,
}
impl RpcSource {
    pub fn new(endpoint: String) -> Result<Self> {
        // No endpoints enter persisted provenance, diagnostics, or error strings.
        if !endpoint.starts_with("https://") {
            return Err(Error("rpc_https_required"));
        }
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(20)))
            .max_redirects(0)
            .build()
            .into();
        Ok(Self {
            endpoint,
            agent,
            verified: false,
            deadline: None,
            diagnostics: RpcDiagnostics::default(),
        })
    }
    #[cfg(feature = "archive")]
    pub(crate) fn bounded(endpoint: String, deadline: Instant) -> Result<Self> {
        let mut source = Self::new(endpoint)?;
        source.deadline = Some(deadline);
        Ok(source)
    }
    fn request_timeout(&self) -> Result<Duration> {
        let timeout = self
            .deadline
            .map(|t| t.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(20))
            .min(Duration::from_secs(20));
        if timeout.is_zero() {
            return Err(Error("rpc_time_limit"));
        }
        Ok(timeout)
    }
    fn request_raw(&mut self, payload: Value, max: usize, too_large: Error) -> Result<Vec<u8>> {
        let timeout = self.request_timeout()?;
        self.diagnostics.requests += 1;
        let response = self
            .agent
            .post(&self.endpoint)
            .config()
            .timeout_global(Some(timeout))
            .build()
            .send_json(payload);
        let mut response = match response {
            Ok(response) => response,
            Err(_) => {
                self.diagnostics.request_failures += 1;
                return Err(Error("rpc_request_failed"));
            }
        };
        read_rpc_body(
            response.body_mut().as_reader(),
            max,
            too_large,
            &mut self.diagnostics,
        )
    }
    fn verify_mainnet(&mut self) -> Result<()> {
        let bytes = self.request_raw(
            json!({"jsonrpc":"2.0","id":1,"method":"getGenesisHash"}),
            4096,
            Error("rpc_mainnet_required"),
        )?;
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| Error("invalid_rpc_response"))?;
        if bytes.len() > 4096
            || value["result"] != "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
            || value["id"] != 1
            || value["jsonrpc"] != "2.0"
            || value.get("error").is_some()
        {
            return Err(Error("rpc_mainnet_required"));
        }
        self.verified = true;
        Ok(())
    }
}
impl RpcSource {
    pub fn finalized_reference(&mut self) -> Result<(u64, Option<i64>)> {
        self.verify_mainnet()?;
        let mut request = |method: &str, params: Value| -> Result<Value> {
            let raw = self.request_raw(
                json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}),
                4096,
                Error("invalid_rpc_response"),
            )?;
            let value: Value =
                serde_json::from_slice(&raw).map_err(|_| Error("invalid_rpc_response"))?;
            if value["id"] != 1 || value["jsonrpc"] != "2.0" || value.get("error").is_some() {
                return Err(Error("invalid_rpc_response"));
            }
            Ok(value["result"].clone())
        };
        let slot = request("getSlot", json!([{"commitment":"finalized"}]))?
            .as_u64()
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or(Error("invalid_rpc_reference"))?;
        let time = request("getBlockTime", json!([slot]))?;
        if !time.is_null() && !time.is_i64() {
            return Err(Error("invalid_rpc_reference"));
        }
        Ok((slot, time.as_i64()))
    }
}
impl BlockSource for RpcSource {
    fn diagnostics(&self) -> Value {
        json!({"measurement":"response body bytes consumed; excludes request/headers/TLS and billing","requests":self.diagnostics.requests,"request_failures":self.diagnostics.request_failures,"read_failures":self.diagnostics.read_failures,"oversized_responses":self.diagnostics.oversized_responses,"response_body_bytes_read":self.diagnostics.response_body_bytes_read})
    }
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        if !self.verified {
            self.verify_mainnet()?;
        }
        self.request_raw(json!({"jsonrpc":"2.0","id":1,"method":"getBlock","params":[slot,{
            "commitment":"finalized","encoding":"json","transactionDetails":"full","maxSupportedTransactionVersion":1,"rewards":false}]}),MAX_BLOCK_BYTES,Error("block_too_large"))
    }
}

#[derive(Debug, Serialize)]
pub struct SlotCoverage {
    pub slot: u64,
    pub status: String,
    pub error_class: Option<String>,
    pub attempts: u64,
    pub block_time: Option<i64>,
}
/// Requested range boundaries and verified (complete or skipped) slot counts.
#[derive(Debug, Serialize)]
pub struct Progress {
    pub first_slot: u64,
    pub last_slot: u64,
    pub slots_requested: u64,
    pub slots_completed: u64,
}
#[derive(Debug, Serialize)]
pub struct Coverage {
    pub mint: String,
    pub decoder_version: &'static str,
    pub start: u64,
    pub end: u64,
    pub contiguous_through: Option<u64>,
    pub complete: bool,
    pub slots: Vec<SlotCoverage>,
    pub pending: usize,
    pub oldest_pending_age_seconds: Option<u64>,
    pub progress: Progress,
}
#[derive(Debug, Default, Serialize)]
pub struct Metrics {
    pub input_bytes: u64,
    pub archive_input: bool,
    pub corroborated_input: bool,
    pub source_diagnostics: Value,
    pub source_attempts: u64,
    pub cached_blocks: u64,
    pub source_errors: u64,
    pub write_errors: u64,
    pub retries: u64,
    pub decoded_transactions: u64,
    pub decoded_transfers: u64,
    pub decode_failures: u64,
    pub elapsed_seconds: f64,
    /// Successful decode attempts per elapsed second; not a benchmark result.
    pub transactions_per_second: f64,
    pub cancelled: bool,
    /// Writes made by this run's store; transfers written exclude identical replays.
    pub storage: crate::storage::WriteStats,
}
impl Metrics {
    pub fn prometheus(&self, coverage: &Coverage) -> String {
        let mut output = format!(
            "indexer_input_bytes_total{{source_kind=\"rpc\",mode=\"historical\"}} {}\nindexer_transactions_total{{stage=\"decode\",outcome=\"success\",mode=\"historical\"}} {}\nindexer_transfers_decoded_total{{mode=\"historical\"}} {}\nindexer_decode_failures_total{{failure_class=\"invalid_or_unsupported\",mode=\"historical\"}} {}\nindexer_retries_total{{operation=\"historical_slot\",error_class=\"unfinished\"}} {}\nindexer_historical_source_errors_total {}\nindexer_historical_write_errors_total {}\nindexer_backlog_items{{stage=\"historical_slot\",mode=\"historical\"}} {}\nindexer_coverage_valid{{mode=\"historical\"}} {}\nindexer_historical_elapsed_seconds {}\nindexer_historical_transactions_per_second {}\n{}",
            self.input_bytes,
            self.decoded_transactions,
            self.decoded_transfers,
            self.decode_failures,
            self.retries,
            self.source_errors,
            self.write_errors,
            coverage.pending,
            usize::from(coverage.complete),
            self.elapsed_seconds,
            self.transactions_per_second,
            coverage
                .oldest_pending_age_seconds
                .map_or(String::new(), |age| format!(
                    "indexer_oldest_pending_age_seconds{{mode=\"historical\"}} {age}\n"
                ))
        );
        output.push_str(&format!(
            "indexer_historical_cached_blocks_total {}\n",
            self.cached_blocks
        ));
        let progress = &coverage.progress;
        output.push_str(&format!(
            "indexer_historical_range_slots_requested{{mode=\"historical\"}} {}\nindexer_historical_range_slots_completed{{mode=\"historical\"}} {}\nindexer_historical_range_first_slot{{mode=\"historical\"}} {}\nindexer_historical_range_last_slot{{mode=\"historical\"}} {}\n",
            progress.slots_requested,
            progress.slots_completed,
            progress.first_slot,
            progress.last_slot
        ));
        let mut prior_pending = false;
        let mut missing_ranges = 0;
        for slot in &coverage.slots {
            let pending = slot.status != "complete" && slot.status != "skipped";
            if pending && !prior_pending {
                missing_ranges += 1;
            }
            prior_pending = pending;
        }
        output.push_str(&format!(
            "indexer_missing_ranges{{mode=\"historical\"}} {missing_ranges}\n"
        ));
        output.push_str(&self.storage.counter_lines("historical"));
        // One metric family must be contiguous for strict text-format parsers.
        output.push_str(&self.storage.duration_lines("historical"));
        output.push_str(&format!("indexer_stage_duration_seconds_bucket{{operation=\"historical_range\",mode=\"historical\",le=\"+Inf\"}} 1\nindexer_stage_duration_seconds_sum{{operation=\"historical_range\",mode=\"historical\"}} {}\nindexer_stage_duration_seconds_count{{operation=\"historical_range\",mode=\"historical\"}} 1\n",self.elapsed_seconds));
        if self.archive_input {
            output = output.replace("source_kind=\"rpc\"", "source_kind=\"archive\"");
        }
        if self.corroborated_input {
            output = output.replace("source_kind=\"rpc\"", "source_kind=\"corroborated\"");
        }
        output
    }
}

pub struct History {
    db: HistoryLog,
    database: Database,
    telemetry: crate::telemetry::Telemetry,
    revision_repair: bool,
    active_source: &'static str,
    gate: std::sync::Arc<crate::coordinator::WriteGate>,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
pub(crate) fn scope(mint: &str, start: u64, end: u64) -> Result<()> {
    if start > end || end > i64::MAX as u64 || end - start >= MAX_SLOTS {
        return Err(Error("invalid_historical_range"));
    }
    if mint.len() > 44
        || bs58::decode(mint)
            .into_vec()
            .map_or(true, |v| v.len() != 32)
    {
        return Err(Error("invalid_mint"));
    }
    Ok(())
}
impl History {
    pub fn open(database: &Database) -> Result<Self> {
        // Initialize the shared transaction store before adding compatible, independent tables.
        Store::open(database)?;
        let db = HistoryLog::open(database)?;
        let mut telemetry = crate::telemetry::Telemetry::default();
        telemetry.set_context("backfill", "rpc", "historical");
        Ok(Self {
            db,
            revision_repair: false,
            active_source: "finalized-mainnet-rpc",
            database: database.clone(),
            telemetry,
            gate: std::sync::Arc::new(crate::coordinator::WriteGate::default()),
        })
    }
    pub(crate) fn enable_revision_repair(&mut self, run_id: String) {
        self.revision_repair = true;
        self.telemetry.run_id = run_id;
        self.telemetry
            .set_context("decoder_repair", "retained_block", "repair");
    }
    #[cfg(feature = "archive")]
    pub(crate) fn reverify(&mut self, mint: &str, start: u64, end: u64) -> Result<()> {
        scope(mint, start, end)?;
        let _permit = self.gate.historical()?;
        self.db.require_reverification(mint, start, end)
    }
    /// Reuse verified finalized parent links across adjacent source batches.
    #[cfg(feature = "archive")]
    pub(crate) fn verify_parent_gaps(&self, mint: &str, start: u64, end: u64) -> Result<()> {
        scope(mint, start, end)?;
        for skipped in start..=end {
            if !self.db.parent_link_spans(mint, skipped)? {
                continue;
            }
            if !self.db.produced_block_observed(skipped)? {
                self.status(mint, skipped, "skipped", None, None)?;
            }
        }
        Ok(())
    }
    #[cfg(feature = "archive")]
    pub(crate) fn acknowledge_bridge_overlap(&self, mint: &str, slot: u64) -> Result<()> {
        let _permit = self.gate.historical()?;
        self.db.acknowledge_bridge_overlap(mint, slot)
    }
    #[cfg(feature = "archive")]
    pub(crate) fn retained_input(&self, mint: &str, slot: u64, source: &str) -> Result<Vec<u8>> {
        scope(mint, slot, slot)?;
        let raw = self.db.bounded_input(mint, slot, source)?;
        raw.ok_or(Error("retained_block_unavailable"))?
            .ok_or(Error("block_too_large"))
    }
    pub fn set_gate(&mut self, gate: std::sync::Arc<crate::coordinator::WriteGate>) {
        self.gate = gate;
    }
    pub fn run_id(&self) -> &str {
        &self.telemetry.run_id
    }
    pub fn coverage(&self, mint: &str, start: u64, end: u64) -> Result<Coverage> {
        scope(mint, start, end)?;
        let mut slots = Vec::new();
        let mut contiguous = Some(start);
        let mut through = None;
        let mut oldest = None;
        let mut age_unknown = false;
        let mut completed = 0;
        for slot in start..=end {
            let record = self.db.slot(mint, slot)?;
            let scheduled = record.is_some();
            let (status, error_class, attempts, block_time, first) =
                record.unwrap_or(("unknown".into(), None, 0, None, now()));
            if status == "complete" || status == "skipped" {
                completed += 1;
                if contiguous == Some(slot) {
                    through = Some(slot);
                    contiguous = slot.checked_add(1);
                }
            } else {
                contiguous = None;
                age_unknown |= !scheduled;
                oldest = Some(oldest.map_or(first, |v: u64| v.min(first)));
            }
            slots.push(SlotCoverage {
                slot,
                status,
                error_class,
                attempts,
                block_time,
            });
        }
        let pending = slots
            .iter()
            .filter(|s| s.status != "complete" && s.status != "skipped")
            .count();
        Ok(Coverage {
            mint: mint.into(),
            decoder_version: decoder::DECODER_VERSION,
            start,
            end,
            contiguous_through: through,
            complete: pending == 0,
            slots,
            pending,
            oldest_pending_age_seconds: if pending == 0 {
                Some(0)
            } else if age_unknown {
                None
            } else {
                oldest.map(|t| now().saturating_sub(t))
            },
            progress: Progress {
                first_slot: start,
                last_slot: end,
                slots_requested: end - start + 1,
                slots_completed: completed,
            },
        })
    }
    fn status(
        &self,
        mint: &str,
        slot: u64,
        status: &str,
        error: Option<&str>,
        time: Option<i64>,
    ) -> Result<()> {
        let _permit = self.gate.historical()?;
        self.db
            .set_status(mint, slot, self.active_source, status, error, time)
    }
    /// Single worker, no queue. Cancellation checked between slots and transaction writes.
    /// A request already in flight is bounded by the source's request deadline.
    pub fn run(
        &mut self,
        mint: &str,
        start: u64,
        end: u64,
        source: &mut impl BlockSource,
        cancelled: impl Fn() -> bool,
    ) -> Result<(Coverage, Metrics)> {
        scope(mint, start, end)?;
        self.active_source = source.provenance();
        for slot in start..=end {
            let _permit = self.gate.historical()?;
            self.db.schedule(mint, slot, now())?;
        }
        let started = Instant::now();
        let mut metrics = Metrics {
            archive_input: source.capture_source() == "old-faithful-jetstreamer",
            corroborated_input: source.capture_source() == "archive-rpc-corroborated",
            ..Metrics::default()
        };
        if metrics.archive_input && !self.revision_repair {
            self.telemetry
                .set_context("backfill", "archive", "historical");
        } else if metrics.corroborated_input && !self.revision_repair {
            self.telemetry
                .set_context("backfill", "corroborated", "historical");
        } else if !self.revision_repair {
            self.telemetry.set_context("backfill", "rpc", "historical");
        }
        let mut store = {
            let _permit = self.gate.historical()?;
            Store::open(&self.database)?
        };
        for slot in start..=end {
            if cancelled() {
                metrics.cancelled = true;
                break;
            }
            let current = self.coverage(mint, slot, slot)?.slots.remove(0);
            let source_done = self.db.source_done(mint, slot, self.active_source)?;
            if current.status == "skipped" || (current.status == "complete" && source_done) {
                continue;
            }
            if current.attempts > 0 {
                metrics.retries += 1;
                event(
                    &self.telemetry,
                    "retry",
                    "historical_slot",
                    "retry",
                    slot,
                    mint,
                    None,
                );
            }
            {
                let _permit = self.gate.historical()?;
                self.db.add_attempt(mint, slot)?;
            }
            let cached: Option<Vec<u8>> = if self.revision_repair
                || current.error_class.is_none()
                || current.error_class.as_deref() == Some("cancelled")
                || matches!(
                    current.error_class.as_deref(),
                    Some("decoder_revision_conflict" | "decoder_repair_requires_verification")
                )
                || current
                    .error_class
                    .as_deref()
                    .is_some_and(|e| e.starts_with("storage_"))
            {
                self.db.latest_input(mint, slot, source.provenance())?
            } else {
                None
            };
            let response = match cached {
                Some(bytes) => {
                    metrics.cached_blocks += 1;
                    Ok(bytes)
                }
                None => {
                    metrics.source_attempts += 1;
                    source.block(slot)
                }
            };
            let raw = match response {
                Ok(bytes) if bytes.len() <= MAX_BLOCK_BYTES => bytes,
                result => {
                    let error = result.err().unwrap_or(Error("block_too_large"));
                    if error.0 == "archive_cache_write_failed" {
                        metrics.write_errors += 1;
                    } else {
                        metrics.source_errors += 1;
                    }
                    self.status(mint, slot, "unknown", Some(error.0), None)?;
                    event(
                        &self.telemetry,
                        "coverage_gap",
                        "source",
                        "failed",
                        slot,
                        mint,
                        Some(error.0),
                    );
                    continue;
                }
            };
            metrics.input_bytes += raw.len() as u64;
            // Raw input acknowledgment precedes decoding. Every failed attempt remains reproducible.
            {
                let _permit = self.gate.historical()?;
                self.db.retain_input(
                    mint,
                    slot,
                    current.attempts + 1,
                    source.provenance(),
                    now(),
                    &raw,
                )?;
            }
            let failures_before = metrics.decode_failures;
            let outcome = process_block(
                &raw,
                slot,
                &mut store,
                &mut metrics,
                &cancelled,
                BlockContext {
                    gate: &self.gate,
                    revision_repair: self.revision_repair,
                    capture_source: source.capture_source(),
                },
            );
            match outcome {
                Ok((parent, time)) => {
                    // A finalized child links directly to its parent; intermediate slots are skipped.
                    // Never overwrite observed produced data with skip evidence.
                    for skipped in start.max(parent.saturating_add(1))..slot {
                        if !self.db.produced_block_observed(skipped)? {
                            self.status(mint, skipped, "skipped", None, None)?;
                        }
                    }
                    self.status(mint, slot, "complete", None, time)?;
                    event(
                        &self.telemetry,
                        "durable_range_completed",
                        "coverage",
                        "success",
                        slot,
                        mint,
                        None,
                    );
                }
                Err(error) => {
                    if error.0 == "cancelled" {
                        metrics.cancelled = true;
                    }
                    if error.0 == "rpc_block_unavailable" {
                        metrics.source_errors += 1;
                    } else if error.0.starts_with("storage_")
                        || matches!(
                            error.0,
                            "capture_conflict"
                                | "block_identity_conflict"
                                | "block_parent_conflict"
                        )
                    {
                        metrics.write_errors += 1;
                    } else if error.0 != "cancelled" && metrics.decode_failures == failures_before {
                        metrics.source_errors += 1;
                    }
                    self.status(mint, slot, "unknown", Some(error.0), None)?;
                    event(
                        &self.telemetry,
                        "coverage_gap",
                        "decode_store",
                        "failed",
                        slot,
                        mint,
                        Some(error.0),
                    );
                    if metrics.cancelled {
                        break;
                    }
                }
            }
        }
        metrics.source_diagnostics = source.diagnostics();
        metrics.storage = store.stats();
        metrics.elapsed_seconds = started.elapsed().as_secs_f64();
        metrics.transactions_per_second =
            metrics.decoded_transactions as f64 / metrics.elapsed_seconds.max(f64::EPSILON);
        let coverage = self.coverage(mint, start, end)?;
        // Only the re-read coverage (the durable state) can prove the whole range is complete.
        if !metrics.cancelled && coverage.complete {
            range_completed(&self.telemetry, mint, start, end);
        }
        Ok((coverage, metrics))
    }
}
struct BlockContext<'a> {
    gate: &'a crate::coordinator::WriteGate,
    revision_repair: bool,
    capture_source: &'a str,
}
fn process_block(
    raw: &[u8],
    slot: u64,
    store: &mut Store,
    metrics: &mut Metrics,
    cancelled: &impl Fn() -> bool,
    context: BlockContext<'_>,
) -> Result<(u64, Option<i64>)> {
    let BlockContext {
        gate,
        revision_repair,
        capture_source,
    } = context;
    let value: Value = serde_json::from_slice(raw).map_err(|_| Error("invalid_block_json"))?;
    if value["jsonrpc"] != "2.0" || value["id"] != 1 || value.get("error").is_some() {
        return Err(Error("rpc_block_unavailable"));
    }
    let block = &value["result"];
    if !block.is_object() {
        return Err(Error("rpc_block_unavailable"));
    }
    let parent = block["parentSlot"]
        .as_u64()
        .filter(|p| *p < slot)
        .ok_or(Error("missing_block_parent"))?;
    for field in ["blockhash", "previousBlockhash"] {
        let text = block[field].as_str().ok_or(Error("invalid_block_hash"))?;
        if text.len() > 44
            || bs58::decode(text)
                .into_vec()
                .map_or(true, |b| b.len() != 32)
        {
            return Err(Error("invalid_block_hash"));
        }
    }
    if !block
        .get("blockTime")
        .is_some_and(|v| v.is_null() || v.as_i64().is_some())
    {
        return Err(Error("invalid_block_time"));
    }
    let transactions = block["transactions"]
        .as_array()
        .ok_or(Error("missing_block_transactions"))?;
    if transactions.len() > 100_000 {
        return Err(Error("too_many_block_transactions"));
    }
    {
        let _permit = gate.historical()?;
        store.observe_block_identity(slot, block, transactions)?;
    }
    let mut signatures = std::collections::HashSet::new();
    let mut failure = None;
    for tx in transactions {
        if cancelled() {
            return Err(Error("cancelled"));
        }
        let mut result = tx.clone();
        let object = result
            .as_object_mut()
            .ok_or(Error("invalid_block_transaction"))?;
        object.insert("slot".into(), json!(slot));
        object.insert("blockTime".into(), block["blockTime"].clone());
        let mut capture = json!({"schema_version":1,"cluster":"mainnet-beta","commitment":"finalized","source":capture_source,"result":result});
        if let Some(provenance) = value
            .get("archive_provenance")
            .or_else(|| value.get("evidence_provenance"))
        {
            capture["source_provenance"] = provenance.clone();
        }
        let bytes = serde_json::to_vec(&capture).map_err(|_| Error("invalid_block_transaction"))?;
        let parsed = match decoder::parse_capture(&bytes) {
            Ok(c) => c,
            Err(e) => {
                metrics.decode_failures += 1;
                failure = Some(e);
                continue;
            }
        };
        if !signatures.insert(parsed.result.transaction.signatures[0].clone()) {
            return Err(Error("duplicate_block_transaction"));
        }
        let decoded = decoder::decode(&parsed);
        if decoded.is_ok() || revision_repair {
            let _permit = gate.historical()?;
            match store.persist(&parsed, &bytes, &decoded) {
                Err(e) if revision_repair && e.0 == "decoder_revision_conflict" => {}
                result => {
                    result?;
                }
            }
            if revision_repair {
                let corrected = store.correct(&parsed.result.transaction.signatures[0])?;
                if let Some(error) = corrected.error {
                    metrics.decode_failures += 1;
                    failure = Some(error);
                    continue;
                }
            }
        }
        match decoded {
            Ok(transfers) => {
                metrics.decoded_transactions += 1;
                metrics.decoded_transfers += transfers.len() as u64;
            }
            Err(e) => {
                metrics.decode_failures += 1;
                failure = Some(e);
            }
        }
    }
    if let Some(error) = failure {
        return Err(error);
    }
    Ok((parent, block["blockTime"].as_i64()))
}
fn event(
    telemetry: &crate::telemetry::Telemetry,
    name: &'static str,
    operation: &'static str,
    outcome: &'static str,
    slot: u64,
    mint: &str,
    error: Option<&'static str>,
) {
    use std::io::Write;
    let mut event = telemetry.event(name, operation, outcome, error);
    event["slot"] = json!(slot);
    event["mint"] = json!(mint);
    let _ = writeln!(std::io::stderr().lock(), "{event}");
}

/// Once per invocation, after every slot in the requested range is verified durable.
fn range_completed(telemetry: &crate::telemetry::Telemetry, mint: &str, start: u64, end: u64) {
    use std::io::Write;
    let mut event = telemetry.event("historical_range_completed", "coverage", "success", None);
    event["mint"] = json!(mint);
    event["slot_start"] = json!(start);
    event["slot_end"] = json!(end);
    let _ = writeln!(std::io::stderr().lock(), "{event}");
}

#[cfg(test)]
mod rpc_diagnostics_tests {
    use super::*;
    #[test]
    fn consumed_bodies_include_small_responses_and_oversize_sentinel() {
        let mut stats = RpcDiagnostics::default();
        assert_eq!(
            read_rpc_body(&b"genesis"[..], 4096, Error("oversized"), &mut stats).unwrap(),
            b"genesis"
        );
        assert_eq!(
            read_rpc_body(&b"abcdef"[..], 3, Error("oversized"), &mut stats)
                .unwrap_err()
                .0,
            "oversized"
        );
        assert_eq!(stats.response_body_bytes_read, 11);
        assert_eq!(stats.oversized_responses, 1);
    }
    #[test]
    fn partial_read_failures_keep_consumed_byte_accounting_and_diagnostics_are_redacted() {
        struct Partial(bool);
        impl Read for Partial {
            fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                if self.0 {
                    return Err(std::io::Error::other("private transport detail"));
                }
                self.0 = true;
                buffer[..3].copy_from_slice(b"abc");
                Ok(3)
            }
        }
        let mut source = RpcSource::new("https://example.invalid/?api-key=private".into()).unwrap();
        assert_eq!(
            read_rpc_body(
                Partial(false),
                100,
                Error("oversized"),
                &mut source.diagnostics
            )
            .unwrap_err()
            .0,
            "rpc_read_failed"
        );
        let diagnostic = source.diagnostics();
        assert_eq!(diagnostic["response_body_bytes_read"], 3);
        assert_eq!(diagnostic["read_failures"], 1);
        assert!(!diagnostic.to_string().contains("private"));
        assert!(!diagnostic.to_string().contains("example.invalid"));
    }
}
