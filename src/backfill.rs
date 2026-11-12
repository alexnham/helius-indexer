//! Bounded finalized block ingestion. Coverage is a separate durable acknowledgment.
use crate::{Error, Result, decoder, storage::Store};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    io::Read,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

pub const MAX_SLOTS: u64 = 128;
pub const MAX_BLOCK_BYTES: usize = 64 * 1024 * 1024;

/// Implementations must request full, finalized mainnet blocks; errors are never skip evidence.
pub trait BlockSource {
    fn block(&mut self, slot: u64) -> Result<Vec<u8>>;
}

pub struct RpcSource {
    endpoint: String,
    agent: ureq::Agent,
    verified: bool,
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
        })
    }
    fn verify_mainnet(&mut self) -> Result<()> {
        let mut response = self
            .agent
            .post(&self.endpoint)
            .send_json(json!({"jsonrpc":"2.0","id":1,"method":"getGenesisHash"}))
            .map_err(|_| Error("rpc_request_failed"))?;
        let mut bytes = Vec::new();
        response
            .body_mut()
            .as_reader()
            .take(4097)
            .read_to_end(&mut bytes)
            .map_err(|_| Error("rpc_read_failed"))?;
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
impl BlockSource for RpcSource {
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        if !self.verified {
            self.verify_mainnet()?;
        }
        let mut response = self
            .agent
            .post(&self.endpoint)
            .send_json(json!({
                "jsonrpc":"2.0","id":1,"method":"getBlock","params":[slot,{
                    "commitment":"finalized","encoding":"json","transactionDetails":"full",
                    "maxSupportedTransactionVersion":1,"rewards":false}]
            }))
            .map_err(|_| Error("rpc_request_failed"))?;
        let mut bytes = Vec::new();
        response
            .body_mut()
            .as_reader()
            .take(MAX_BLOCK_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| Error("rpc_read_failed"))?;
        if bytes.len() > MAX_BLOCK_BYTES {
            return Err(Error("block_too_large"));
        }
        Ok(bytes)
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
}
#[derive(Debug, Default, Serialize)]
pub struct Metrics {
    pub input_bytes: u64,
    pub source_attempts: u64,
    pub source_errors: u64,
    pub write_errors: u64,
    pub retries: u64,
    pub decoded_transactions: u64,
    pub decoded_transfers: u64,
    pub decode_failures: u64,
    pub elapsed_seconds: f64,
    pub cancelled: bool,
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
            self.decoded_transactions as f64 / self.elapsed_seconds.max(f64::EPSILON),
            coverage
                .oldest_pending_age_seconds
                .map_or(String::new(), |age| format!(
                    "indexer_oldest_pending_age_seconds{{mode=\"historical\"}} {age}\n"
                ))
        );
        let mut prior_pending = false;
        let mut missing_ranges = 0;
        for slot in &coverage.slots {
            let pending = slot.status != "complete" && slot.status != "skipped";
            if pending && !prior_pending {
                missing_ranges += 1;
            }
            prior_pending = pending;
        }
        output.push_str(&format!("indexer_missing_ranges{{mode=\"historical\"}} {missing_ranges}\nindexer_stage_duration_seconds_sum{{operation=\"historical_range\"}} {}\nindexer_stage_duration_seconds_count{{operation=\"historical_range\"}} 1\nindexer_stage_duration_seconds_bucket{{operation=\"historical_range\",le=\"+Inf\"}} 1\n",self.elapsed_seconds));
        output
    }
}

pub struct History {
    db: Connection,
    path: PathBuf,
    telemetry: crate::telemetry::Telemetry,
    gate: std::sync::Arc<crate::coordinator::WriteGate>,
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn write_error(_: rusqlite::Error) -> Error {
    Error("history_write_failed")
}

fn read_error(_: rusqlite::Error) -> Error {
    Error("history_read_failed")
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
    pub fn open(path: &Path) -> Result<Self> {
        // Initialize the shared transaction store before adding compatible, independent tables.
        Store::open(path)?;
        let db = Connection::open(path).map_err(|_| Error("history_open_failed"))?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(write_error)?;
        db.execute_batch(
            "PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS historical_slots (
                mint TEXT NOT NULL, decoder TEXT NOT NULL, slot INTEGER NOT NULL,
                status TEXT NOT NULL, error_class TEXT, first_pending INTEGER NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, block_time INTEGER,
                PRIMARY KEY(mint,decoder,slot));
            CREATE TABLE IF NOT EXISTS historical_inputs (
                mint TEXT NOT NULL, decoder TEXT NOT NULL, slot INTEGER NOT NULL,
                attempt INTEGER NOT NULL, source_kind TEXT NOT NULL, received_at INTEGER NOT NULL,
                response BLOB NOT NULL, PRIMARY KEY(mint,decoder,slot,attempt));",
        )
        .map_err(write_error)?;
        let mut telemetry = crate::telemetry::Telemetry::default();
        telemetry.set_context("backfill", "rpc", "historical");
        Ok(Self {
            db,
            path: path.to_path_buf(),
            telemetry,
            gate: std::sync::Arc::new(crate::coordinator::WriteGate::default()),
        })
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
        for slot in start..=end {
            let record=self.db.query_row("SELECT status,error_class,attempts,block_time,first_pending FROM historical_slots WHERE mint=?1 AND decoder=?2 AND slot=?3",params![mint,decoder::DECODER_VERSION,slot as i64],|r| Ok((r.get::<_,String>(0)?,r.get::<_,Option<String>>(1)?,u64::try_from(r.get::<_,i64>(2)?).map_err(|_|rusqlite::Error::InvalidQuery)?,r.get::<_,Option<i64>>(3)?,u64::try_from(r.get::<_,i64>(4)?).map_err(|_|rusqlite::Error::InvalidQuery)?))).optional().map_err(read_error)?;
            let scheduled = record.is_some();
            let (status, error_class, attempts, block_time, first) =
                record.unwrap_or(("unknown".into(), None, 0, None, now()));
            if status == "complete" || status == "skipped" {
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
        self.db.execute("UPDATE historical_slots SET status=?1,error_class=?2,block_time=?3 WHERE mint=?4 AND decoder=?5 AND slot=?6",params![status,error,time,mint,decoder::DECODER_VERSION,slot as i64]).map_err(write_error)?;
        Ok(())
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
        for slot in start..=end {
            let _permit = self.gate.historical()?;
            self.db.execute("INSERT OR IGNORE INTO historical_slots(mint,decoder,slot,status,first_pending) VALUES (?1,?2,?3,'unknown',?4)",params![mint,decoder::DECODER_VERSION,slot as i64,now() as i64]).map_err(write_error)?;
        }
        let started = Instant::now();
        let mut metrics = Metrics::default();
        let mut store = {
            let _permit = self.gate.historical()?;
            Store::open(&self.path)?
        };
        for slot in start..=end {
            if cancelled() {
                metrics.cancelled = true;
                break;
            }
            let current = self.coverage(mint, slot, slot)?.slots.remove(0);
            if current.status == "complete" || current.status == "skipped" {
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
                self.db.execute("UPDATE historical_slots SET attempts=attempts+1 WHERE mint=?1 AND decoder=?2 AND slot=?3",params![mint,decoder::DECODER_VERSION,slot as i64]).map_err(write_error)?;
            }
            let cached: Option<Vec<u8>> = if current.error_class.is_none()
                || current.error_class.as_deref() == Some("cancelled")
                || current
                    .error_class
                    .as_deref()
                    .is_some_and(|e| e.starts_with("storage_"))
            {
                self.db.query_row("SELECT response FROM historical_inputs WHERE mint=?1 AND decoder=?2 AND slot=?3 ORDER BY attempt DESC LIMIT 1",params![mint,decoder::DECODER_VERSION,slot as i64],|r|r.get(0)).optional().map_err(read_error)?
            } else {
                None
            };
            let response = match cached {
                Some(bytes) => Ok(bytes),
                None => {
                    metrics.source_attempts += 1;
                    source.block(slot)
                }
            };
            let raw = match response {
                Ok(bytes) if bytes.len() <= MAX_BLOCK_BYTES => bytes,
                result => {
                    let error = result.err().unwrap_or(Error("block_too_large"));
                    metrics.source_errors += 1;
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
                self.db.execute("INSERT INTO historical_inputs VALUES (?1,?2,?3,?4,'finalized-mainnet-rpc',?5,?6)",params![mint,decoder::DECODER_VERSION,slot as i64,(current.attempts+1) as i64,now() as i64,raw]).map_err(write_error)?;
            }
            let failures_before = metrics.decode_failures;
            let outcome =
                process_block(&raw, slot, &mut store, &mut metrics, &cancelled, &self.gate);
            match outcome {
                Ok((parent, time)) => {
                    // A finalized child links directly to its parent; intermediate slots are skipped.
                    // Never overwrite observed produced data with skip evidence.
                    for skipped in start.max(parent.saturating_add(1))..slot {
                        let observed: bool=self.db.query_row("SELECT EXISTS(SELECT 1 FROM historical_inputs WHERE slot=?1 AND CASE WHEN json_valid(CAST(response AS TEXT)) THEN json_type(CAST(response AS TEXT),'$.result')='object' ELSE 0 END)",[skipped as i64],|r|r.get(0)).map_err(read_error)?;
                        if !observed {
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
                    } else if error.0.starts_with("storage_") || error.0 == "capture_conflict" {
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
        metrics.elapsed_seconds = started.elapsed().as_secs_f64();
        Ok((self.coverage(mint, start, end)?, metrics))
    }
}
fn process_block(
    raw: &[u8],
    slot: u64,
    store: &mut Store,
    metrics: &mut Metrics,
    cancelled: &impl Fn() -> bool,
    gate: &crate::coordinator::WriteGate,
) -> Result<(u64, Option<i64>)> {
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
        let capture = json!({"schema_version":1,"cluster":"mainnet-beta","commitment":"finalized","source":"solana-mainnet-rpc","result":result});
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
        if decoded.is_ok() {
            let _permit = gate.historical()?;
            store.persist(&parsed, &bytes, &decoded)?;
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
