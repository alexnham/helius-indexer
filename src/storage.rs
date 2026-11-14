#[cfg(feature = "archive")]
pub(crate) mod bridge;
pub(crate) mod history;
pub(crate) mod live;
pub mod pg;
pub(crate) mod repair;
pub(crate) mod tls;
use crate::{
    Error, Result,
    decoder::DECODER_VERSION,
    model::{Capture, Transfer},
    reconcile,
};
pub use pg::Database;
use postgres::{Client, GenericClient, Transaction, fallible_iterator::FallibleIterator};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};

/// Transfers are range-partitioned by slot; one partition spans one epoch of slots.
const PARTITION_SLOTS: i64 = 432_000;

/// Storage write outcomes for one Store, for stage metrics. Latency is observed per batch
/// (one PostgreSQL transaction); failures are storage errors only, never decode outcomes.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
pub struct WriteStats {
    /// Batches committed or attempted; latency buckets observe one batch each.
    pub batches: u64,
    /// Batches retried item by item after the whole batch failed.
    pub fallbacks: u64,
    /// Items written (transactions), across all batches.
    pub writes: u64,
    /// Raw input bytes across all batches.
    pub batch_bytes: u64,
    pub failed_writes: u64,
    pub transfers_written: u64,
    pub seconds: f64,
    /// Cumulative counts of writes at or under 0.001, 0.01, 0.1, 1 and 10 seconds.
    pub buckets: [u64; 5],
    pub partitions_created: u64,
}

pub const WRITE_BUCKETS: [f64; 5] = [0.001, 0.01, 0.1, 1.0, 10.0];

impl WriteStats {
    /// `indexer_stage_duration_seconds` samples for transfer writes; callers keep the
    /// family contiguous with their own samples. Labels are bounded by `mode`.
    pub fn duration_lines(&self, mode: &str) -> String {
        let mut out = String::new();
        for (bound, count) in WRITE_BUCKETS.iter().zip(self.buckets) {
            out.push_str(&format!(
                "indexer_stage_duration_seconds_bucket{{operation=\"transfer_write\",mode=\"{mode}\",le=\"{bound}\"}} {count}\n"
            ));
        }
        out.push_str(&format!(
            "indexer_stage_duration_seconds_bucket{{operation=\"transfer_write\",mode=\"{mode}\",le=\"+Inf\"}} {writes}\nindexer_stage_duration_seconds_sum{{operation=\"transfer_write\",mode=\"{mode}\"}} {}\nindexer_stage_duration_seconds_count{{operation=\"transfer_write\",mode=\"{mode}\"}} {writes}\n",
            self.seconds,
            writes = self.batches,
        ));
        out
    }

    pub fn counter_lines(&self, mode: &str) -> String {
        format!(
            "indexer_storage_write_failures_total{{mode=\"{mode}\"}} {}\nindexer_storage_transfers_written_total{{mode=\"{mode}\"}} {}\nindexer_storage_batches_total{{mode=\"{mode}\"}} {}\nindexer_storage_batch_items_total{{mode=\"{mode}\"}} {}\nindexer_storage_batch_bytes_total{{mode=\"{mode}\"}} {}\nindexer_storage_batch_fallbacks_total{{mode=\"{mode}\"}} {}\n",
            self.failed_writes,
            self.transfers_written,
            self.batches,
            self.writes,
            self.batch_bytes,
            self.fallbacks
        )
    }
}

/// Batch statements, prepared once per connection so each later batch costs one round
/// trip per statement instead of two.
const BATCH_CAPTURES: &str = "INSERT INTO captures(signature,slot,result_json,capture_json,status,decoder_version)
                 SELECT s,sl,r,c,st,$6 FROM unnest($1::text[],$2::bigint[],$3::text[],$4::bytea[],$5::text[]) AS u(s,sl,r,c,st)
                 ON CONFLICT(signature) DO NOTHING RETURNING signature";
// Copies the payload from the captures just inserted, so it is uploaded once, not twice.
const BATCH_INPUTS: &str = "INSERT INTO capture_inputs(signature,source,result_digest,slot,result_json,capture_json,status)
                 SELECT c.signature,u.source,u.digest,c.slot,c.result_json,c.capture_json,c.status
                 FROM unnest($1::text[],$2::text[],$3::bytea[]) AS u(signature,source,digest)
                 JOIN captures c ON c.signature=u.signature ORDER BY c.signature
                 ON CONFLICT(signature,source,result_digest) DO UPDATE SET status=excluded.status";
const BATCH_TRANSFERS: &str = "INSERT INTO transfers(transfer_id,signature,slot,outer_index,inner_index,mint,record_json)
         SELECT * FROM unnest($1::text[],$2::text[],$3::bigint[],$4::int[],$5::int[],$6::text[],$7::text[])
         ON CONFLICT(slot,transfer_id) DO NOTHING RETURNING signature";
const BATCH_ACKNOWLEDGE: &str = "UPDATE live_raw SET status=u.status FROM unnest($1::bigint[],$2::text[]) AS u(id,status) WHERE live_raw.id=u.id";

#[derive(Clone)]
struct BatchStatements {
    captures: postgres::Statement,
    inputs: postgres::Statement,
    transfers: postgres::Statement,
    acknowledge: postgres::Statement,
}

pub struct Store {
    db: Client,
    /// Partitions this connection has confirmed; partitions are never dropped.
    partitions: HashSet<i64>,
    stats: WriteStats,
    statements: Option<BatchStatements>,
}

fn write_failed(_: postgres::Error) -> Error {
    Error("storage_write_failed")
}

fn read_failed(_: postgres::Error) -> Error {
    Error("storage_read_failed")
}

impl Store {
    pub fn open(db: &Database) -> Result<Self> {
        Ok(Self {
            db: db.connect()?,
            partitions: HashSet::new(),
            stats: WriteStats::default(),
            statements: None,
        })
    }

    /// Creates and migrates the session schema, reporting the schema version.
    pub fn migrate(db: &Database) -> Result<serde_json::Value> {
        db.connect()?;
        Ok(
            serde_json::json!({"session":db.session(),"schema_version":pg::MIGRATIONS.last().map_or(0, |m| m.0)}),
        )
    }

    /// Creates missing transfer partitions outside the caller's write transaction, so
    /// concurrent writers serialize only on the first creation of a partition.
    pub(crate) fn ensure_partitions(&mut self, slots: impl IntoIterator<Item = u64>) -> Result<()> {
        for slot in slots {
            let index =
                i64::try_from(slot).map_err(|_| Error("unsupported_slot"))? / PARTITION_SLOTS;
            if self.partitions.contains(&index) {
                continue;
            }
            let mut tx = self.db.transaction().map_err(write_failed)?;
            tx.execute(
                "SELECT pg_advisory_xact_lock(731194600, $1::int)",
                &[&(index as i32)],
            )
            .map_err(write_failed)?;
            tx.batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS transfers_p{index} PARTITION OF transfers FOR VALUES FROM ({}) TO ({})",
                index * PARTITION_SLOTS,
                (index + 1) * PARTITION_SLOTS
            ))
            .map_err(write_failed)?;
            tx.commit().map_err(|_| Error("storage_commit_failed"))?;
            self.partitions.insert(index);
            self.stats.partitions_created += 1;
        }
        Ok(())
    }

    /// Stores a complete transaction's output atomically. Replay of identical input is a no-op.
    pub fn persist(
        &mut self,
        capture: &Capture,
        raw: &[u8],
        decoded: &Result<Vec<Transfer>>,
    ) -> Result<usize> {
        let item = BatchItem {
            capture,
            raw,
            decoded,
        };
        self.persist_batch(std::slice::from_ref(&item)).remove(0)
    }

    /// Stores many transactions in one PostgreSQL transaction, returning one result per item
    /// in input order. New transactions use set-based statements, so a batch costs a few
    /// round trips rather than several per item; repeats reconcile one by one in savepoints.
    ///
    /// READ COMMITTED: first observations insert without reading; a repeat waits on the
    /// stored capture's row lock. Rows are written in signature and transfer-ID order, and
    /// deadlocks retry the whole batch. A batch that still fails is retried item by item,
    /// so one bad input cannot block the rest.
    pub fn persist_batch(&mut self, items: &[BatchItem<'_>]) -> Vec<Result<usize>> {
        self.persist_batch_acknowledged(items, None)
    }

    pub(crate) fn persist_batch_acknowledged(
        &mut self,
        items: &[BatchItem<'_>],
        acknowledge: Option<&Acknowledge<'_>>,
    ) -> Vec<Result<usize>> {
        let started = std::time::Instant::now();
        let mut fallback = false;
        let results = match self.write_batch(items, acknowledge) {
            Ok(results) => results,
            Err(_) if items.len() > 1 => {
                fallback = true;
                let results = items
                    .iter()
                    .enumerate()
                    .map(|(index, item)| {
                        let single = acknowledge.map(|a| Acknowledge {
                            ids: &a.ids[index..=index],
                            failed: &[],
                        });
                        self.write_batch(std::slice::from_ref(item), single.as_ref())
                            .and_then(|mut r| r.remove(0))
                    })
                    .collect();
                if let Some(a) = acknowledge.filter(|a| !a.failed.is_empty()) {
                    // Inputs that failed before storage have no output; record them on their own.
                    let empty = Acknowledge {
                        ids: &[],
                        failed: a.failed,
                    };
                    if self.write_batch(&[], Some(&empty)).is_err() {
                        // Those inputs stay pending and retryable; the failure is still counted.
                        self.stats.failed_writes += 1;
                    }
                }
                results
            }
            Err(error) => vec![Err(error); items.len()],
        };
        let seconds = started.elapsed().as_secs_f64();
        let stats = &mut self.stats;
        stats.batches += 1;
        stats.fallbacks += u64::from(fallback);
        stats.writes += items.len() as u64;
        stats.batch_bytes += items.iter().map(|i| i.raw.len() as u64).sum::<u64>();
        stats.seconds += seconds;
        for (count, bound) in stats.buckets.iter_mut().zip(WRITE_BUCKETS) {
            *count += u64::from(seconds <= bound);
        }
        for result in &results {
            match result {
                Ok(inserted) => stats.transfers_written += *inserted as u64,
                Err(error) if error.0.starts_with("storage_") => stats.failed_writes += 1,
                Err(_) => {}
            }
        }
        results
    }

    pub fn stats(&self) -> WriteStats {
        self.stats
    }

    fn write_batch(
        &mut self,
        items: &[BatchItem<'_>],
        acknowledge: Option<&Acknowledge<'_>>,
    ) -> Result<Vec<Result<usize>>> {
        let mut early: Vec<Option<Result<usize>>> = vec![None; items.len()];
        let mut prepared = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            match prepare(index, item) {
                Ok(p) => prepared.push(p),
                Err(error) => early[index] = Some(Err(error)),
            }
        }
        // Every transfer of a capture carries the capture's slot, including repaired ones.
        self.ensure_partitions(prepared.iter().map(|p| p.slot as u64))?;
        prepared.sort_by(|a, b| a.signature.cmp(b.signature).then(a.index.cmp(&b.index)));
        let mut attempt = 0;
        loop {
            match self.write_prepared(items, &prepared, acknowledge, &early) {
                Ok(results) => return Ok(results),
                Err(error) if attempt < 2 && retryable(&error) => attempt += 1,
                Err(_) => return Err(Error("storage_write_failed")),
            }
        }
    }

    fn write_prepared(
        &mut self,
        items: &[BatchItem<'_>],
        prepared: &[Prepared<'_>],
        acknowledge: Option<&Acknowledge<'_>>,
        early: &[Option<Result<usize>>],
    ) -> std::result::Result<Vec<Result<usize>>, postgres::Error> {
        let mut results = early.to_vec();
        if self.statements.is_none() {
            self.statements = Some(BatchStatements {
                captures: self.db.prepare(BATCH_CAPTURES)?,
                inputs: self.db.prepare(BATCH_INPUTS)?,
                transfers: self.db.prepare(BATCH_TRANSFERS)?,
                acknowledge: self.db.prepare(BATCH_ACKNOWLEDGE)?,
            });
        }
        let statements = self.statements.clone().expect("prepared above");
        let mut tx = self.db.transaction()?;
        let mut seen = HashSet::new();
        let firsts: Vec<&Prepared<'_>> = prepared
            .iter()
            .filter(|p| seen.insert(p.signature))
            .collect();
        let column =
            |f: &dyn Fn(&Prepared<'_>) -> String| firsts.iter().map(|p| f(p)).collect::<Vec<_>>();
        let signatures = column(&|p| p.signature.to_string());
        let slots: Vec<i64> = firsts.iter().map(|p| p.slot).collect();
        let references = column(&|p| p.result_json.clone());
        let raws: Vec<&[u8]> = firsts.iter().map(|p| p.item.raw).collect();
        let statuses = column(&|p| p.status.to_string());
        let inserted: HashSet<String> = if firsts.is_empty() {
            HashSet::new()
        } else {
            tx.query(
                &statements.captures,
                &[
                    &signatures,
                    &slots,
                    &references,
                    &raws,
                    &statuses,
                    &DECODER_VERSION,
                ],
            )?
            .iter()
            .map(|r| r.get(0))
            .collect()
        };
        // In-batch duplicates of a new signature take the repeat path after its first copy.
        let new: Vec<&Prepared<'_>> = firsts
            .iter()
            .copied()
            .filter(|p| inserted.contains(p.signature))
            .collect();
        let repeats = prepared
            .iter()
            .filter(|p| !new.iter().any(|n| n.index == p.index));
        if !new.is_empty() {
            let column =
                |f: &dyn Fn(&Prepared<'_>) -> String| new.iter().map(|p| f(p)).collect::<Vec<_>>();
            let sources = column(&|p| p.item.capture.source.clone());
            let digests: Vec<&[u8]> = new.iter().map(|p| p.digest.as_slice()).collect();
            tx.execute(
                &statements.inputs,
                &[&column(&|p| p.signature.to_string()), &sources, &digests],
            )?;
            let mut rows: Vec<(&str, &Transfer)> = new
                .iter()
                .filter_map(|p| p.item.decoded.as_ref().ok().map(|t| (p.signature, t)))
                .flat_map(|(signature, transfers)| transfers.iter().map(move |t| (signature, t)))
                .collect();
            rows.sort_by(|a, b| (a.1.slot, &a.1.transfer_id).cmp(&(b.1.slot, &b.1.transfer_id)));
            let counts = insert_transfer_rows(&mut tx, &statements.transfers, &rows)?;
            for p in &new {
                results[p.index] = Some(Ok(counts.get(p.signature).copied().unwrap_or(0)));
            }
        }
        for p in repeats {
            let mut item = tx.savepoint("item")?;
            match reconcile_repeat(&mut item, p) {
                Ok(kept) => {
                    item.commit()?;
                    results[p.index] = Some(kept);
                }
                Err(error) => {
                    item.rollback()?;
                    results[p.index] = Some(Err(error));
                }
            }
        }
        let results: Vec<Result<usize>> = results
            .into_iter()
            .map(|r| r.unwrap_or(Err(Error("storage_write_failed"))))
            .collect();
        if let Some(acknowledge) = acknowledge {
            // Progress commits with the output it describes, never before or without it.
            let mut ids: Vec<i64> = acknowledge.ids.to_vec();
            let mut statuses: Vec<&str> = results
                .iter()
                .zip(items)
                .map(|(result, item)| match (result, item.decoded) {
                    (Ok(_), Ok(_)) => "decoded",
                    (Ok(_), Err(error)) | (Err(error), _) => error.0,
                })
                .collect();
            for (id, status) in acknowledge.failed {
                ids.push(*id);
                statuses.push(status);
            }
            tx.execute(&statements.acknowledge, &[&ids, &statuses])?;
        }
        tx.commit()?;
        Ok(results)
    }

    pub fn summary(db: &Database, mint: &str) -> Result<serde_json::Value> {
        let mut client = db.connect()?;
        let mut rows = client
            .query_raw(
                "SELECT record_json FROM transfers t WHERE mint=$1 AND NOT EXISTS(SELECT 1 FROM decoder_repairs r WHERE r.signature=t.signature AND r.status='unresolved')",
                [&mint],
            )
            .map_err(read_failed)?;
        let (mut count, mut volume, mut fees, mut unknown_fees) = (0u64, 0u128, 0u128, 0u64);
        let mut other_decoder_transfers = 0u64;
        while let Some(row) = rows.next().map_err(read_failed)? {
            let transfer: Transfer = serde_json::from_str(row.get::<_, &str>(0))
                .map_err(|_| Error("stored_record_invalid"))?;
            if transfer.decoder_version != DECODER_VERSION {
                other_decoder_transfers = other_decoder_transfers
                    .checked_add(1)
                    .ok_or(Error("aggregate_overflow"))?;
            }
            count = count.checked_add(1).ok_or(Error("aggregate_overflow"))?;
            volume = volume
                .checked_add(u128::from(transfer.amount_raw))
                .ok_or(Error("aggregate_overflow"))?;
            if let Some(fee) = transfer.fee_raw {
                fees = fees
                    .checked_add(u128::from(fee))
                    .ok_or(Error("aggregate_overflow"))?;
            } else {
                unknown_fees = unknown_fees
                    .checked_add(1)
                    .ok_or(Error("aggregate_overflow"))?;
            }
        }
        Ok(
            serde_json::json!({"mint":mint,"transfer_count":count,"target_decoder":DECODER_VERSION,"other_decoder_transfer_count":other_decoder_transfers,"known_volume_raw":volume.to_string(),"known_fees_raw":fees.to_string(),"unknown_fee_count":unknown_fees,"coverage":"unknown","window":"all_stored"}),
        )
    }

    /// Runtime-test retention: keeps the newest `keep` transfers and drops inputs and
    /// progress for older slots, so pruned slots report unknown rather than complete.
    /// Partitions are kept; time-window retention by dropping partitions belongs to #10.
    pub fn prune(db: &Database, keep: usize) -> Result<serde_json::Value> {
        if !(1..=1_000_000).contains(&keep) {
            return Err(Error("invalid_retention_limit"));
        }
        let mut client = db.connect()?;
        let mut transaction = client.transaction().map_err(write_failed)?;
        // Serializes concurrent prunes; writers are not blocked.
        transaction
            .execute("SELECT pg_advisory_xact_lock(7311946003)", &[])
            .map_err(write_failed)?;
        let transfers = transaction
            .execute(
                "DELETE FROM transfers WHERE (slot,transfer_id) NOT IN (SELECT slot,transfer_id FROM transfers
             ORDER BY slot DESC, signature DESC, outer_index DESC, inner_index DESC LIMIT $1)",
                &[&(keep as i64)],
            )
            .map_err(write_failed)?;
        let cutoff: Option<i64> = transaction
            .query_one("SELECT MIN(slot) FROM transfers", &[])
            .map_err(read_failed)?
            .get(0);
        let mut inputs = 0;
        if let Some(cutoff) = cutoff {
            for table in ["captures", "capture_inputs"] {
                inputs += transaction
                    .execute(
                        &format!(
                            "DELETE FROM {table} c WHERE slot < $1 AND NOT EXISTS(SELECT 1 FROM transfers t WHERE t.signature=c.signature)"
                        ),
                        &[&cutoff],
                    )
                    .map_err(write_failed)?;
            }
            for table in [
                "decoder_repairs",
                "live_raw",
                "historical_slots",
                "historical_inputs",
                "historical_source_slots",
                "historical_block_identity",
                "bridge_overlap_proofs",
            ] {
                // A count cap can split its oldest retained slot. Its completion
                // proofs must disappear atomically with the removed transfers.
                let proof = matches!(
                    table,
                    "historical_slots" | "historical_source_slots" | "bridge_overlap_proofs"
                );
                let comparison = if proof && transfers > 0 { "<=" } else { "<" };
                inputs += transaction
                    .execute(
                        &format!("DELETE FROM {table} WHERE slot {comparison} $1"),
                        &[&cutoff],
                    )
                    .map_err(write_failed)?;
            }
        }
        transaction.commit().map_err(write_failed)?;
        Ok(
            serde_json::json!({"kept_limit":keep,"removed_transfers":transfers,"removed_input_rows":inputs,
            "oldest_retained_slot":cutoff,"coverage":"unknown"}),
        )
    }

    /// Read-only export for the independent Python cross-check: retained block responses
    /// (at most `blocks` observations, 64 MiB) and this mint's stored rows in their slot range.
    pub fn comparison_input(db: &Database, mint: &str, blocks: i64) -> Result<serde_json::Value> {
        let mut client = db.connect()?;
        let mut tx = client
            .build_transaction()
            .isolation_level(postgres::IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(read_failed)?;
        let mut size = 0;
        let mut retained = Vec::new();
        for row in tx
            .query(
                "SELECT slot,response FROM historical_inputs ORDER BY slot,attempt LIMIT $1",
                &[&blocks],
            )
            .map_err(read_failed)?
        {
            let response: Vec<u8> = row.get(1);
            size += response.len();
            if size > 64 * 1024 * 1024 {
                return Err(Error("comparison_input_limit"));
            }
            let response: serde_json::Value =
                serde_json::from_slice(&response).map_err(|_| Error("stored_record_invalid"))?;
            retained.push(serde_json::json!({"slot":row.get::<_, i64>(0),"response":response}));
        }
        let slots = retained.iter().filter_map(|b| b["slot"].as_i64());
        let (low, high) = (slots.clone().min(), slots.max());
        let transfers = match (low, high) {
            (Some(low), Some(high)) => tx
                .query(
                    "SELECT record_json FROM transfers WHERE mint=$1 AND slot BETWEEN $2 AND $3 LIMIT 100001",
                    &[&mint, &low, &high],
                )
                .map_err(read_failed)?
                .iter()
                .map(|r| serde_json::from_str(r.get::<_, &str>(0)).map_err(|_| Error("stored_record_invalid")))
                .collect::<Result<Vec<serde_json::Value>>>()?,
            _ => Vec::new(),
        };
        tx.commit().map_err(read_failed)?;
        Ok(serde_json::json!({"blocks":retained,"transfers":transfers}))
    }

    pub fn query(db: &Database, mint: Option<&str>, limit: usize) -> Result<Vec<Transfer>> {
        Self::query_ordered(db, mint, limit, false)
    }

    pub fn query_recent(db: &Database, mint: Option<&str>, limit: usize) -> Result<Vec<Transfer>> {
        Self::query_ordered(db, mint, limit, true)
    }

    fn query_ordered(
        db: &Database,
        mint: Option<&str>,
        limit: usize,
        recent: bool,
    ) -> Result<Vec<Transfer>> {
        if !(1..=1000).contains(&limit) {
            return Err(Error("invalid_query_limit"));
        }
        let order = if recent { "DESC" } else { "ASC" };
        let mut client = db.connect()?;
        let rows = client
            .query(
                &format!(
                    "SELECT record_json FROM transfers t
            WHERE ($1::text IS NULL OR mint=$1)
            AND NOT EXISTS(SELECT 1 FROM decoder_repairs r WHERE r.signature=t.signature AND r.status='unresolved')
            ORDER BY slot {order}, signature {order}, outer_index {order}, inner_index {order} LIMIT $2"
                ),
                &[&mint, &(limit as i64)],
            )
            .map_err(read_failed)?;
        rows.iter()
            .map(|row| {
                serde_json::from_str(row.get::<_, &str>(0))
                    .map_err(|_| Error("stored_record_invalid"))
            })
            .collect()
    }

    pub(crate) fn observe_block_identity(
        &mut self,
        slot: u64,
        block: &serde_json::Value,
        transactions: &[serde_json::Value],
    ) -> Result<()> {
        let mut tx = self.db.transaction().map_err(write_failed)?;
        observe_block_identity_scan(&mut tx, slot, block, transactions)?;
        tx.commit().map_err(|_| Error("storage_commit_failed"))
    }
}

/// One input to `persist_batch`.
pub struct BatchItem<'a> {
    pub capture: &'a Capture,
    pub raw: &'a [u8],
    pub decoded: &'a Result<Vec<Transfer>>,
}

/// Live raw-input progress written in the same transaction as the batch output: `ids`
/// align with the batch items, and `failed` holds inputs that never reached storage.
pub(crate) struct Acknowledge<'a> {
    pub ids: &'a [i64],
    pub failed: &'a [(i64, &'static str)],
}

struct Prepared<'a> {
    index: usize,
    item: &'a BatchItem<'a>,
    signature: &'a str,
    value: serde_json::Value,
    result_json: String,
    digest: Vec<u8>,
    slot: i64,
    status: &'static str,
}

fn prepare<'a>(index: usize, item: &'a BatchItem<'a>) -> Result<Prepared<'a>> {
    let signature = item
        .capture
        .result
        .transaction
        .signatures
        .first()
        .ok_or(Error("missing_signature"))?;
    let value: serde_json::Value =
        serde_json::from_slice(item.raw).map_err(|_| Error("invalid_json"))?;
    let result_json = serde_json::to_string(&value["result"]).map_err(|_| Error("invalid_json"))?;
    let digest = Sha256::digest(result_json.as_bytes()).to_vec();
    let slot = i64::try_from(item.capture.result.slot).map_err(|_| Error("unsupported_slot"))?;
    let status = match item.decoded {
        Ok(_) if !item.capture.result.meta.err.is_null() => "failed_transaction",
        Ok(_) => "decoded",
        Err(e) => e.0,
    };
    Ok(Prepared {
        index,
        item,
        signature,
        value,
        result_json,
        digest,
        slot,
        status,
    })
}

fn retryable(error: &postgres::Error) -> bool {
    use postgres::error::SqlState;
    error.code().is_some_and(|c| {
        *c == SqlState::T_R_DEADLOCK_DETECTED || *c == SqlState::T_R_SERIALIZATION_FAILURE
    })
}

/// Inserts many transactions' transfers in one statement and counts new rows per signature.
fn insert_transfer_rows(
    tx: &mut Transaction<'_>,
    statement: &postgres::Statement,
    rows: &[(&str, &Transfer)],
) -> std::result::Result<std::collections::HashMap<String, usize>, postgres::Error> {
    let mut counts = std::collections::HashMap::new();
    if rows.is_empty() {
        return Ok(counts);
    }
    let mut columns = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for (signature, transfer) in rows {
        columns.0.push(transfer.transfer_id.as_str());
        columns.1.push(*signature);
        columns.2.push(transfer.slot as i64);
        columns.3.push(transfer.instruction_index as i32);
        columns
            .4
            .push(transfer.inner_instruction_index.map_or(-1, |n| n as i32));
        columns.5.push(transfer.mint.as_str());
        columns
            .6
            .push(serde_json::to_string(transfer).unwrap_or_default());
    }
    for row in tx.query(
        statement,
        &[
            &columns.0, &columns.1, &columns.2, &columns.3, &columns.4, &columns.5, &columns.6,
        ],
    )? {
        *counts.entry(row.get::<_, String>(0)).or_insert(0) += 1;
    }
    Ok(counts)
}

/// Reconciles an observation of an already stored transaction inside the caller's
/// savepoint. `Err` rolls the item back; `Ok(result)` keeps its writes, including a
/// durable quarantine receipt when `result` is an error.
fn reconcile_repeat(tx: &mut Transaction<'_>, prepared: &Prepared<'_>) -> Result<Result<usize>> {
    let Prepared {
        item: BatchItem {
            capture,
            raw,
            decoded,
        },
        signature,
        value,
        result_json,
        digest,
        slot,
        status,
        ..
    } = prepared;
    let (capture, raw, decoded, signature, slot, status) =
        (*capture, *raw, *decoded, *signature, *slot, *status);
    let observe = "INSERT INTO capture_inputs(signature,source,result_digest,slot,result_json,capture_json,status) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(signature,source,result_digest) DO NOTHING";
    let receipt =
        "UPDATE capture_inputs SET status=$1 WHERE signature=$2 AND source=$3 AND result_digest=$4";
    tx.execute(
        observe,
        &[
            &signature,
            &capture.source,
            &digest,
            &slot,
            &result_json,
            &raw,
            &"received",
        ],
    )
    .map_err(write_failed)?;
    let row = tx
        .query_one(
            "SELECT result_json,decoder_version,status FROM captures WHERE signature=$1 FOR UPDATE",
            &[&signature],
        )
        .map_err(read_failed)?;
    let (prior, version, prior_status): (String, String, String) =
        (row.get(0), row.get(1), row.get(2));
    let prior_value: serde_json::Value =
        serde_json::from_str(&prior).map_err(|_| Error("stored_record_invalid"))?;
    let mut repaired = None;
    let mut outcome = if version != DECODER_VERSION {
        Err(Error("decoder_revision_conflict"))
    } else {
        reconcile::compatible_results(&prior_value, &value["result"])
    };
    let mut reference = None;
    if outcome.is_ok() {
        outcome = (|| {
            let mut combined = reconcile::reference(&prior_value, &value["result"])?;
            if decoded.is_err() && !prior_value["meta"]["logMessages"].is_null() {
                combined["meta"]["logMessages"] = prior_value["meta"]["logMessages"].clone();
            }
            let mut envelope = value.clone();
            envelope["result"] = combined.clone();
            let raw = serde_json::to_vec(&envelope).map_err(|_| Error("serialization_failed"))?;
            let combined_capture = crate::decoder::parse_capture(&raw)?;
            if decoded.is_ok() {
                repaired = Some(crate::decoder::decode(&combined_capture)?);
            }
            reference =
                Some(serde_json::to_string(&combined).map_err(|_| Error("serialization_failed"))?);
            Ok(())
        })();
    }
    let mut updates = Vec::new();
    if outcome.is_ok()
        && prior_status == "decoded"
        && let Some(incoming) = repaired.as_ref()
    {
        let old = tx
            .query(
                "SELECT transfer_id,record_json FROM transfers WHERE signature=$1",
                &[&signature],
            )
            .map_err(read_failed)?
            .iter()
            .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
            .collect::<BTreeMap<_, _>>();
        if old.len() != incoming.len() {
            outcome = Err(Error("capture_conflict"));
        }
        for new in incoming {
            let Some(old) = old.get(&new.transfer_id) else {
                outcome = Err(Error("capture_conflict"));
                break;
            };
            let old: Transfer =
                serde_json::from_str(old).map_err(|_| Error("stored_record_invalid"))?;
            match reconcile::transfer(&old, new) {
                Ok(merged) => updates.push(merged),
                Err(e) => {
                    outcome = Err(e);
                    break;
                }
            }
        }
    }
    let receipt_status = outcome.as_ref().map_or_else(|e| e.0, |_| status);
    tx.execute(
        receipt,
        &[&receipt_status, &signature, &capture.source, &digest],
    )
    .map_err(write_failed)?;
    if let Err(error) = outcome {
        // Quarantine is durable, but existing logical query results remain unchanged.
        return Ok(Err(error));
    }
    tx.execute(
        "UPDATE captures SET result_json=$1 WHERE signature=$2",
        &[&reference, &signature],
    )
    .map_err(write_failed)?;
    if prior_status == "decoded" || prior_status == "failed_transaction" {
        for merged in updates {
            let text = serde_json::to_string(&merged).map_err(|_| Error("serialization_failed"))?;
            tx.execute(
                "UPDATE transfers SET record_json=$1 WHERE signature=$2 AND transfer_id=$3",
                &[&text, &signature, &merged.transfer_id],
            )
            .map_err(write_failed)?;
        }
        return Ok(Ok(0));
    }
    // A same-revision unresolved observation may become decodable after metadata arrives.
    if decoded.is_err() {
        return Ok(Ok(0));
    }
    tx.execute(
        "UPDATE captures SET capture_json=$1,status=$2 WHERE signature=$3",
        &[&raw, &status, &signature],
    )
    .map_err(write_failed)?;
    let inserted = insert_transfers(
        tx,
        signature,
        repaired.as_ref().or_else(|| decoded.as_ref().ok()),
    )?;
    Ok(Ok(inserted))
}

/// One multi-row statement per transaction; existing identities are left unchanged.
/// The inner index is -1 for outer instructions; an unknown fee stays null in record_json.
pub(crate) fn insert_transfers(
    tx: &mut Transaction<'_>,
    signature: &str,
    transfers: Option<&Vec<Transfer>>,
) -> Result<usize> {
    let Some(transfers) = transfers.filter(|t| !t.is_empty()) else {
        return Ok(0);
    };
    let (mut ids, mut slots, mut outer, mut inner, mut mints, mut records) = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for transfer in transfers {
        ids.push(transfer.transfer_id.as_str());
        slots.push(i64::try_from(transfer.slot).map_err(|_| Error("unsupported_slot"))?);
        outer.push(
            i32::try_from(transfer.instruction_index).map_err(|_| Error("unsupported_index"))?,
        );
        inner.push(match transfer.inner_instruction_index {
            Some(n) => i32::try_from(n).map_err(|_| Error("unsupported_index"))?,
            None => -1,
        });
        mints.push(transfer.mint.as_str());
        records.push(serde_json::to_string(transfer).map_err(|_| Error("serialization_failed"))?);
    }
    let inserted = tx
        .execute(
            "INSERT INTO transfers(transfer_id,signature,slot,outer_index,inner_index,mint,record_json)
             SELECT id,$2,slot,outer_index,inner_index,mint,record FROM unnest($1::text[],$3::bigint[],$4::int[],$5::int[],$6::text[],$7::text[])
             AS u(id,slot,outer_index,inner_index,mint,record)
             ON CONFLICT(slot,transfer_id) DO NOTHING",
            &[&ids, &signature, &slots, &outer, &inner, &mints, &records],
        )
        .map_err(write_failed)?;
    Ok(inserted as usize)
}

fn observe_block_identity_scan(
    tx: &mut Transaction<'_>,
    slot: u64,
    block: &serde_json::Value,
    transactions: &[serde_json::Value],
) -> Result<()> {
    let slot_key = slot as i64;
    let known: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM historical_block_identity WHERE slot=$1)",
            &[&slot_key],
        )
        .map_err(read_failed)?
        .get(0);
    if !known {
        let mut total = 0;
        for index in 0..33i64 {
            let raw: Option<Option<Vec<u8>>> = tx
                .query_opt(
                    "SELECT CASE WHEN octet_length(response)<=67108864 THEN response END FROM historical_inputs WHERE slot=$1 ORDER BY received_at,attempt LIMIT 1 OFFSET $2",
                    &[&slot_key, &index],
                )
                .map_err(read_failed)?
                .map(|r| r.get(0));
            let Some(raw) = raw else {
                break;
            };
            let raw = raw.ok_or(Error("block_identity_observation_limit"))?;
            total += raw.len();
            if index == 32
                || total > 128 * 1024 * 1024
                || raw.len() > crate::backfill::MAX_BLOCK_BYTES
            {
                return Err(Error("block_identity_observation_limit"));
            }
            let Ok(value) = serde_json::from_slice::<serde_json::Value>(&raw) else {
                continue;
            };
            if value["jsonrpc"] != "2.0" || value["id"] != 1 || value.get("error").is_some() {
                continue;
            }
            let prior = &value["result"];
            let Some(txs) = prior["transactions"].as_array() else {
                continue;
            };
            if txs.len() > 100_000
                || !prior["parentSlot"].as_u64().is_some_and(|p| p < slot)
                || !prior
                    .get("blockTime")
                    .is_some_and(|v| v.is_null() || v.is_i64())
            {
                continue;
            }
            if ["blockhash", "previousBlockhash"].iter().any(|field| {
                prior[*field].as_str().is_none_or(|s| {
                    s.len() > 44 || bs58::decode(s).into_vec().map_or(true, |b| b.len() != 32)
                })
            }) {
                continue;
            }
            observe_block_identity_inner(tx, slot, prior, txs)?;
        }
    }
    observe_block_identity_inner(tx, slot, block, transactions)
}

fn observe_block_identity_inner(
    tx: &mut impl GenericClient,
    slot: u64,
    block: &serde_json::Value,
    transactions: &[serde_json::Value],
) -> Result<()> {
    // Lock absent as well as existing parent/child anchors. Row locks alone cannot
    // serialize two first observations of different, adjacent slots.
    tx.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || ':block_identity', 0))",
        &[],
    )
    .map_err(write_failed)?;
    let mut ids = Vec::with_capacity(transactions.len());
    let mut seen = std::collections::HashSet::new();
    for transaction in transactions {
        let signatures = transaction["transaction"]["signatures"]
            .as_array()
            .filter(|s| !s.is_empty())
            .ok_or(Error("missing_signature"))?;
        for signature in signatures {
            let signature = signature
                .as_str()
                .filter(|s| s.len() <= 100)
                .ok_or(Error("invalid_base58_identifier"))?;
            if bs58::decode(signature)
                .into_vec()
                .map_or(true, |s| s.len() != 64)
            {
                return Err(Error("invalid_base58_identifier"));
            }
        }
        if !seen.insert(&signatures[0]) {
            return Err(Error("duplicate_block_transaction"));
        }
        ids.push(signatures);
    }
    let digest =
        Sha256::digest(serde_json::to_vec(&ids).map_err(|_| Error("serialization_failed"))?)
            .to_vec();
    let hash = block["blockhash"]
        .as_str()
        .ok_or(Error("invalid_block_hash"))?;
    let previous = block["previousBlockhash"]
        .as_str()
        .ok_or(Error("invalid_block_hash"))?;
    let parent = block["parentSlot"]
        .as_i64()
        .ok_or(Error("missing_block_parent"))?;
    let time = block["blockTime"].as_i64();
    let slot_key = slot as i64;
    let parent_hash: Option<String> = tx
        .query_opt(
            "SELECT blockhash FROM historical_block_identity WHERE slot=$1",
            &[&parent],
        )
        .map_err(read_failed)?
        .map(|r| r.get(0));
    let conflicting_child: bool = tx
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM historical_block_identity WHERE parent_slot=$1 AND previous_blockhash<>$2)",
            &[&slot_key, &hash],
        )
        .map_err(read_failed)?
        .get(0);
    if parent_hash.as_deref().is_some_and(|h| h != previous) || conflicting_child {
        return Err(Error("block_parent_conflict"));
    }
    let prior = tx
        .query_opt(
            "SELECT blockhash,parent_slot,previous_blockhash,transaction_ids_hash,block_time FROM historical_block_identity WHERE slot=$1 FOR UPDATE",
            &[&slot_key],
        )
        .map_err(read_failed)?;
    if let Some(row) = prior {
        let (old_hash, old_parent, old_previous, old_ids, old_time): (
            String,
            i64,
            String,
            Vec<u8>,
            Option<i64>,
        ) = (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
        if old_hash != hash
            || old_parent != parent
            || old_previous != previous
            || old_ids != digest
            || matches!((old_time,time),(Some(a),Some(b)) if a!=b)
        {
            return Err(Error("block_identity_conflict"));
        }
        tx.execute(
            "UPDATE historical_block_identity SET block_time=COALESCE(block_time,$2) WHERE slot=$1",
            &[&slot_key, &time],
        )
        .map_err(write_failed)?;
    } else {
        tx.execute(
            "INSERT INTO historical_block_identity VALUES($1,$2,$3,$4,$5,$6)",
            &[&slot_key, &hash, &parent, &previous, &digest, &time],
        )
        .map_err(write_failed)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
