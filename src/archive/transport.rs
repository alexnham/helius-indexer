use super::JETSTREAMER_REVISION;
use crate::{Error, Result, backfill::BlockSource};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
const ORIGIN: &str = "https://files.old-faithful.net";
const MAX_CHUNK: usize = 16 * 1024 * 1024;
/// Slots whose index records one prefetch reads in a single request (12 bytes each): a
/// whole bridge subrange.
const PREFETCH_SLOTS: u64 = 128;
const EPOCH_SLOTS: u64 = 432000;
/// Largest range one prefetch request covers; pieces of a window download concurrently.
const PIECE_BYTES: usize = 8 * 1024 * 1024;
/// Concurrent piece downloads. One archive stream gets ~4-11 MiB/s and the total levels
/// off at 8-16 streams (docs/validation/2026-11-18-archive-throughput-baseline.md).
const STREAMS: usize = 8;
/// Pieces per window: small pieces keep a stalled piece's deadline short.
const PIECES: u32 = 2 * STREAMS as u32;
/// A window is split into about `pieces` pieces of 1 MiB to `PIECE_BYTES` each.
fn piece_span(window: u64, pieces: u32) -> usize {
    (window / u64::from(pieces.max(1))).clamp(1024 * 1024, PIECE_BYTES as u64) as usize
}
/// A read that waits this long for bytes fails, so a stalled stream is retried instead of
/// holding its window until the 20-second deadline.
const STALL: Duration = Duration::from_secs(3);
/// Slowest per-stream rate a piece may take before it is retried, so a trickling stream
/// costs seconds rather than the full deadline.
// shortcut: assumes each of the `STREAMS` gets at least 1 MiB/s; a slower link fails pieces twice, make it configurable then.
const MIN_PIECE_RATE: u64 = 1024 * 1024;
/// Deadline for one piece of `span` bytes: 2 seconds plus its size at `MIN_PIECE_RATE`.
fn piece_deadline(span: usize) -> Duration {
    Duration::from_secs(2) + Duration::from_secs_f64(span as f64 / MIN_PIECE_RATE as f64)
}
/// (slot, CAR offset, length, 12-byte index record).
type IndexEntry = (u64, u64, u32, Vec<u8>);
static FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
pub struct Limits {
    pub bytes: u64,
    pub requests: u32,
    pub seconds: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            bytes: 128 * 1024 * 1024,
            requests: 32,
            seconds: 120,
        }
    }
}
#[derive(Default, Serialize)]
pub struct Stats {
    pub response_body_bytes: u64,
    pub requests: u32,
    pub cached_chunks: u32,
    /// Failed pieces requested again.
    pub retries: u32,
}
#[derive(Deserialize, Serialize)]
struct Receipt {
    schema_version: u32,
    jetstreamer_revision: String,
    epoch: u64,
    slot: u64,
    offset: u64,
    length: u32,
    index_record: Vec<u8>,
    sha256: String,
}

pub struct ArchiveSource {
    cache: PathBuf,
    network: bool,
    limits: Limits,
    began: Instant,
    /// One agent per source, so requests reuse its connection to the archive.
    agent: ureq::Agent,
    origin: String,
    stall: Duration,
    /// The run's slot range; a cache miss inside it prefetches the adjacent slots.
    planned: Option<(u64, u64)>,
    /// The run's mint: planned blocks are decoded with other transactions pruned.
    mint: Option<String>,
    /// Blocks decoded at once inside the planned range (`INDEXER_HISTORICAL_WORKERS`).
    pub(super) threads: u64,
    /// Normalized blocks decoded ahead of their `block` call.
    ready: BTreeMap<u64, Result<Vec<u8>>>,
    /// Recently decoded slots' own block hashes, so a child needs no second parse of its parent.
    hashes: BTreeMap<u64, String>,
    pub stats: Stats,
}
impl ArchiveSource {
    pub fn new(cache: &Path, network: bool, limits: Limits) -> Result<Self> {
        if limits.bytes == 0
            || limits.bytes > 128 * 1024 * 1024
            || limits.requests == 0
            || limits.requests > 128
            || limits.seconds == 0
            || limits.seconds > 120
        {
            return Err(Error("invalid_archive_limits"));
        }
        if network && std::env::var("INDEXER_ARCHIVE_OPT_IN").as_deref() != Ok("1") {
            return Err(Error("archive_opt_in_required"));
        }
        fs::create_dir_all(cache).map_err(|_| Error("archive_cache_write_failed"))?;
        Ok(Self {
            cache: cache.into(),
            network,
            limits,
            began: Instant::now(),
            agent: ureq::Agent::config_builder()
                .max_redirects(0)
                .http_status_as_error(false)
                .build()
                .into(),
            origin: ORIGIN.into(),
            stall: STALL,
            planned: None,
            mint: None,
            threads: 1,
            ready: BTreeMap::new(),
            hashes: BTreeMap::new(),
            stats: Stats::default(),
        })
    }
    /// Time left for one request: the command budget, at most 20 seconds.
    fn request_timeout(&self) -> Result<Duration> {
        Duration::from_secs(self.limits.seconds)
            .checked_sub(self.began.elapsed())
            .filter(|d| !d.is_zero())
            .map(|d| d.min(Duration::from_secs(20)))
            .ok_or(Error("archive_time_limit"))
    }
    /// Counts one request of `length` bytes against the limits, beyond `reserved` bytes
    /// already admitted but not yet read, and returns its deadline.
    fn admit(&mut self, offset: u64, length: usize, reserved: u64) -> Result<Duration> {
        if !self.network {
            return Err(Error("archive_cache_miss"));
        }
        if length == 0
            || length > MAX_CHUNK
            || self.stats.requests >= self.limits.requests
            || self.stats.response_body_bytes + reserved + length as u64 + 1 > self.limits.bytes
        {
            return Err(Error("archive_download_limit"));
        }
        let timeout = self.request_timeout()?;
        offset
            .checked_add(length as u64 - 1)
            .ok_or(Error("archive_invalid_range"))?;
        self.stats.requests += 1;
        Ok(timeout)
    }
    fn fetch(&mut self, path: &str, offset: u64, length: usize) -> Result<Vec<u8>> {
        let timeout = self.admit(offset, length, 0)?;
        let url = format!("{}/{path}", self.origin);
        let (bytes, read) = get(&self.agent, &url, offset, length, timeout, self.stall);
        self.stats.response_body_bytes += read;
        bytes
    }
    pub(super) fn probe_epoch(&mut self, epoch: u64) -> Result<bool> {
        if !self.network {
            return Err(Error("archive_cache_miss"));
        }
        if self.stats.requests >= self.limits.requests {
            return Err(Error("archive_download_limit"));
        }
        let timeout = self.request_timeout()?;
        self.stats.requests += 1;
        let response = self
            .agent
            .head(format!("{}/{epoch}/epoch-{epoch}.car", self.origin))
            .config()
            .timeout_global(Some(timeout))
            .build()
            .call()
            .map_err(|_| Error("archive_request_failed"))?;
        match response.status().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            403 => Err(Error("archive_access_denied")),
            _ => Err(Error("archive_availability_unknown")),
        }
    }
    pub(super) fn tip_candidate(&mut self, epoch: u64) -> Result<u64> {
        let first = epoch
            .checked_mul(432000)
            .ok_or(Error("invalid_archive_range"))?;
        let records = self.fetch(
            &format!("{epoch}/epoch-{epoch}-slot-ranges.raw"),
            431872 * 12,
            128 * 12,
        )?;
        for (index, record) in records.as_chunks::<12>().0.iter().enumerate().rev() {
            if record.iter().any(|b| *b != 0) {
                decode_index(record)?;
                return Ok(first + 431872 + index as u64);
            }
        }
        Err(Error("archive_tip_not_observed"))
    }
    pub(super) fn tip_time(&mut self, slot: u64) -> Result<i64> {
        let (raw, _) = self.chunk(slot)?;
        runtime()?.block_on(super::decode::block_time(&raw, slot))
    }
    fn paths(&self, slot: u64) -> (PathBuf, PathBuf) {
        let stem = format!("epoch-{}-slot-{slot}", slot / EPOCH_SLOTS);
        (
            self.cache.join(format!("{stem}.json")),
            self.cache.join(format!("{stem}.car-range")),
        )
    }
    fn chunk(&mut self, slot: u64) -> Result<(Vec<u8>, Receipt)> {
        if let Some((start, end)) = self.planned
            && (start..=end).contains(&slot)
            && !self.paths(slot).0.exists()
        {
            self.prefetch(slot, start, end)?;
        }
        let epoch = slot / EPOCH_SLOTS;
        let (manifest, file) = self.paths(slot);
        if manifest.exists() {
            let bytes = read_bounded(&manifest, 4096)?;
            let receipt: Receipt =
                serde_json::from_slice(&bytes).map_err(|_| Error("archive_invalid_receipt"))?;
            if receipt.schema_version != 1
                || receipt.jetstreamer_revision != JETSTREAMER_REVISION
                || receipt.slot != slot
                || receipt.epoch != epoch
                || receipt.length == 0
                || receipt.length as usize > MAX_CHUNK
                || receipt.index_record.len() != 12
            {
                return Err(Error("archive_invalid_receipt"));
            }
            let (offset, length) = decode_index(&receipt.index_record)?;
            if offset != receipt.offset || length != receipt.length {
                return Err(Error("archive_invalid_receipt"));
            }
            let raw = read_bounded(&file, MAX_CHUNK)?;
            if raw.len() != receipt.length as usize || hex(&Sha256::digest(&raw)) != receipt.sha256
            {
                return Err(Error("archive_cache_integrity_failed"));
            }
            self.stats.cached_chunks += 1;
            return Ok((raw, receipt));
        }
        let record = self.fetch(
            &format!("{epoch}/epoch-{epoch}-slot-ranges.raw"),
            (slot % 432000) * 12,
            12,
        )?;
        let (offset, length) = decode_index(&record)?;
        let raw = self.fetch(
            &format!("{epoch}/epoch-{epoch}.car"),
            offset,
            length as usize,
        )?;
        let receipt = self.store(slot, offset, length, record, &raw)?;
        Ok((raw, receipt))
    }
    fn store(
        &self,
        slot: u64,
        offset: u64,
        length: u32,
        index_record: Vec<u8>,
        raw: &[u8],
    ) -> Result<Receipt> {
        let receipt = Receipt {
            schema_version: 1,
            jetstreamer_revision: JETSTREAMER_REVISION.into(),
            epoch: slot / EPOCH_SLOTS,
            slot,
            offset,
            length,
            index_record,
            sha256: hex(&Sha256::digest(raw)),
        };
        let (manifest, file) = self.paths(slot);
        atomic_write(&file, raw)?;
        atomic_write(
            &manifest,
            &serde_json::to_vec(&receipt).map_err(|_| Error("serialization_failed"))?,
        )?;
        Ok(receipt)
    }
    /// Caches the uncached slots of the planned range near `slot` (from its predecessor,
    /// usually its parent) with one index request and one request per contiguous run of
    /// blocks, about `PIECES` runs per window, at most `STREAMS` at once. Each slot keeps
    /// its own receipt; slots without an index entry, or whose piece is lost, are left for
    /// `chunk` to fetch or report.
    fn prefetch(&mut self, slot: u64, start: u64, end: u64) -> Result<()> {
        let epoch = slot / EPOCH_SLOTS;
        let first = slot.saturating_sub(1).max(start).max(epoch * EPOCH_SLOTS);
        let last = end
            .min(first + PREFETCH_SLOTS - 1)
            .min(epoch * EPOCH_SLOTS + EPOCH_SLOTS - 1);
        let records = self.fetch(
            &format!("{epoch}/epoch-{epoch}-slot-ranges.raw"),
            (first % EPOCH_SLOTS) * 12,
            ((last - first + 1) * 12) as usize,
        )?;
        let mut wanted = Vec::new();
        for (s, record) in (first..=last).zip(records.as_chunks::<12>().0) {
            if let (false, Ok((offset, length))) = (self.paths(s).0.exists(), decode_index(record))
            {
                wanted.push((s, offset, length, record.to_vec()));
            }
        }
        // Pieces are admitted in order against the budget, then downloaded concurrently. A
        // piece that stalls or drops is requested once more, also within the budget.
        let url = format!("{}/{epoch}/epoch-{epoch}.car", self.origin);
        // Up to `PIECES` pieces, using at most half the requests left so retries and the
        // run's later windows still fit the command's request limit.
        let window = wanted.iter().map(|e| u64::from(e.2)).sum();
        let left = self.limits.requests.saturating_sub(self.stats.requests);
        let pieces = (left / 2).min(PIECES);
        let (mut pending, mut refused, mut first_error) = (
            contiguous_runs(wanted, piece_span(window, pieces)),
            None,
            None,
        );
        for round in 0..2 {
            let (mut admitted, mut reserved) = (Vec::new(), 0);
            for run in pending {
                let base = run[0].1;
                let span = run.last().map(|e| e.1 + u64::from(e.2) - base).unwrap() as usize;
                match self.admit(base, span, reserved) {
                    Ok(timeout) => {
                        reserved += span as u64;
                        admitted.push((base, span, timeout.min(piece_deadline(span)), run));
                    }
                    Err(error) => {
                        refused.get_or_insert(error);
                        break;
                    }
                }
            }
            // Each stream also caches its pieces' slots, so their file syncs overlap.
            admitted.reverse();
            let (queue, done) = (Mutex::new(admitted), Mutex::new(Vec::new()));
            let (this, url) = (&*self, &url);
            std::thread::scope(|scope| {
                let streams: Vec<_> = (0..STREAMS)
                    .map(|_| {
                        scope.spawn(|| {
                            while let Some((base, span, timeout, run)) =
                                queue.lock().ok().and_then(|mut q| q.pop())
                            {
                                let (bytes, read) =
                                    get(&this.agent, url, base, span, timeout, this.stall);
                                let stored = bytes.and_then(|bytes| {
                                    run.iter().try_for_each(|(s, offset, length, record)| {
                                        let at = (offset - base) as usize;
                                        let raw = &bytes[at..at + *length as usize];
                                        this.store(*s, *offset, *length, record.clone(), raw)
                                            .map(drop)
                                    })
                                });
                                if let Ok(mut done) = done.lock() {
                                    done.push((stored, read, run));
                                }
                            }
                        })
                    })
                    .collect();
                // A panicked stream loses its piece; those slots are fetched one by one later.
                for stream in streams {
                    let _ = stream.join();
                }
            });
            let pieces: Vec<(Result<()>, u64, Vec<IndexEntry>)> =
                done.into_inner().unwrap_or_default();
            pending = Vec::new();
            for (stored, read, run) in pieces {
                self.stats.response_body_bytes += read;
                match stored {
                    Ok(()) => {}
                    Err(Error(
                        "archive_request_failed"
                        | "archive_read_failed"
                        | "archive_truncated_range",
                    )) if round == 0 && !run.is_empty() => {
                        self.stats.retries += 1;
                        pending.push(run);
                    }
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                }
            }
            if pending.is_empty() {
                break;
            }
        }
        match first_error.or(refused) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
impl BlockSource for ArchiveSource {
    fn plan(&mut self, mint: &str, start: u64, end: u64) {
        self.planned = Some((start, end));
        self.mint = Some(mint.into());
        self.threads = crate::backfill::historical_workers().unwrap_or(1) as u64;
        self.ready.clear();
    }
    fn diagnostics(&self) -> serde_json::Value {
        serde_json::json!({"jetstreamer_revision":JETSTREAMER_REVISION,"response_body_bytes":self.stats.response_body_bytes,"requests":self.stats.requests,"cached_chunks":self.stats.cached_chunks,"retries":self.stats.retries,"limits":{"response_body_bytes":self.limits.bytes,"requests":self.limits.requests,"seconds":self.limits.seconds}})
    }
    fn provenance(&self) -> &'static str {
        super::SOURCE
    }
    fn capture_source(&self) -> &'static str {
        super::SOURCE
    }
    /// Inside the planned range, decodes the next `threads` slots at once and serves them
    /// in order. Cache reads and downloads stay sequential, so limits and stats are exact.
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        if let Some(done) = self.ready.remove(&slot) {
            return done;
        }
        // shortcut: a window may read (or download) slots the run then skips as complete; fine while windows stay at most 16 slots.
        // Only planned slots are pruned: boundary evidence outside the range stays whole.
        let (last, mint) = match self.planned {
            Some((start, end)) if (start..=end).contains(&slot) => {
                (end.min(slot + self.threads.max(1) - 1), self.mint.clone())
            }
            _ => (slot, None),
        };
        self.ready.clear();
        self.hashes
            .retain(|s, _| *s + PREFETCH_SLOTS >= slot && *s < slot);
        let chunks: Vec<_> = (slot..=last).map(|s| (s, self.chunk(s))).collect();
        let decoded = parallel(chunks, |s, chunk| {
            let (raw, receipt) = chunk?;
            decode(&raw, s, mint.as_deref()).map(|d| (d, receipt))
        });
        for (s, result) in &decoded {
            if let Ok(((_, _, hash), _)) = result {
                self.hashes.insert(*s, hash.clone());
            }
        }
        let mut linked = Vec::new();
        for (s, result) in decoded {
            linked.push((
                s,
                result.and_then(|((block, parent, _), receipt)| {
                    let hash = match self.hashes.get(&parent) {
                        Some(hash) => hash.clone(),
                        None => {
                            let (raw, _) = self.chunk(parent)?;
                            runtime()?.block_on(super::decode::parent_hash(&raw, parent))?
                        }
                    };
                    Ok((block, hash, receipt))
                }),
            ));
        }
        self.ready = parallel(linked, |s, linked| {
            let (block, hash, receipt) = linked?;
            normalized(block, hash, s, &receipt)
        })
        .into_iter()
        .collect();
        self.ready
            .remove(&slot)
            .unwrap_or(Err(Error("archive_decode_failed")))
    }
}
fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| Error("archive_runtime_failed"))
}
fn decode(raw: &[u8], slot: u64, mint: Option<&str>) -> Result<(serde_json::Value, u64, String)> {
    runtime()?.block_on(super::decode::decode_pruned(raw, slot, mint))
}
/// Runs `work` on each item on its own thread; a panic becomes that slot's error.
fn parallel<T: Send, U: Send>(
    items: Vec<(u64, T)>,
    work: impl Fn(u64, T) -> Result<U> + Sync,
) -> Vec<(u64, Result<U>)> {
    let work = &work;
    std::thread::scope(|scope| {
        let handles: Vec<_> = items
            .into_iter()
            .map(|(s, item)| (s, scope.spawn(move || work(s, item))))
            .collect();
        handles
            .into_iter()
            .map(|(s, h)| (s, h.join().unwrap_or(Err(Error("archive_decode_failed")))))
            .collect()
    })
}
fn normalized(
    mut block: serde_json::Value,
    parent_hash: String,
    slot: u64,
    receipt: &Receipt,
) -> Result<Vec<u8>> {
    block["previousBlockhash"] = json!(parent_hash);
    let bytes = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":block,"archive_provenance":{"jetstreamer_revision":receipt.jetstreamer_revision,"epoch":receipt.epoch,"slot":slot,"car_offset":receipt.offset,"car_length":receipt.length,"car_sha256":receipt.sha256}}))
        .map_err(|_| Error("serialization_failed"))?;
    if bytes.len() > crate::backfill::MAX_BLOCK_BYTES {
        return Err(Error("archive_normalized_block_limit"));
    }
    Ok(bytes)
}
/// One admitted range request; also returns the response body bytes read, for the stats.
fn get(
    agent: &ureq::Agent,
    url: &str,
    offset: u64,
    length: usize,
    timeout: Duration,
    stall: Duration,
) -> (Result<Vec<u8>>, u64) {
    let end = offset + length as u64 - 1;
    let response = agent
        .get(url)
        .header("Range", format!("bytes={offset}-{end}"))
        .header("Accept-Encoding", "identity")
        .config()
        .timeout_global(Some(timeout))
        .timeout_per_read(Some(stall))
        .build()
        .call();
    let mut response = match response {
        Ok(response) => response,
        Err(_) => return (Err(Error("archive_request_failed")), 0),
    };
    let checked = match response.status().as_u16() {
        403 => Err(Error("archive_access_denied")),
        404 => Err(Error("archive_object_unavailable")),
        206 => response
            .headers()
            .get("content-range")
            .and_then(|h| h.to_str().ok())
            .ok_or(Error("archive_invalid_content_range"))
            .and_then(|range| validate_range(range, offset, end)),
        _ => Err(Error("archive_range_not_honored")),
    }
    .and_then(|()| {
        if response
            .headers()
            .get("content-encoding")
            .is_some_and(|h| h != "identity")
        {
            Err(Error("archive_unexpected_encoding"))
        } else {
            Ok(())
        }
    });
    if let Err(error) = checked {
        return (Err(error), 0);
    }
    let mut bytes = Vec::new();
    let read = response
        .body_mut()
        .as_reader()
        .take(length as u64 + 1)
        .read_to_end(&mut bytes);
    let count = bytes.len() as u64;
    let result = match read {
        Err(_) => Err(Error("archive_read_failed")),
        Ok(_) if bytes.len() != length => Err(Error("archive_truncated_range")),
        Ok(_) => Ok(bytes),
    };
    (result, count)
}
fn read_bounded(path: &Path, max: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|_| Error("archive_cache_read_failed"))?
        .take(max as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error("archive_cache_read_failed"))?;
    if bytes.len() > max {
        return Err(Error("archive_cache_size_limit"));
    }
    Ok(bytes)
}
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        FILE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp)
        .map_err(|_| Error("archive_cache_write_failed"))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| Error("archive_cache_write_failed"))?;
    fs::rename(temp, path).map_err(|_| Error("archive_cache_write_failed"))?;
    fs::File::open(path.parent().ok_or(Error("archive_cache_write_failed"))?)
        .and_then(|f| f.sync_all())
        .map_err(|_| Error("archive_cache_write_failed"))
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn decode_index(record: &[u8]) -> Result<(u64, u32)> {
    if record.len() != 12 {
        return Err(Error("archive_invalid_index"));
    }
    let offset = u64::from_le_bytes(record[..8].try_into().unwrap());
    let length = u32::from_le_bytes(record[8..].try_into().unwrap());
    if offset == 0 || length == 0 {
        return Err(Error("archive_slot_unavailable"));
    }
    if length as usize > MAX_CHUNK || offset.checked_add(length as u64).is_none() {
        return Err(Error("archive_slot_size_limit"));
    }
    Ok((offset, length))
}
/// Groups (slot, offset, length, record) entries into runs fetched with one request: each
/// block starts where the previous one ended, and a run spans at most `max_span` bytes
/// (a single larger block, up to 16 MiB, is its own run).
fn contiguous_runs(entries: Vec<IndexEntry>, max_span: usize) -> Vec<Vec<IndexEntry>> {
    let mut runs: Vec<Vec<IndexEntry>> = Vec::new();
    for entry in entries {
        match runs.last_mut() {
            Some(run)
                if run.last().is_some_and(|p| p.1 + u64::from(p.2) == entry.1)
                    && entry.1 + u64::from(entry.2) - run[0].1 <= max_span as u64 =>
            {
                run.push(entry)
            }
            _ => runs.push(vec![entry]),
        }
    }
    runs
}
fn validate_range(value: &str, start: u64, end: u64) -> Result<()> {
    let (range, total) = value
        .strip_prefix("bytes ")
        .and_then(|v| v.split_once('/'))
        .ok_or(Error("archive_invalid_content_range"))?;
    if range != format!("{start}-{end}") || total.parse::<u64>().ok().is_none_or(|n| n <= end) {
        return Err(Error("archive_invalid_content_range"));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::BufRead, io::BufReader, net::TcpListener, sync::Arc};
    #[test]
    fn range_and_index_errors_never_mean_skipped_slots() {
        assert!(validate_range("bytes 10-21/100", 10, 21).is_ok());
        for invalid in ["bytes 11-22/100", "bytes 10-21/*", "bytes 10-21/21"] {
            assert!(validate_range(invalid, 10, 21).is_err());
        }
        assert_eq!(
            decode_index(&[0; 12]).unwrap_err().0,
            "archive_slot_unavailable"
        );
        assert!(decode_index(&[1; 11]).is_err());
    }
    #[test]
    fn only_adjacent_blocks_share_a_request_and_a_run_stays_within_a_piece() {
        let e = |slot, offset, length: usize| (slot, offset, length as u32, vec![]);
        let starts = |runs: Vec<Vec<IndexEntry>>| {
            runs.iter()
                .map(|r| r.iter().map(|e| e.0).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        };
        // 1-2 adjacent; 3 leaves a gap; 4 is adjacent to 3 but would pass the piece size;
        // 5 is larger than a piece on its own and still gets a run.
        let piece = PIECE_BYTES as u64;
        let runs = contiguous_runs(
            vec![
                e(1, 100, 10),
                e(2, 110, 5),
                e(3, 200, PIECE_BYTES - 10),
                e(4, 200 + piece - 10, 11),
                e(5, 201 + piece, PIECE_BYTES + 1),
            ],
            PIECE_BYTES,
        );
        assert_eq!(starts(runs), vec![vec![1, 2], vec![3], vec![4], vec![5]]);
        // A 64 MiB window becomes 16 pieces of 4 MiB; small and huge windows stay bounded.
        let mib = 1024 * 1024;
        assert_eq!(piece_span(64 * mib, 16), 4 * mib as usize);
        assert_eq!(piece_span(64 * mib, 8), 8 * mib as usize);
        assert_eq!(piece_span(3 * mib, 16), mib as usize);
        assert_eq!(piece_span(1024 * mib, 16), PIECE_BYTES);
        assert_eq!(piece_span(2 * mib, 0), 2 * mib as usize);
    }
    /// Serves `index` and `car` for epoch 0 Range requests. The first `stalls` block requests
    /// send nothing and are dropped or, with `trickle`, send one byte every 150 ms. Returns
    /// the origin and the block request count.
    fn archive_server(
        index: Vec<u8>,
        car: Vec<u8>,
        stalls: u64,
        trickle: bool,
    ) -> (String, Arc<AtomicU64>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let car_requests = Arc::new(AtomicU64::new(0));
        let count = car_requests.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let (mut stream, index, car, count) =
                    (stream.unwrap(), index.clone(), car.clone(), count.clone());
                std::thread::spawn(move || {
                    let mut reader = BufReader::new(stream.try_clone().unwrap());
                    let (mut path, mut range) = (String::new(), (0, 0));
                    loop {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap() == 0 || line.trim().is_empty() {
                            break;
                        }
                        let line = line.trim().to_ascii_lowercase();
                        if let Some(rest) = line.strip_prefix("get ") {
                            path = rest.split(' ').next().unwrap().to_string();
                        }
                        if let Some((a, b)) = line
                            .strip_prefix("range: bytes=")
                            .and_then(|r| r.split_once('-'))
                        {
                            range = (a.parse::<usize>().unwrap(), b.parse::<usize>().unwrap());
                        }
                    }
                    let body = if path.ends_with(".car") { &car } else { &index };
                    let stalled =
                        path.ends_with(".car") && count.fetch_add(1, Ordering::SeqCst) < stalls;
                    if stalled && !trickle {
                        std::thread::sleep(Duration::from_secs(2));
                        return;
                    }
                    let part = &body[range.0..=range.1];
                    let head = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        range.0,
                        range.1,
                        body.len(),
                        part.len()
                    );
                    stream.write_all(head.as_bytes()).unwrap();
                    if stalled {
                        for byte in part {
                            if stream.write_all(&[*byte]).is_err() {
                                return;
                            }
                            std::thread::sleep(Duration::from_millis(150));
                        }
                        return;
                    }
                    stream.write_all(part).unwrap();
                });
            }
        });
        (origin, car_requests)
    }
    #[test]
    fn a_stalled_piece_is_requested_once_more_within_the_request_budget() {
        // Slots 10-12 of epoch 0 are three adjacent 10-byte blocks: one piece.
        let car: Vec<u8> = (0..130u8).collect();
        let mut index = vec![0; 13 * 12];
        for (i, slot) in (10..13).enumerate() {
            let at = slot * 12;
            index[at..at + 8].copy_from_slice(&(100 + 10 * i as u64).to_le_bytes());
            index[at + 8..at + 12].copy_from_slice(&10u32.to_le_bytes());
        }
        let source = |origin: String| {
            let dir = tempfile::tempdir().unwrap();
            let mut source = ArchiveSource::new(dir.path(), false, Limits::default()).unwrap();
            (source.network, source.origin) = (true, origin);
            source.stall = Duration::from_millis(100);
            source.plan(crate::live::USDC, 10, 12);
            (source, dir)
        };
        // One stall: the retry caches every slot of the piece.
        let (origin, car_requests) = archive_server(index.clone(), car.clone(), 1, false);
        let (mut one, _dir) = source(origin);
        assert_eq!(one.chunk(10).unwrap().0, car[100..110]);
        assert_eq!((one.stats.requests, one.stats.retries), (3, 1));
        for slot in [11, 12] {
            let at = 100 + 10 * (slot as usize - 10);
            assert_eq!(one.chunk(slot).unwrap().0, car[at..at + 10]);
        }
        assert_eq!((one.stats.requests, one.stats.cached_chunks), (3, 3));
        assert_eq!(car_requests.load(Ordering::SeqCst), 2);
        // Two stalls: the slot fails after one retry instead of retrying again.
        let (origin, car_requests) = archive_server(index.clone(), car.clone(), 2, false);
        let (mut two, _dir) = source(origin);
        assert_eq!(two.chunk(10).err(), Some(Error("archive_request_failed")));
        assert_eq!((two.stats.requests, two.stats.retries), (3, 1));
        assert_eq!(car_requests.load(Ordering::SeqCst), 2);
        // A trickle that never stalls a read still misses the piece's own 2-second deadline.
        let (origin, _) = archive_server(index, car.clone(), 1, true);
        let (mut slow, _dir) = source(origin);
        slow.stall = Duration::from_millis(500);
        let began = Instant::now();
        assert_eq!(slow.chunk(10).unwrap().0, car[100..110]);
        assert_eq!(slow.stats.retries, 1);
        assert!(began.elapsed() < Duration::from_secs(4));
        assert_eq!(piece_deadline(4 * 1024 * 1024), Duration::from_secs(6));
    }
    #[test]
    fn offline_cache_miss_never_attempts_a_network() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = ArchiveSource::new(dir.path(), false, Limits::default()).unwrap();
        assert_eq!(source.block(468265447).unwrap_err().0, "archive_cache_miss");
        assert_eq!(source.stats.requests, 0);
    }
}
