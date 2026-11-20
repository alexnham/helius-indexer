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
        Arc, Mutex,
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
/// off at 8-16 streams (docs/validation/2026-11-18-archive-throughput-baseline.md). 16
/// streams (and 32 pieces per window) drew rate limiting from a VM
/// (docs/validation/2026-11-20-archive-vm-test.md), so this stays at 8 until 429s back off.
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
/// A decoded block (value, parent slot, own hash) with its cache receipt.
type Decoded = Result<((serde_json::Value, u64, String), Receipt)>;
/// A downloaded piece: its decoded blocks (or why it failed), bytes read, and its run.
type Piece = (Result<Vec<(u64, Decoded)>>, u64, Vec<IndexEntry>);
static FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
pub struct Limits {
    pub bytes: u64,
    pub requests: u32,
    pub seconds: u64,
}
const MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const MAX_REQUESTS: u32 = 2048;
impl Limits {
    /// A bridge batch of `slots`: 1 MiB and half a request per slot (blocks average about
    /// 0.5 MB), at least the single-command limits and at most 2 GiB and 2048 requests.
    pub fn bridge(slots: u64, seconds: u64) -> Self {
        let mib = 1024 * 1024;
        Self {
            bytes: (slots * mib).clamp(128 * mib, MAX_BYTES),
            requests: (slots / 2).clamp(128, u64::from(MAX_REQUESTS)) as u32,
            seconds,
        }
    }
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
    /// Decode threads for cached blocks inside the planned range (`INDEXER_HISTORICAL_WORKERS`).
    pub(super) threads: u64,
    /// Normalized blocks decoded ahead of their `block` call.
    ready: BTreeMap<u64, Result<Vec<u8>>>,
    /// Blocks a prefetch decoded as their pieces arrived, not yet linked to their parents.
    early: BTreeMap<u64, Decoded>,
    /// Recently decoded slots' own block hashes, so a child needs no second parse of its parent.
    hashes: BTreeMap<u64, String>,
    /// The batch's whole range (`BlockSource::ahead`): the next window downloads while the
    /// current one is written.
    batch: Option<(u64, u64)>,
    /// Index records already read, by slot: one request covers the rest of the batch.
    index: BTreeMap<u64, Vec<u8>>,
    /// The next window's download in flight, and the bytes admitted for it.
    next: Option<(std::thread::JoinHandle<Vec<Piece>>, u64)>,
    /// Set by an HTTP 429: 1 + the `Retry-After` seconds (0 when absent). Once set, no
    /// more requests are admitted for this source, so a rate-limited batch stops asking.
    limited: Arc<AtomicU64>,
    pub stats: Stats,
}
impl ArchiveSource {
    pub fn new(cache: &Path, network: bool, limits: Limits) -> Result<Self> {
        if limits.bytes == 0
            || limits.bytes > MAX_BYTES
            || limits.requests == 0
            || limits.requests > MAX_REQUESTS
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
            early: BTreeMap::new(),
            hashes: BTreeMap::new(),
            batch: None,
            index: BTreeMap::new(),
            next: None,
            limited: Arc::new(AtomicU64::new(0)),
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
        if self.limited.load(Ordering::Relaxed) > 0 {
            return Err(Error("archive_rate_limited"));
        }
        if length == 0
            || length > MAX_CHUNK
            || self.stats.requests >= self.limits.requests
            || self.stats.response_body_bytes
                + reserved
                + self.next.as_ref().map_or(0, |n| n.1)
                + length as u64
                + 1
                > self.limits.bytes
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
        let (bytes, read) = get(
            &self.agent,
            &url,
            offset,
            length,
            timeout,
            self.stall,
            &self.limited,
        );
        self.stats.response_body_bytes += read;
        bytes
    }
    pub(super) fn probe_epoch(&mut self, epoch: u64) -> Result<bool> {
        if !self.network {
            return Err(Error("archive_cache_miss"));
        }
        if self.limited.load(Ordering::Relaxed) > 0 {
            return Err(Error("archive_rate_limited"));
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
            429 => Err(rate_limited(&response, &self.limited)),
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
        paths(&self.cache, slot)
    }
    /// Reads a cached block and checks it against its receipt.
    fn cached(
        &self,
        slot: u64,
        epoch: u64,
        manifest: &Path,
        file: &Path,
    ) -> Result<(Vec<u8>, Receipt)> {
        let bytes = read_bounded(manifest, 4096)?;
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
        let raw = read_bounded(file, MAX_CHUNK)?;
        if raw.len() != receipt.length as usize || hex(&Sha256::digest(&raw)) != receipt.sha256 {
            return Err(Error("archive_cache_integrity_failed"));
        }
        Ok((raw, receipt))
    }
    fn chunk(&mut self, slot: u64) -> Result<(Vec<u8>, Receipt)> {
        self.finish_next();
        if let Some((start, end)) = self.planned
            && (start..=end).contains(&slot)
            && !self.paths(slot).0.exists()
        {
            self.prefetch(slot, start, end)?;
        }
        let epoch = slot / EPOCH_SLOTS;
        let (manifest, file) = self.paths(slot);
        if manifest.exists() {
            match self.cached(slot, epoch, &manifest, &file) {
                Ok(found) => {
                    self.stats.cached_chunks += 1;
                    return Ok(found);
                }
                // Cache files are written without fsync, so one cut short by a crash is
                // downloaded again rather than failing its slot.
                Err(_) if self.network => {
                    let _ = fs::remove_file(&manifest);
                    let _ = fs::remove_file(&file);
                }
                Err(error) => return Err(error),
            }
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
        let receipt = store(&self.cache, slot, offset, length, record, &raw)?;
        Ok((raw, receipt))
    }
    /// Index records for `first..=last` of `epoch`, read once for the rest of the batch.
    fn records(&mut self, epoch: u64, first: u64, last: u64) -> Result<Vec<(u64, Vec<u8>)>> {
        if !(first..=last).all(|s| self.index.contains_key(&s)) {
            let through = self
                .batch
                .filter(|(start, end)| (start..=end).contains(&&last))
                .map_or(last, |(_, end)| {
                    end.min(epoch * EPOCH_SLOTS + EPOCH_SLOTS - 1)
                })
                .max(last);
            let records = self.fetch(
                &format!("{epoch}/epoch-{epoch}-slot-ranges.raw"),
                (first % EPOCH_SLOTS) * 12,
                ((through - first + 1) * 12) as usize,
            )?;
            for (s, record) in (first..=through).zip(records.as_chunks::<12>().0) {
                self.index.insert(s, record.to_vec());
            }
        }
        Ok((first..=last)
            .map(|s| (s, self.index[&s].clone()))
            .collect())
    }
    /// The uncached, indexed slots of `first..=last` in pieces of about `PIECES` per window,
    /// using at most half the requests left so retries and later windows still fit.
    fn runs(&mut self, epoch: u64, first: u64, last: u64) -> Result<Vec<Vec<IndexEntry>>> {
        let mut wanted = Vec::new();
        for (s, record) in self.records(epoch, first, last)? {
            if let (false, Ok((offset, length))) = (self.paths(s).0.exists(), decode_index(&record))
            {
                wanted.push((s, offset, length, record));
            }
        }
        let window = wanted.iter().map(|e| u64::from(e.2)).sum();
        let left = self.limits.requests.saturating_sub(self.stats.requests);
        Ok(contiguous_runs(
            wanted,
            piece_span(window, (left / 2).min(PIECES)),
        ))
    }
    /// Admits pieces in order against the budget until one is refused.
    fn admit_runs(&mut self, runs: Vec<Vec<IndexEntry>>) -> (Vec<Admitted>, Option<Error>) {
        let (mut admitted, mut reserved) = (Vec::new(), 0);
        for run in runs {
            let base = run[0].1;
            let span = run.last().map(|e| e.1 + u64::from(e.2) - base).unwrap() as usize;
            match self.admit(base, span, reserved) {
                Ok(timeout) => {
                    reserved += span as u64;
                    admitted.push((base, span, timeout.min(piece_deadline(span)), run));
                }
                Err(error) => return (admitted, Some(error)),
            }
        }
        (admitted, None)
    }
    /// Counts downloaded pieces and keeps their decoded blocks; returns the runs worth one
    /// more request when `retry`, and records the first other error.
    fn absorb(
        &mut self,
        pieces: Vec<Piece>,
        retry: bool,
        first_error: &mut Option<Error>,
    ) -> Vec<Vec<IndexEntry>> {
        let mut again = Vec::new();
        for (stored, read, run) in pieces {
            self.stats.response_body_bytes += read;
            match stored {
                Ok(blocks) => self.early.extend(blocks),
                Err(Error(
                    "archive_request_failed" | "archive_read_failed" | "archive_truncated_range",
                )) if retry && !run.is_empty() => {
                    self.stats.retries += 1;
                    again.push(run);
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        again
    }
    /// Caches the uncached slots of the planned range near `slot` (from its predecessor,
    /// usually its parent) with one request per contiguous run of blocks, at most `STREAMS`
    /// at once. Each slot keeps its own receipt, and each stream decodes its pieces' blocks
    /// into `early` while other pieces download; slots without an index entry, or whose
    /// piece is lost, are left for `chunk` to fetch or report.
    fn prefetch(&mut self, slot: u64, start: u64, end: u64) -> Result<()> {
        let epoch = slot / EPOCH_SLOTS;
        let first = slot.saturating_sub(1).max(start).max(epoch * EPOCH_SLOTS);
        let last = end
            .min(first + PREFETCH_SLOTS - 1)
            .min(epoch * EPOCH_SLOTS + EPOCH_SLOTS - 1);
        let url = format!("{}/{epoch}/epoch-{epoch}.car", self.origin);
        // A piece that stalls or drops is requested once more, also within the budget.
        let (mut pending, mut refused, mut first_error) =
            (self.runs(epoch, first, last)?, None, None);
        for round in 0..2 {
            let (admitted, refusal) = self.admit_runs(pending);
            refused = refused.or(refusal);
            let pieces = download(
                &self.agent,
                &self.cache,
                &url,
                self.mint.as_deref(),
                self.stall,
                &self.limited,
                admitted,
            );
            pending = self.absorb(pieces, round == 0, &mut first_error);
            if pending.is_empty() {
                break;
            }
        }
        match first_error.or(refused) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    /// Starts downloading the batch's window after `last` in the background, within the
    /// budget. Anything it cannot start or loses is fetched when its slot is read.
    // shortcut: a batch that stops early drops the window in flight uncounted in `stats` (at most one window's bytes), count it if the byte charge must be exact.
    fn start_next(&mut self, last: u64) {
        let Some((_, end)) = self
            .batch
            .filter(|(start, end)| (start..=end).contains(&&(last + 1)))
        else {
            return;
        };
        if self.next.is_some() || !self.network {
            return;
        }
        let (first, epoch) = (last + 1, (last + 1) / EPOCH_SLOTS);
        let through = end
            .min(first + PREFETCH_SLOTS - 1)
            .min(epoch * EPOCH_SLOTS + EPOCH_SLOTS - 1);
        let Ok(runs) = self.runs(epoch, first, through) else {
            return;
        };
        let (admitted, _) = self.admit_runs(runs);
        if admitted.is_empty() {
            return;
        }
        let bytes = admitted.iter().map(|a| a.1 as u64).sum();
        let (agent, cache, mint, stall, limited) = (
            self.agent.clone(),
            self.cache.clone(),
            self.mint.clone(),
            self.stall,
            self.limited.clone(),
        );
        let url = format!("{}/{epoch}/epoch-{epoch}.car", self.origin);
        let handle = std::thread::spawn(move || {
            download(
                &agent,
                &cache,
                &url,
                mint.as_deref(),
                stall,
                &limited,
                admitted,
            )
        });
        self.next = Some((handle, bytes));
    }
    /// Waits for the window downloading in the background and keeps what arrived.
    fn finish_next(&mut self) {
        if let Some((handle, _)) = self.next.take() {
            let pieces = handle.join().unwrap_or_default();
            self.absorb(pieces, false, &mut None);
        }
    }
}
impl BlockSource for ArchiveSource {
    fn ahead(&mut self, start: u64, end: u64) {
        self.batch = Some((start, end));
    }
    fn plan(&mut self, mint: &str, start: u64, end: u64) {
        self.planned = Some((start, end));
        self.mint = Some(mint.into());
        self.threads = crate::backfill::historical_workers().unwrap_or(1) as u64;
        self.ready.clear();
    }
    fn diagnostics(&self) -> serde_json::Value {
        serde_json::json!({"jetstreamer_revision":JETSTREAMER_REVISION,"response_body_bytes":self.stats.response_body_bytes,"requests":self.stats.requests,"cached_chunks":self.stats.cached_chunks,"retries":self.stats.retries,"rate_limited":self.limited.load(Ordering::Relaxed)>0,"retry_after_seconds":self.limited.load(Ordering::Relaxed).checked_sub(1).filter(|s|*s>0),"limits":{"response_body_bytes":self.limits.bytes,"requests":self.limits.requests,"seconds":self.limits.seconds}})
    }
    fn provenance(&self) -> &'static str {
        super::SOURCE
    }
    fn capture_source(&self) -> &'static str {
        super::SOURCE
    }
    /// Inside the planned range, serves the next prefetch window (`PREFETCH_SLOTS`) in
    /// order: blocks a prefetch decoded as they arrived, and the rest read from the cache
    /// and decoded on `threads` threads. Cache reads stay sequential, so stats are exact.
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        if let Some(done) = self.ready.remove(&slot) {
            return done;
        }
        // shortcut: a window may read (or download) slots the run then skips as complete; the prefetch window downloads them anyway, revisit if resumed runs skip many.
        // Only planned slots are pruned: boundary evidence outside the range stays whole.
        let (last, mint) = match self.planned {
            Some((start, end)) if (start..=end).contains(&slot) => {
                (end.min(slot + PREFETCH_SLOTS - 1), self.mint.clone())
            }
            _ => (slot, None),
        };
        self.ready.clear();
        self.early.retain(|s, _| *s >= slot);
        self.hashes
            .retain(|s, _| *s + PREFETCH_SLOTS >= slot && *s < slot);
        let mut chunks = Vec::new();
        for s in slot..=last {
            if !self.early.contains_key(&s) {
                let chunk = self.chunk(s);
                // Reading `s` may have prefetched (and decoded) it.
                if !self.early.contains_key(&s) {
                    chunks.push((s, chunk));
                }
            }
        }
        let mut decoded = parallel(chunks, self.threads, |s, chunk| {
            let (raw, receipt) = chunk?;
            decode(&raw, s, mint.as_deref()).map(|d| (d, receipt))
        });
        let early = self.early.split_off(&(last + 1));
        decoded.extend(std::mem::replace(&mut self.early, early));
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
        self.ready = parallel(linked, self.threads, |s, linked| {
            let (block, hash, receipt) = linked?;
            normalized(block, hash, s, &receipt)
        })
        .into_iter()
        .collect();
        if mint.is_some() {
            self.start_next(last);
        }
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
/// A panic becomes the slot's error, so a bad block never takes down its thread's others.
fn decode(raw: &[u8], slot: u64, mint: Option<&str>) -> Result<(serde_json::Value, u64, String)> {
    std::panic::catch_unwind(|| runtime()?.block_on(super::decode::decode_pruned(raw, slot, mint)))
        .unwrap_or(Err(Error("archive_decode_failed")))
}
/// Runs `work` on each item on at most `threads` threads; a panic becomes that slot's error.
fn parallel<T: Send, U: Send>(
    items: Vec<(u64, T)>,
    threads: u64,
    work: impl Fn(u64, T) -> Result<U> + Sync,
) -> Vec<(u64, Result<U>)> {
    let (queue, done) = (Mutex::new(items), Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        for _ in 0..threads.max(1) {
            scope.spawn(|| {
                while let Some((s, item)) = queue.lock().ok().and_then(|mut q| q.pop()) {
                    let result =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work(s, item)))
                            .unwrap_or(Err(Error("archive_decode_failed")));
                    if let Ok(mut done) = done.lock() {
                        done.push((s, result));
                    }
                }
            });
        }
    });
    done.into_inner().unwrap_or_default()
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
/// A piece admitted for download: CAR offset, length, deadline, and its blocks.
type Admitted = (u64, usize, Duration, Vec<IndexEntry>);
/// Downloads admitted pieces on up to `STREAMS` streams; each stream caches and decodes its
/// pieces' blocks while other pieces download.
fn download(
    agent: &ureq::Agent,
    cache: &Path,
    url: &str,
    mint: Option<&str>,
    stall: Duration,
    limited: &AtomicU64,
    mut admitted: Vec<Admitted>,
) -> Vec<Piece> {
    admitted.reverse();
    let (queue, done) = (Mutex::new(admitted), Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        let streams: Vec<_> = (0..STREAMS)
            .map(|_| {
                scope.spawn(|| {
                    while let Some((base, span, timeout, run)) =
                        queue.lock().ok().and_then(|mut q| q.pop())
                    {
                        // After a 429 the pieces still queued are not requested.
                        if limited.load(Ordering::Relaxed) > 0 {
                            if let Ok(mut done) = done.lock() {
                                done.push((Err(Error("archive_rate_limited")), 0, run));
                            }
                            continue;
                        }
                        let (bytes, read) = get(agent, url, base, span, timeout, stall, limited);
                        let stored = bytes.and_then(|bytes| {
                            let mut blocks = Vec::new();
                            for (s, offset, length, record) in &run {
                                let at = (offset - base) as usize;
                                let raw = &bytes[at..at + *length as usize];
                                blocks.push((
                                    *s,
                                    raw,
                                    store(cache, *s, *offset, *length, record.clone(), raw)?,
                                ));
                            }
                            Ok(blocks
                                .into_iter()
                                .map(|(s, raw, receipt)| {
                                    (s, decode(raw, s, mint).map(|d| (d, receipt)))
                                })
                                .collect::<Vec<_>>())
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
    done.into_inner().unwrap_or_default()
}
fn paths(cache: &Path, slot: u64) -> (PathBuf, PathBuf) {
    let stem = format!("epoch-{}-slot-{slot}", slot / EPOCH_SLOTS);
    (
        cache.join(format!("{stem}.json")),
        cache.join(format!("{stem}.car-range")),
    )
}
fn store(
    cache: &Path,
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
    let (manifest, file) = paths(cache, slot);
    atomic_write(&file, raw)?;
    atomic_write(
        &manifest,
        &serde_json::to_vec(&receipt).map_err(|_| Error("serialization_failed"))?,
    )?;
    Ok(receipt)
}
/// Records an HTTP 429 and its `Retry-After` seconds (the delta form; a date counts as
/// absent) so the source stops requesting, and the run driver can wait before the next batch.
fn rate_limited<B>(response: &ureq::http::Response<B>, limited: &AtomicU64) -> Error {
    let seconds = response
        .headers()
        .get("retry-after")
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0)
        .min(86_400);
    limited.fetch_max(1 + seconds, Ordering::Relaxed);
    Error("archive_rate_limited")
}
/// One admitted range request; also returns the response body bytes read, for the stats.
fn get(
    agent: &ureq::Agent,
    url: &str,
    offset: u64,
    length: usize,
    timeout: Duration,
    stall: Duration,
    limited: &AtomicU64,
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
        429 => Err(rate_limited(&response, limited)),
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
    // No fsync: a cache file is checked against its sha256 on every read and downloaded again
    // if a crash cut it short, and syncing cost as much as downloading it.
    file.write_all(bytes)
        .map_err(|_| Error("archive_cache_write_failed"))?;
    fs::rename(temp, path).map_err(|_| Error("archive_cache_write_failed"))
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
    /// send nothing and are dropped or, with `trickle`, send one byte every 150 ms; with
    /// `limited`, every block request gets a 429. Returns the origin and the block request count.
    fn archive_server(
        index: Vec<u8>,
        car: Vec<u8>,
        stalls: u64,
        trickle: bool,
        limited: bool,
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
                    if limited && path.ends_with(".car") {
                        let body = r#"{"code":"too_many_requests"}"#;
                        let head = format!(
                            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 7\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(head.as_bytes());
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
        let (origin, car_requests) = archive_server(index.clone(), car.clone(), 1, false, false);
        let (mut one, _dir) = source(origin);
        assert_eq!(one.chunk(10).unwrap().0, car[100..110]);
        assert_eq!((one.stats.requests, one.stats.retries), (3, 1));
        for slot in [11, 12] {
            let at = 100 + 10 * (slot as usize - 10);
            assert_eq!(one.chunk(slot).unwrap().0, car[at..at + 10]);
        }
        assert_eq!((one.stats.requests, one.stats.cached_chunks), (3, 3));
        assert_eq!(car_requests.load(Ordering::SeqCst), 2);
        // A cached block cut short (written without fsync) is downloaded again.
        fs::write(one.paths(11).1, b"cut").unwrap();
        assert_eq!(one.chunk(11).unwrap().0, car[110..120]);
        assert_eq!((one.stats.requests, one.stats.cached_chunks), (5, 3));
        assert_eq!(one.chunk(11).unwrap().0, car[110..120]);
        assert_eq!(one.stats.cached_chunks, 4);
        // Two stalls: the slot fails after one retry instead of retrying again.
        let (origin, car_requests) = archive_server(index.clone(), car.clone(), 2, false, false);
        let (mut two, _dir) = source(origin);
        assert_eq!(two.chunk(10).err(), Some(Error("archive_request_failed")));
        assert_eq!((two.stats.requests, two.stats.retries), (3, 1));
        assert_eq!(car_requests.load(Ordering::SeqCst), 2);
        // A trickle that never stalls a read still misses the piece's own 2-second deadline.
        let (origin, _) = archive_server(index.clone(), car.clone(), 1, true, false);
        let (mut slow, _dir) = source(origin);
        slow.stall = Duration::from_millis(500);
        let began = Instant::now();
        assert_eq!(slow.chunk(10).unwrap().0, car[100..110]);
        assert_eq!(slow.stats.retries, 1);
        assert!(began.elapsed() < Duration::from_secs(4));
        assert_eq!(piece_deadline(4 * 1024 * 1024), Duration::from_secs(6));
        // A 429 fails the slot as rate-limited, records Retry-After, and nothing more is
        // requested: not a retry, not the next slot.
        let (origin, car_requests) = archive_server(index, car, 0, false, true);
        let (mut limited, _dir) = source(origin);
        assert_eq!(limited.chunk(10).err(), Some(Error("archive_rate_limited")));
        assert_eq!((limited.stats.requests, limited.stats.retries), (2, 0));
        assert_eq!(limited.chunk(11).err(), Some(Error("archive_rate_limited")));
        assert_eq!(limited.stats.requests, 2);
        assert_eq!(car_requests.load(Ordering::SeqCst), 1);
        let diagnostics = limited.diagnostics();
        assert_eq!(diagnostics["rate_limited"], true);
        assert_eq!(diagnostics["retry_after_seconds"], 7);
    }
    #[test]
    fn blocks_decoded_as_their_pieces_arrive_equal_blocks_decoded_from_the_cache() {
        use crate::archive::tests::{fixture, slot_range};
        let value = fixture();
        let s = value["result"]["slot"].as_u64().unwrap();
        // s-2 <- s-1 <- s <- s+1 <- s+2, adjacent in the CAR from offset 100.
        let (mut car, mut index) = (vec![0; 100], vec![0; ((s % EPOCH_SLOTS + 3) * 12) as usize]);
        for (hash, slot) in (s - 2..=s + 2).enumerate() {
            let raw = slot_range(&value, slot, slot - 1, hash as u8 + 1, true);
            let at = ((slot % EPOCH_SLOTS) * 12) as usize;
            index[at..at + 8].copy_from_slice(&(car.len() as u64).to_le_bytes());
            index[at + 8..at + 12].copy_from_slice(&(raw.len() as u32).to_le_bytes());
            car.extend(raw);
        }
        let (origin, _) = archive_server(index, car, 0, false, false);
        let dir = tempfile::tempdir().unwrap();
        let source = |network| {
            let mut source = ArchiveSource::new(dir.path(), false, Limits::default()).unwrap();
            (source.network, source.origin) = (network, origin.clone());
            source.plan(crate::live::USDC, s - 1, s + 2);
            source.threads = 4;
            source
        };
        let mut online = source(true);
        let fetched: Vec<_> = (s - 1..=s + 2).map(|x| online.block(x)).collect();
        // Every planned block was decoded during the prefetch; only the slot whose read
        // started it is read back.
        assert_eq!(online.stats.cached_chunks, 1);
        let mut offline = source(false);
        let cached: Vec<_> = (s - 1..=s + 2).map(|x| offline.block(x)).collect();
        assert!(fetched.iter().all(Result::is_ok));
        assert_eq!(fetched, cached);
        // One batch read as two runs: the second run's window downloads while the first is
        // served, and one index request covers the batch.
        let dir = tempfile::tempdir().unwrap();
        let mut batch = ArchiveSource::new(dir.path(), false, Limits::default()).unwrap();
        (batch.network, batch.origin, batch.threads) = (true, origin.clone(), 4);
        batch.ahead(s - 1, s + 2);
        batch.plan(crate::live::USDC, s - 1, s);
        let mut ahead: Vec<_> = (s - 1..=s).map(|x| batch.block(x)).collect();
        assert!(batch.next.is_some());
        batch.plan(crate::live::USDC, s + 1, s + 2);
        ahead.extend((s + 1..=s + 2).map(|x| batch.block(x)));
        assert_eq!(ahead, cached);
        // One index request and one piece per window, plus s-2 (the parent outside the
        // batch) read alone: its index record and its block.
        assert_eq!((batch.stats.requests, batch.stats.retries), (5, 0));
    }
    #[test]
    fn offline_cache_miss_never_attempts_a_network() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = ArchiveSource::new(dir.path(), false, Limits::default()).unwrap();
        assert_eq!(source.block(468265447).unwrap_err().0, "archive_cache_miss");
        assert_eq!(source.stats.requests, 0);
    }
}
