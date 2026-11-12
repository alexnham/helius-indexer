use super::{JETSTREAMER_REVISION, decode_slot};
use crate::{Error, Result, backfill::BlockSource};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
const ORIGIN: &str = "https://files.old-faithful.net";
const MAX_CHUNK: usize = 16 * 1024 * 1024;
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
    pub stats: Stats,
}
impl ArchiveSource {
    pub fn new(cache: &Path, network: bool, limits: Limits) -> Result<Self> {
        if limits.bytes == 0
            || limits.bytes > 128 * 1024 * 1024
            || limits.requests == 0
            || limits.requests > 32
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
            stats: Stats::default(),
        })
    }
    fn fetch(&mut self, path: &str, offset: u64, length: usize) -> Result<Vec<u8>> {
        if !self.network {
            return Err(Error("archive_cache_miss"));
        }
        if length == 0
            || length > MAX_CHUNK
            || self.stats.requests >= self.limits.requests
            || self.stats.response_body_bytes + length as u64 + 1 > self.limits.bytes
        {
            return Err(Error("archive_download_limit"));
        }
        let remaining = Duration::from_secs(self.limits.seconds)
            .checked_sub(self.began.elapsed())
            .filter(|d| !d.is_zero())
            .ok_or(Error("archive_time_limit"))?;
        let end = offset
            .checked_add(length as u64 - 1)
            .ok_or(Error("archive_invalid_range"))?;
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(remaining.min(Duration::from_secs(20))))
            .max_redirects(0)
            .http_status_as_error(false)
            .build()
            .into();
        self.stats.requests += 1;
        let mut response = agent
            .get(format!("{ORIGIN}/{path}"))
            .header("Range", format!("bytes={offset}-{end}"))
            .header("Accept-Encoding", "identity")
            .call()
            .map_err(|_| Error("archive_request_failed"))?;
        match response.status().as_u16() {
            403 => return Err(Error("archive_access_denied")),
            404 => return Err(Error("archive_object_unavailable")),
            206 => {}
            _ => return Err(Error("archive_range_not_honored")),
        }
        let range = response
            .headers()
            .get("content-range")
            .and_then(|h| h.to_str().ok())
            .ok_or(Error("archive_invalid_content_range"))?;
        validate_range(range, offset, end)?;
        if response
            .headers()
            .get("content-encoding")
            .is_some_and(|h| h != "identity")
        {
            return Err(Error("archive_unexpected_encoding"));
        }
        let mut bytes = Vec::new();
        let read = response
            .body_mut()
            .as_reader()
            .take(length as u64 + 1)
            .read_to_end(&mut bytes);
        self.stats.response_body_bytes += bytes.len() as u64;
        read.map_err(|_| Error("archive_read_failed"))?;
        if bytes.len() != length {
            return Err(Error("archive_truncated_range"));
        }
        Ok(bytes)
    }
    pub(super) fn probe_epoch(&mut self, epoch: u64) -> Result<bool> {
        if !self.network {
            return Err(Error("archive_cache_miss"));
        }
        if self.stats.requests >= self.limits.requests {
            return Err(Error("archive_download_limit"));
        }
        let remaining = Duration::from_secs(self.limits.seconds)
            .checked_sub(self.began.elapsed())
            .filter(|d| !d.is_zero())
            .ok_or(Error("archive_time_limit"))?;
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .timeout_global(Some(remaining.min(Duration::from_secs(20))))
            .max_redirects(0)
            .http_status_as_error(false)
            .build()
            .into();
        self.stats.requests += 1;
        let response = agent
            .head(format!("{ORIGIN}/{epoch}/epoch-{epoch}.car"))
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
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Error("archive_runtime_failed"))?;
        runtime.block_on(super::decode::block_time(&raw, slot))
    }
    fn chunk(&mut self, slot: u64) -> Result<(Vec<u8>, Receipt)> {
        let epoch = slot / 432000;
        let stem = format!("epoch-{epoch}-slot-{slot}");
        let manifest = self.cache.join(format!("{stem}.json"));
        let file = self.cache.join(format!("{stem}.car-range"));
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
        let receipt = Receipt {
            schema_version: 1,
            jetstreamer_revision: JETSTREAMER_REVISION.into(),
            epoch,
            slot,
            offset,
            length,
            index_record: record,
            sha256: hex(&Sha256::digest(&raw)),
        };
        atomic_write(&file, &raw)?;
        atomic_write(
            &manifest,
            &serde_json::to_vec(&receipt).map_err(|_| Error("serialization_failed"))?,
        )?;
        Ok((raw, receipt))
    }
}
impl BlockSource for ArchiveSource {
    fn diagnostics(&self) -> serde_json::Value {
        serde_json::json!({"jetstreamer_revision":JETSTREAMER_REVISION,"response_body_bytes":self.stats.response_body_bytes,"requests":self.stats.requests,"cached_chunks":self.stats.cached_chunks,"limits":{"response_body_bytes":self.limits.bytes,"requests":self.limits.requests,"seconds":self.limits.seconds}})
    }
    fn provenance(&self) -> &'static str {
        super::SOURCE
    }
    fn capture_source(&self) -> &'static str {
        super::SOURCE
    }
    fn block(&mut self, slot: u64) -> Result<Vec<u8>> {
        let (raw, receipt) = self.chunk(slot)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Error("archive_runtime_failed"))?;
        let (mut block, parent, _) = runtime.block_on(decode_slot(&raw, slot))?;
        let (parent_raw, _) = self.chunk(parent)?;
        let parent_hash = runtime.block_on(super::decode::parent_hash(&parent_raw, parent))?;
        block["previousBlockhash"] = json!(parent_hash);
        let bytes = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"result":block,"archive_provenance":{"jetstreamer_revision":receipt.jetstreamer_revision,"epoch":receipt.epoch,"slot":slot,"car_offset":receipt.offset,"car_length":receipt.length,"car_sha256":receipt.sha256}}))
            .map_err(|_| Error("serialization_failed"))?;
        if bytes.len() > crate::backfill::MAX_BLOCK_BYTES {
            return Err(Error("archive_normalized_block_limit"));
        }
        Ok(bytes)
    }
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
    fn offline_cache_miss_never_attempts_a_network() {
        let dir = tempfile::tempdir().unwrap();
        let mut source = ArchiveSource::new(dir.path(), false, Limits::default()).unwrap();
        assert_eq!(source.block(468265447).unwrap_err().0, "archive_cache_miss");
        assert_eq!(source.stats.requests, 0);
    }
}
