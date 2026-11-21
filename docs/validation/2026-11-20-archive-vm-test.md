# Archive backfill on a datacenter VM, and the batch bottlenecks it exposed

**Date:** 2026-11-20 · **Source:** provider (Old Faithful, public mainnet RPC) + PostgreSQL

## Question

Does a datacenter VM run the archive backfill faster than the laptop, and what limits a
batch once the network is fast? Part of #124.

## 1-hour VM test

- **VM:** Vultr `vc2-8c-32gb` (8 shared vCPU, 32 GB, 640 GB SSD) in Chicago, created with
  `infra/vultr` and destroyed afterwards. It cost $0.54.
- **Binary:** `main` at df097b8, built on the VM with `--features archive`.
- **PostgreSQL:** the compose service, limited to 4 CPUs and 16 GB with
  `shared_buffers=8GB`.
- **Run:** `scripts/archive-backfill.py`, USDC from slot 470447000,
  `--slots-per-batch 200`, `INDEXER_HISTORICAL_WORKERS=8`, cancelled after 60 minutes.

Old Faithful bandwidth from the VM, measured with N parallel 8 MiB range requests at
random offsets of `1088/epoch-1088.car` (3 repeats each):

| Streams | 8 | 16 | 32 |
| --- | --- | --- | --- |
| MiB/s | 62–77 | 119–131 (one stalled repeat: 22) | 128–132 |

From the laptop, the same streams levelled off at about 33 MiB/s (2026-11-18 baseline).

Backfill results:

| | VM |
| --- | --- |
| Batches | 301, 59,369 slots in 3,785 s: **15.7 slots/s** |
| Per 5 minutes | 15–19 slots/s, with no decline over the hour |
| Downloaded | 31.6 GB, 0.53 MB/slot, **8.4 MB/s on average** |
| PostgreSQL | 21 GB, 355 KB/slot |
| Slots | 59,362 complete; 617 `archive_download_limit`; 12 `missing_token_metadata`; 7 skipped; 1 `invalid_transfer_instruction`; 1 `unsupported_token_instruction` |

The VM used about a tenth of the bandwidth available and about 40% of its CPU. A batch
took a median of 12.2 s, and the wrapper's `bridge-status` call takes 0.07 s, so the time
was inside `bridge`.

**Not tested:** the 30-day `--days 30` plan failed from the VM (`time-window discovery
failed`; the public RPC probe likely throttles datacenter addresses), so the run started
from a fixed slot.

## Where a batch's time goes (laptop)

The VM gave no per-stage timings, so the archive source was timed on the laptop. This
used a temporary build with timers (not committed), over cold 200-slot batches. Figures
are per batch:

| Stage | Seconds |
| --- | --- |
| Download stage, wall clock (8 streams) | 6.0–7.3 |
| … waiting on HTTP, summed across streams | 23–30 |
| … writing cache files with `fsync`, summed across streams | 12–27 |
| … decoding, summed across streams | ~3 |
| Index requests, one per 128-slot window | 0.5–1.1 |
| Linking and normalizing | < 0.6 |

Two findings:

- **`fsync` cost as much as downloading.** Each cached slot cost four `fsync` calls (the
  file, its receipt, and the directory after each).
- **Download and write took turns.** A window's writes (~2 s per batch) waited for its
  download, and the next download waited for those writes.

## Changes (`codex/archive-throughput`)

1. **Cache files are written without `fsync`.** They are still renamed into place, and
   every read checks them against their receipt and sha256. A cut-short file is now
   downloaded again instead of failing its slot. That case only arises when the network
   is enabled; offline repair still fails closed.
2. **The next window downloads while the current one is written.** The bridge tells the
   archive source its batch range (`BlockSource::ahead`). One index request covers the
   batch. Pieces the background download loses are fetched when their slot is read.
3. **The per-batch archive budget scales with the batch:** 1 MiB and half a request per
   slot, at least 128 MiB/128 requests, at most 2 GiB/2048 requests. Streams stay at 8
   (see the second VM run).

## Results (laptop, alternating cold runs)

Ranges from 470447000, with all versions in each round:

| Version | 200-slot batch, median of 9 | Slots/s |
| --- | --- | --- |
| `main` | 10.27 s | 19.5 |
| + no `fsync` | 8.69 s | 23.0 |
| + next window during writes | 7.34 s | 27.2 |

| Version, 2 rounds | Slots/s |
| --- | --- |
| `main`, 200-slot batches | 18.0, 19.2 |
| all changes, 200-slot batches | 23.7, 27.8 |
| all changes, **1000-slot batches** | **35.2, 37.8** |

**Equivalence:** over slots 470447000–470447599, `main` at 200 and the branch at 200 and
at 1000 wrote the same 32,437 transfers (identical row hash). The slot statuses were also
identical: 599 complete, 1 `missing_token_metadata`.

## Second VM run, and rate limiting

Same VM type and start slot. The branch at 758d208 (with 16 streams and 32 pieces per
window), 1000-slot batches, cancelled after 15 minutes. It cost $0.27; an attempt an hour
earlier was abandoned when every Old Faithful request, from the laptop too, returned HTTP
429 "too_many_requests".

| Batch | Seconds | Slots failed with `archive_range_not_honored` (429) |
| --- | --- | --- |
| 1 | 26 (**38 slots/s**) | 0 |
| 7 | 37 (27 slots/s) | 0 |
| the other 13 | 49–87 | 80–312 of 1,000 |

Over the run: 12,645 slots in 923 s (13.7 slots/s), and 2,354 slots left as
`archive_range_not_honored`.

- **Clean batches** matched the laptop's 1000-slot rate.
- **Rate-limited batches** took twice as long and lost 8–31% of their slots.
- **The limit is intermittent:** minutes after the run, the VM got 206 while the laptop got
  429.
- **The code misfiles the error:** a 429 is reported as `archive_range_not_honored`, and
  nothing backs off.

16 streams doubled the pieces, and so the requests, per window. The stream count is back
at 8 here.

**Limits:**

- The laptop is network-bound at about 20 MB/s, so the higher ceiling is not shown here.
- The VM rate is bounded by Old Faithful's rate limiting until 429s are classified and
  backed off.
- A batch that stops early does not count the window still downloading in its reported
  bytes (at most one window, ~66 MB).

## Third VM run: `main` with rate-limit backoff

Same VM type and start slot. `main` at 18985a3 (#133 and #135: 8 streams, look-ahead,
rate-limit backoff), 1000-slot batches, cancelled after 20 minutes. It cost $0.28.

| | |
| --- | --- |
| Batches | 38, 37,993 slots in 1,273 s: **29.8 slots/s** |
| Per 3 minutes | 28.0–32.3 slots/s, with no decline |
| Rate limiting | none: 0 `archive_rate_limited` slots and no waits |
| Downloaded | 20.3 GB, 0.53 MB/slot, 16 MB/s |
| PostgreSQL | 13 GB, ~340 KB/slot |
| Slots | 37,986 complete; 7 `missing_token_metadata`; 7 skipped |

Against the first VM run (`main` at df097b8, 200-slot batches), throughput rose from 15.7
to 29.8 slots/s. At this rate 30 days (≈12.1M slots) takes about 4.7 days. Disk remains the
constraint: about 4 TB of PostgreSQL, plus the download cache unless it is evicted (#124).
Whether Old Faithful rate-limits a multi-day run is not known from 20 minutes.
