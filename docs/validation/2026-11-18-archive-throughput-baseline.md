# Archive bridge throughput baseline, and stalled pieces

**Date:** 2026-11-18 · **Source:** provider (Old Faithful, public mainnet RPC) + local PostgreSQL

## Question

How fast does one `bridge` batch run end to end, downloads included, and where
does the time go? The later throughput changes are measured against this.

## Setup

- Binary: `main` at deccbb7 (#118), `cargo build --release --features archive`.
- Dataset: USDC (`EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`), the 200 slots from
  470447000 (epoch 1088), `INDEXER_HISTORICAL_WORKERS=8`, `INDEXER_BRIDGE_BATCH_SLOTS=200`.
- Each run: `bridge-plan` then one timed `bridge` batch into a fresh session schema on
  local PostgreSQL 18.6. *Cold* deletes the archive file cache first, so every block is
  downloaded; *warm* keeps it, so no archive requests are made.
- Machine: 8-core Apple laptop, 16 GB, home network; load average ~23 from other work,
  so absolute times are noisy. Comparisons alternate binaries run by run.

The 200 slots hold 48,281 non-vote transactions (241 per slot); 4,459 transfer USDC.
The blocks are 101 MB (0.5 MB per slot).

## Baseline (main, deccbb7)

| Run | Seconds | Slots/s | In `block` calls | Archive requests |
| --- | --- | --- | --- | --- |
| cold 1 | 18.1 | 11.0 | 16.7 s | 22 |
| cold 2 | 35.4 | 5.6 | 33.8 s | 23, 1 slot failed |
| cold 3 | 35.5 | 5.6 | 33.8 s | 24, 1 slot failed |
| warm 1–3 | 6.1 / 6.1 / 6.7 | ~32 | 4.7–5.2 s | 0 |

A clean cold batch spends ~12 s downloading (~8.5 MB/s), ~5 s reading and decoding
blocks on 8 threads, and ~1.4 s on everything else, storage included. In cold runs 2
and 3 one range request (slot 470447128) stalled until its 20-second deadline and failed
with `archive_read_failed`: one stall doubled the batch and left a slot for a later
batch.

## Stalled pieces retried (`codex/archive-stall-retry`)

A block piece now fails when a read waits 3 s for bytes or when it exceeds 2 s plus its
size at 2 MiB/s (6 s for 8 MiB), and a failed piece is requested once more within the
batch limits.

Alternating cold runs, main (A) against the branch (B):

| Set | A seconds | B seconds | Notes |
| --- | --- | --- | --- |
| 4 + 4, first build (read timeout and retry only) | 36.6, 18.8, 17.3, 14.0 | 38.1, 17.3, 15.8, 13.8 | Both first runs stalled: A lost a slot (`source_errors` 1); B retried the piece (23 requests) and lost none. That stall trickled bytes, so the read timeout missed it and both waited ~20 s. |
| 6 + 6, with the size deadline | median 14.1 (13.7–15.2) | median 14.4 (14.0–14.6) | No stall in any run: no cost when the network behaves. |

So far the branch is shown to turn a stalled piece into a retry rather than a failed
slot. The size deadline (stall cost ~20 s → ≤ ~6 s plus the retry) is covered by the
local trickle test only, because no trickle happened during the second set.

## Limits

- Old Faithful bandwidth varies from run to run; one range per comparison.
- Stalls are intermittent (2 of 3 baseline runs, 2 of 8 in the first set, 0 of 12 in the
  second), so their rate is not measured here.
- The 2 MiB/s floor is a shortcut: a link slower than 2 MiB/s per stream would fail
  pieces twice.

## Concurrent pieces (`codex/archive-overlap-download`)

A planned window now covers a whole 128-slot bridge subrange, is split into up to 16
pieces of 1–8 MiB (using at most half the requests left), and downloads on 8
concurrent streams. A bridge batch allows 128 archive requests instead of 32; the
128 MiB byte limit is unchanged. The piece rate floor drops to 1 MiB/s per stream.

Old Faithful from this laptop, 8 MiB ranges at random offsets, 3 repeats:

| Streams | 1 | 2 | 4 | 8 | 16 |
| --- | --- | --- | --- | --- | --- |
| MiB/s | 3.9–11.1 | 12.2–17.8 | 16.3–25.2 | 29.4–32.1 | 33.4–36.9 |

One of the 93 requests (4 streams, first repeat) hung for its full 60 s.

Alternating cold runs, the stall-retry branch (A) against this branch (B):

| Variant of B | A median | B median | B requests | B failed slots |
| --- | --- | --- | --- | --- |
| 16 pieces at once, 32-request limit | 14.6 s | 11.2 s | 32 (cap) | 141 (`archive_download_limit`) |
| Pieces from the request budget, 32-request limit | 15.2 s | 13.0 s | 27–32 | 82 in one run |
| 16 streams, 128 requests, 2 MiB/s floor | 15.1 s | 12.5 s | 40–67 | 18 in one run: retries spent the 128 MiB byte limit |
| **8 streams, 16 pieces, 128 requests, 1 MiB/s floor** | **14.8 s** | **12.9 s** | 40–41 | **0** |

The final variant: 6 + 6 runs, 13.9–15.2 s (A) and 12.5–15.6 s (B); time in `block`
calls 13.3 → 11.4 s median; 4,459 transactions decoded in every run. About 11 → 15.5
slots/s on a clean cold batch.

What is left: each subrange is downloaded completely before its blocks are decoded, so
download and decode still take turns. Overlapping them is the next download change.
