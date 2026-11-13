# Bounded Old Faithful validation

Verified against primary sources and mainnet on 2026-11-12. This is an opt-in
validation adapter, not a production backfill scheduler. It uses the maintained
[Vaultic Jetstreamer](https://github.com/vaultic-win/jetstreamer/tree/8f91a4a7c918674f8799a9c887ace0f7f39fb844)
firehose library at v0.10.0, commit
`8f91a4a7c918674f8799a9c887ace0f7f39fb844`. The
[archived Anza repository](https://github.com/anza-xyz/jetstreamer) documents the
maintenance handoff. Only `jetstreamer-firehose` is enabled, without its default
features; the facade's ClickHouse integration and Geyser loader are unnecessary.
The optional dependency still brings a substantial Agave dependency tree.

Build with Rust 1.98.1 or newer, a C/C++ compiler and CMake for native dependencies:

```sh
CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo build --locked --features archive
mkdir -p data/archive
INDEXER_ARCHIVE_OPT_IN=1 target/debug/helius-indexer archive data/archive.sqlite data/archive EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v 468265447 468265447
# Offline replay into another database, without provider credentials:
target/debug/helius-indexer archive-replay data/replay.sqlite data/archive EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v 468265447 468265447
INDEXER_ARCHIVE_OPT_IN=1 target/debug/helius-indexer archive-overlap data/bridge.sqlite data/archive EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v 468265447 468265447
INDEXER_ARCHIVE_OPT_IN=1 target/debug/helius-indexer archive-tip data/archive
```

`SOLANA_RPC_URL` optionally selects the existing mainnet RPC source. The default
is public mainnet RPC. Credentials remain environment-only; caches, reports and
SQLite databases belong under ignored `data/`. Do not commit raw CAR payloads
or private endpoints. The ordinary build and tests require no archive access.

## Limits and cancellation

Each command allows at most eight requested slots, 32 archive HTTP requests,
128 MiB of archive response bodies, 16 MiB per raw slot range, and 120 seconds.
Requests are sequential, with no retries or redirects and a maximum 20-second
individual request deadline, shortened by the remaining command budget.
The bridge additionally allows at most eight 64 MiB RPC block responses.
Normalized blocks and metadata decompression have explicit size limits.
`BACKFILL_CANCEL_FILE` stops scheduling at slot boundaries; an in-flight request
finishes or times out. The archive-live command uses the same coordinator and
write gate as RPC history: live writes have priority, with history yielding
between transactions. Its duration cannot extend the archive deadline. It also
requires the existing live opt-in and Helius environment credentials. This
validation did not open a paid live stream or execute a full-window backfill.

## Input and recovery contract

The fixed public mirror is `files.old-faithful.net`. Its modern slot-range index
uses a little-endian u64 CAR offset and u32 byte length per slot. A zero record
is unavailable evidence, never a verified skipped slot. Strict HTTP 206 and
Content-Range checks reject servers that return a full epoch instead of a range.
The parent slot range supplies the previous block hash.

The adapter verifies CAR CIDs (SHA-256), block/entry/transaction links and ordering,
slot and transaction indices, dataframe integrity and bounded decompression.
Jetstreamer's wire decoder supplies legacy, v0 and v1 transactions. Protobuf
execution metadata passes through the existing live normalizer and deterministic
transfer decoder. Missing metadata, older unsupported metadata encodings,
missing account history and unsupported transfer instructions remain unknown.
Archive account updates are unavailable; current account state is never used to
invent historical ownership or extension state.

A raw range and a small receipt preserve epoch, slot, offset, length, index record,
pinned reader revision and SHA-256. Atomic writes publish the receipt last. A
missing receipt can be refetched; a conflicting or corrupt acknowledged cache
fails closed. Offline replay makes zero HTTP requests. Capture provenance retains
the source receipt. Keep both files for reproducibility.

SQLite adds source-specific completion acknowledgments and immutable block
identity (hashes, parent, known time and ordered transaction signatures). Existing
raw historical observations lazily initialize that identity ledger, subject to
bounded reads. Conflicting source identities cannot add transfer rows. A completed
RPC slot does not skip the first archive observation, or vice versa. Both flow
through existing transaction reconciliation, stable transfer IDs and coverage.
Source acknowledgment and decoded output are durable separately. A deliberate
bridge recheck resets coverage to unknown until that observation succeeds.
The migration is additive; retain a database backup before rollback because old
binaries cannot enforce the new source audit. No stored transfers are deleted by
the migration.

The bridge first compares block anchors, ordered transactions and shared compiled
execution context. It ingests both sources, and `overlap_verified` requires both
observations to decode completely. A last-source coverage result alone does not
prove bridge success. Unsupported metadata or truncated RPC logs leave the
bridge unverified. The live tail remains explicitly unknown.

## Actual bounded results

The sample at finalized slot 468265447 (parent 468265446) downloaded 3,312,686
archive bytes in four HTTP requests, including index records and the parent
range. It contained 472 transactions: 147 legacy, 122 v0 and 203 v1. The shared
decoder persisted 407 transfer records without decode errors. Initial development
execution took 4.198 seconds on this local Mac; this is not a throughput benchmark.

Public RPC returned the same block hashes, parent and ordered transactions.
However, two transactions had 194 archive log lines versus 130 RPC lines followed
by `Log truncated`. The shared decoder correctly reported
`incomplete_execution_evidence`; this comparison did not establish verified
overlap coverage. Two neighboring bounded probes encountered existing unsupported
transfer-fee and authority-change instructions. These are explicit decoder gaps,
not empty slots. Offline tests exercise both arrival orders and replays against
real SQLite, corruption, missing metadata and synthetic version variants.

A separate tip snapshot observed finalized reference slot 468625614 at block time
1794508090, epoch 1084 absent (HTTP 404), and epoch 1083 present. Its last indexed
slot was 468287999, block time 1794435498: a 337,615-slot / 72,592-second freshness
gap. This used five archive requests and 2,086,115 response bytes. It measures
availability at one instant, not publication delay after epoch end. The
[Old Faithful documentation](https://docs.old-faithful.net/) describes epoch-end
publication and an expected 10–20-hour delay; repeated observations would be
needed to measure publication delay. `archive-tip` probes at most four epochs and
the last 128 index records. Unknown HTTP responses remain unknown, so a failed
probe never proves absence or an exact tip.

A further bounded sample at slot 468265451 verified successfully against public
RPC: matching anchors, ordered transactions and shared execution context; both
observations decoded completely, leaving 51 deduplicated transfer records. This
probe used 1,006,003 archive response bytes, two HTTP requests and one cached
parent range; RPC returned 3,104,749 bytes. Its one-slot historical coverage is
verified and the live tail remains unknown. A fourth unsuccessful sample at
468265450 also retained incomplete execution evidence. Across these experiments,
five requested sample slots were examined; no epoch CAR was downloaded in full.

A production archive-to-recent-RPC scheduler remains subsequent work. No production throughput, full
30-day coverage, full historical account state, or production infrastructure
selection is claimed.

## Verification evidence

Local Rust 1.99 on Apple Silicon, development/test debug information disabled:
`cargo fmt --check`, default and archive-feature Clippy (`--all-targets -- -D
warnings`), and both complete offline test suites passed. The default suite
passed 74 tests; archive feature passed 82, with four existing opt-in/helper tests
ignored in each suite. CI includes separate default and archive jobs.

The final binary replayed retained slot 468265447 into a fresh SQLite database:
472 transactions / 407 transfers, complete historical coverage, two cached chunks,
zero HTTP requests or response bytes, zero decode/write errors. Its normalized
input was 2,936,990 bytes including the added provenance receipt. Neither local
retained data nor credentials are included in the repository. Reproduction of a
live probe depends on the mirror/RPC availability and metadata returned at that
time; offline captured tests remain deterministic.

The [source-boundary coordinator](source-boundary.md) now supplies a frozen
measured plan, source routing, durable retry scheduling and unfinished-interval
reporting. Its newer bounded mainnet check intentionally retains unsupported
boundary decoding as incomplete. Continuous operation remains unvalidated.

The [explicit RPC evidence recovery](rpc-evidence-recovery.md) path can now
corroborate some truncated RPC observations with full matching archive runtime
logs. Reports retain native overlap verification separately from recovered
readiness. `archive-overlap-replay DB MINT START END` checks retained original
pairs offline, with no CAR download or provider fallback.
