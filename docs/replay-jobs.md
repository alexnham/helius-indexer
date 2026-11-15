# Bounded LaserStream replay jobs

Issue #30 uses the [verified replay source contract](research/2026-11-13-laserstream-replay-contract.md).
The native commands and provider-backed runtime now use the same bounded
LaserStream intake, shared decoder and PostgreSQL storage. The runtime starts
live intake before historical work, then schedules one replay worker independently
within the same process and write-admission gate. Recent backfill no longer uses
getBlock scans. Old Faithful routing is issue #31; its first slice measures and records routes.

## Durable boundaries

Migration 0004 adds frozen jobs, attempts and compact input receipts. Job identity
fixes the mint, inclusive start/end and decoder revision. Recreating an existing
job is idempotent only for that original scope/revision. Scope rejection does not
move the range or consume budget.

Each explicit grant permits 60 seconds, 10,000 transaction messages, 64 MiB of
transaction wire payloads and three connection attempts, with at most 64 grants.
A worker reserves its entire requested time before contacting a provider and
charges each connection before network I/O. Time stays charged on termination,
including crashes; restarting never refills it. Received messages/bytes are charged
even when subsequent raw persistence fails. These counters describe processing
allowances, not exact provider billing or total HTTP/2 transport bytes.

A PostgreSQL session advisory lock permits one active attempt per job. Connection
death releases ownership, not consumed budgets. A subsequent attempt records the
previous running attempt as interrupted. This is single-job ownership, not the
future distributed range-leasing implementation from issue #37.

Raw wire input and its pending receipt commit together. Decoding/output occurs
afterward through the existing intake contract; only its durable acknowledgment
can mark a receipt decoded. A crash between those boundaries leaves retryable
work. Transport metadata changes deduplicate using normalized evidence; changed
execution evidence goes through the shared reconciler and conflicts fail closed.
Unresolved raw input may be refetched if pruning removed it; decoded receipts survive pruning.

Status uses one repeatable-read snapshot and distinguishes unique receipts,
decoded/unresolved receipts and raw inputs still retained. None proves output
retention or range coverage. Observing a transaction past the requested end records
an observation only: verified coverage and boundary remain unknown.

## Compatibility and recovery

Migrations 0004 and 0005 are additive and preserve existing input/output tables.
Main uses versions 2/3 for Kafka; the unpublished replay prototypes used those
numbers for different tables. Migration 4 supplies any missing Kafka tables and
retains existing replay tables. Migration 5 adds missing input provenance without
resetting scope, receipts or charged budgets. This supports both main and the
version 2/3 replay prototypes; migrations 6–8 add routing, archive attempts and overlap outcomes; the current schema version is 8. Raw rows referenced by replay receipts are classified as replay;
missing historical attempt/hash/acknowledgment provenance stays null. Back up the
session using PostgreSQL tooling before upgrading. Earlier binaries reject newer
schema versions (the current version is 8); rolling back requires restoring the
pre-upgrade backup into a separate session and using its matching binary. Do not manually decrement the migration
version or delete receipts to make an older binary open an upgraded session.
The new tables currently have no deletion/TTL policy; job receipt retention will
be decided with the completed replay/archive runtime.

## Commands and inspection

All commands require `DATABASE_URL`; SESSION is a PostgreSQL schema name.

```sh
helius-indexer replay-create SESSION recent MINT START END
helius-indexer replay-status SESSION recent
helius-indexer replay-retry SESSION recent
# Explicit provider opt-in and HELIUS_API_KEY are required for these two commands:
INDEXER_LIVE_OPT_IN=1 helius-indexer replay-route SESSION recent CACHE
INDEXER_LIVE_OPT_IN=1 helius-indexer replay SESSION recent 60
INDEXER_LIVE_OPT_IN=1 helius-indexer replay-monitor SESSION recent LIVE_FROM 60 [CACHE]
# Offline: reconcile routed overlap slots from retained archive and replay input
helius-indexer replay-overlap SESSION recent
# Archive feature, INDEXER_ARCHIVE_OPT_IN=1: one charged batch of routed archive slots
INDEXER_ARCHIVE_OPT_IN=1 helius-indexer replay-archive SESSION recent CACHE
# Explicitly replenish an exhausted allowance; never done automatically:
helius-indexer replay-grant SESSION recent
helius-indexer archive-grant SESSION recent
```

The first test range is 50 slots. Creation accepts at most the existing 128-slot
range bound; mint and integer validation apply before opening the session. Restart
reuses the same job, inclusive range and revision. Offline retries rotate through
at most 100 pending receipts in a ten-second admission window, loading only one
bounded wire payload at a time. Missing raw input reports `replay_input_needs_refetch`.
All provider attempts recheck actual replay availability and record the observation
(and absence versus zero) before subscribing. A later-slot observation terminates
an attempt, not the job's unknown coverage. Unused reserved time remains charged.

`replay-monitor` reports the live and historical workers separately. History starts
after live initialization, without waiting for a live cycle to finish. Historical
expiry, failure or exhausted budgets do not end the live worker. Gate waits are
bounded; the runtime supervisor kills/reaps a timed-out native subprocess. An
in-flight database operation can require cleanup beyond the network intake window.

Status includes frozen scope/revision, attempt history (last 20), availability
age/margin, failure classes (at most 16), pending age, processing and retained-raw
counts, queryable transfer count in scope, and remaining budgets. Retained transfer
count does not prove complete slots. Live raw-input counters exclude replay input.
The console's `/api/status` exposes this report and `/api/transfers` queries actual
retained output.

## Source routing

`replay-route` measures actual replay availability (`SubscribeReplayInfo`) and the
finalized reference, then probes the Old Faithful archive tip only when replay
cannot serve the whole job, so a recent-only job does no archive work. Without the
`archive` feature, or when the probe fails or the tip is not exact, the archive
side is unknown. The command needs replay credentials; with
`INDEXER_LIVE_OPT_IN=1` and the archive feature it may also make the bounded
`archive-tip` requests (at most four epoch probes under the default archive limits, plus a finalized reference from `SOLANA_RPC_URL`) using CACHE.

Each measurement appends a revision (migration 6): replay first-available, reference
slot, archive probe result and segments. Archive serves slots up to its last
available slot, replay serves first-available through the reference, both is
`overlap`, and neither is `uncovered`. Nothing is hard-coded about archive lag or
replay retention. Earlier revisions are kept so later evidence can be tied to the
revision it was routed under. Routes are not progress or coverage: the `replay`
worker still serves only ranges inside the replay window, archive segments are not
yet processed, and overlap is a routed kind, not reconciled evidence. Those are the
next slices of #31. The 128-slot job bound still applies, so mixed and overlapping
routes cover small windows only.

Migration 6 is additive; an older binary refuses the newer schema, so roll back as
above by restoring the pre-upgrade backup.

## Archive batches

`replay-archive` takes the latest route revision and processes the first up to
eight incomplete slots of its `archive` or `overlap` segments, within one segment,
through the shared History path: retained raw input, shared decoder and storage,
and per-slot `complete`/`skipped`/`unknown` status from full blocks. A job with no
route is refused; a job whose route has no pending archive slots charges nothing.

Archive work has its own allowance (migration 7), separate from replay's. Before
any cache or network work, one transaction charges a whole batch and records a
`running` attempt with the ceilings it runs under: 32 requests, 128 MiB of
response bodies and 120 seconds, the existing per-batch limits. A grant allows four
batches; `archive-grant` adds one grant, up to 64, so a job can run at most 256
batches. `replay-grant` never replenishes archive work. A batch served entirely
from cache is still charged. Observed requests, response bytes and cached chunks
are written when the batch ends (finished, incomplete, cancelled or an error
class); they are diagnostics, not provider billing. A crash keeps the charge and
leaves the attempt `running` until the next batch marks it `interrupted`. A
PostgreSQL advisory lock allows one archive worker per job; it is released when
the worker's connection closes, including on a crash. `BACKFILL_CANCEL_FILE` or the
120-second limit stops between slots.

Status adds an `archive` block: grants, batches charged and remaining, attempts and
response bytes by status, the last 20 attempts, and per routed archive/overlap
segment the complete, skipped and pending slot counts with oldest pending age.
Replay segments get no counts. Full-block slot status for an overlap segment is
archive evidence only until `replay-overlap` reconciles it (below). Migration 7 is
additive; roll back as above.

## Overlap reconciliation and verified slots

`replay-overlap` makes no provider request. For each slot of the latest route's
`overlap` segments it compares retained archive and replay captures through the
shared reconciler and decoder, and appends an outcome (migration 8) tied to the
route and decoder revision:

| Outcome | Meaning |
| --- | --- |
| `consistent` | Every replay-delivered signature is in the archive block with identical decoded transfers (identity, raw amounts, fees, confidentiality, instruction positions); block time is compared only when both sides have it. |
| `replay_omission` | As above, and the archive block has mint transactions replay never delivered. Archive is authoritative on membership. |
| `conflict` | `replay_not_in_archive` (a replay signature missing from a complete archive block) or `effects_differ`. Fails closed and stays visible. |
| `pending` | The archive slot is incomplete, a replay input is unresolved or no longer retained, or the stored output's decoder revision differs. |

"Mint transaction" uses the replay filter's predicate: the mint is a static or loaded
account key, or the mint of a pre/post token balance.

The report's `verified` block derives verified slots under the latest route:
`archive` slots whose full-block status is complete or skipped with raw archive
input still retained, and `overlap` slots whose latest outcome is `consistent` or
`replay_omission` and whose archive evidence is still retained. Replay-only and
uncovered slots stay unknown, because filtered replay cannot prove a slot complete.
`verified_through` is the contiguous verified boundary from the job start and never
crosses an unverified slot. A new route revision or decoder revision stops earlier
outcomes from counting. Pruning removes the archive slot evidence, so verified slots
fall back to unknown while outcome receipts remain. The job-level `coverage` field
stays `unknown`; read `verified` for slot evidence.

`replay-monitor` runs one historical cycle beside live intake under the shared
live-priority gate: retained retry, a replay attempt when the route has replay or
overlap slots (or the job has no route), one archive batch when the binary has the
archive feature and CACHE is given, then overlap reconciliation. A failed or
exhausted step is reported and the next step still runs; none can stop live intake.
Migration 8 is additive; roll back as above.

## Scope still pending

Provider-measured overlap, replay coverage outside archive overlap, full 30-day
retention, Kafka, distributed leases and production benchmarking remain
separate work. Budget exhaustion requires an explicit grant; current replay
contract cannot infer which quiet filtered slots were empty. Repeated inclusive
replay can cost provider traffic even when storage deduplicates it.

## Offline evidence (2026-11-13)

On macOS ARM64 with local PostgreSQL 18.6, formatting and default/archive Clippy
passed. Full default/archive offline Rust suites passed with the first four new
regressions (101 and 126 tests respectively, three ignored subprocess helpers
each). After adding the persisted allowance-limit regression, all five replay
regressions and default Clippy passed again. Tests inject a receipt-write failure,
prove raw/receipt atomicity while the received-work charge survives, reject
conflicting scope/input, retain processing receipts after raw deletion, preserve
allowances across connection death/restart and test upgrade compatibility.
These were prototype checks; the current native/runtime slice and additional
recovery/upgrade regressions are recorded in the
[2026-11-14 validation](validation/2026-11-14-replay-jobs.md). Its bounded provider
check preserved scope and budgets across restart, but exhausted its allowance
before finishing the requested range. Coverage remains unknown.
