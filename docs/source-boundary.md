# Measured archive / recent RPC boundary

The optional archive feature now includes a resumable bounded coordinator for
issue #17. It freezes a measured archive tip and a finalized RPC reference into
an additive SQLite plan, then routes older positions to the archive and newer
positions to RPC. It never infers historical completeness from filtered live
transactions. This is a finite validation scheduler, not an unattended production
30-day job.

## Commands

Build as described in [archive validation](archive.md), using Rust 1.98.1+:

```sh
export INDEXER_ARCHIVE_OPT_IN=1
# START must be at or before the measured archive tip.
target/debug/helius-indexer bridge-plan data/bridge.sqlite data/archive MINT START
# One bounded batch; repeat explicitly to resume.
target/debug/helius-indexer bridge data/bridge.sqlite data/archive MINT START
# Offline diagnostic; does not fetch, move boundaries or acknowledge coverage.
target/debug/helius-indexer bridge-status data/bridge.sqlite MINT START
```

Plans are keyed by mint, decoder revision and requested start. They retain the
complete measured boundary snapshot and measurement timestamp. Repeating
`bridge-plan` resumes the existing plan without silently changing its endpoint.
A plan may describe at most 16 million positions; creating it downloads no full
window and allocates no row per future position. A decoder revision requires a
new plan. The plan rejects an unknown/uncertain tip, a start beyond the tip, or
an archive tip ahead of the finalized reference.

`bridge-live DB CACHE START SECONDS` runs the same bounded historical batch with
USDC LaserStream intake through the existing live-priority write gate. Create the
plan first: this command does no archive tip discovery before starting the live
worker. It requires the existing live opt-in and environment credentials. Live
starts inclusively at the frozen finalized reference, not at the old archive
start. A stale plan can be outside the provider's actual replay window; the
existing replay check reports that failure and does not silently skip the gap.
Its stream tail remains unknown even when the historical plan is complete.
Extending a frozen endpoint and operating continuously remain subsequent work.

## Scheduling and proof

Each invocation selects at most eight consecutive positions, starting at pending
work at/after a durable rotating scheduling cursor, then wrapping to the earliest
pending interval. Source/decode failures remain pending. The cursor advances
across selected work even after a mid-batch time limit, allowing unrelated work
and later produced children to run. A cancellation already present at entry does
no work. Cursor movement never moves the verified contiguous checkpoint.

The tip position itself is fetched from both sources, compared using the existing
block/transaction/execution-context comparison, and decoded through both shared
storage paths. Only then is an overlap proof acknowledged, tied to the exact
historical attempt generation. A crash before that acknowledgment or a later
retry invalidates readiness. Source acknowledgments alone do not constitute that
proof. Current decoder coverage, required-source acknowledgments, and the
boundary proof all participate in the reported historical checkpoint.

Immutable block identities also check parent hashes against already retained
parents and children, in either arrival order. Contradictory links fail before
transfer writes. Valid completed child links can resolve previously attempted
skipped positions across source/batch boundaries; an observed produced block is
never overwritten as skipped. Each pass resolves only its selected eight
positions. Source-independent skip evidence can be reused without requesting the
same unavailable position again. Eight consecutive skipped slots cannot trap the
scheduler before the child that proves them.

The status report returns the contiguous boundary, pending position count,
missing interval count, next pending scheduling position, original plan age and
up to 32 unfinished intervals split into archive/RPC portions. Larger reports set
`ranges_truncated`; counts include all intervals. Sparse durable completion is
streamed from SQLite rather than expanding a multi-million-position vector.
The separate live-tail object begins at the frozen reference plus one and always
reports unknown coverage. The storage summary retains its existing all-stored
scope; historical completion here applies only to the frozen plan.

## Resource limits and recovery

A batch is single-worker historical work: at most eight unique requested slots,
plus the second observation at the boundary. Archive limits remain 32 requests,
128 MiB response bodies and 16 MiB per raw range. RPC allows at most nine block
responses, each bounded by 64 MiB (plus its bounded genesis check). These are
upper transport bounds, not measured usage or billed provider bytes. Sources do
not retry within the batch. Per-request deadlines are at most 20 seconds and
shrink to the remaining historical network budget: 120 seconds, or the shorter
live duration. CPU decoding and durable writes may finish after that network
budget; this is not a hard wall-clock process kill. `BACKFILL_CANCEL_FILE` is
checked between scheduling/transaction operations. Gate waits, SQLite's existing
five-second busy timeout, and decoder work retain their existing limits.

Planning is a separate bounded tip probe; a batch never repeats it. Cached raw
ranges and receipts follow the [archive integrity/replay contract](archive.md).
Historical inputs, outputs, source acknowledgments, scheduling cursor and overlap
proof remain separate durable state. Restart reuses verified work and retries
unresolved work without duplicate query output. No perpetual retry loop, full
backfill, database service or paid infrastructure is introduced.

The migration adds `bridge_plans` and `bridge_overlap_proofs`; existing transfer
rows are unchanged. Older binaries ignore these tables and cannot enforce these
boundary proofs. Back up the SQLite database before downgrading. Existing
synthetic multi-block test headers were corrected to link their parent hashes;
no captured on-chain transaction fixture changed.

## Verification

Deterministic offline tests use real SQLite and the sanitized captured transfer.
They exercise both routing portions, exact duplicate-free output, empty produced
blocks, a 16-position skipped run, rotating work after cancellation, failed RPC
and contradictory overlap/parent data, a trigger-injected crash after output but
before proof, generation invalidation, frozen/revision-scoped plans, a million-
position sparse report and expired RPC deadlines before network access. Existing
live admission-priority tests still cover the shared coordinator gate.

Primary references rechecked 2026-11-12:
[Old Faithful epoch publication](https://docs.old-faithful.net/),
[Solana getBlock](https://solana.com/docs/rpc/http/getblock), and
[LaserStream delivery guarantees](https://www.helius.dev/docs/laserstream/delivery-guarantees).
Archive availability is observed, not inferred from the documented publication
expectation; finalized full blocks supply historical completion evidence, while
filtered stream delivery alone does not close the live tail.

### Local and bounded mainnet evidence (2026-11-12)

On local Apple Silicon with Rust 1.99, formatting and Clippy passed for both
feature configurations. Full offline suites passed 74 default and 95 archive
checks, with four existing opt-in/helper tests ignored in each configuration.
Commands used `--offline --locked`; Clippy used `--all-targets -- -D warnings`.
Development/test debug information and incremental compilation were disabled.

A bounded public-provider check used a fresh ignored SQLite database/cache and
USDC. A tip probe selected archive slot 468287999. `bridge-plan ... 468287999`
froze finalized reference 468643125 at block time 1794511860, with a measured
355,126-slot / 76,362-second freshness gap. Planning reused one cached CAR chunk,
made three archive requests and read 1,536 response-body bytes. The preceding tip
probe used five requests / 2,086,115 bytes. Neither number is publication delay.

One `bridge` batch examined only positions 468287999–468288006. Archive/RPC block
comparison passed at the boundary, but the archive decoder encountered
`unsupported_authority_change`; the proof remained unacknowledged. RPC verified
468288000–468288002, encountered the same unsupported case at 003–004, and failed
requests at 005–006. The report retained 355,124 pending positions, an unknown
contiguous boundary and unknown live tail. The scheduling cursor selected
468288007 next. The subsequent offline status command reproduced this state.
Six retained input observations occupied 9,162,855 normalized bytes; RPC stage
metrics reported 591 decoded transactions / 407 decoded transfer attempts and
3,692,840 input bytes. Those attempts are not unique stored transfer counts or
complete transport/billing measurements. No full-window download, automatic
follow-on batch or paid live stream ran.

The real check demonstrates routing and accurate partial readiness, rather than
a claim that this archive tip is fully decodable. The smaller successful shared
range in [archive validation](archive.md) remains the positive mainnet overlap
example. Fixing unsupported authority changes and validating a production
scheduler/continuous boundary extension remain separate work. Inspect specific
bounded gaps with the existing `coverage DB MINT START END` command.

### Explicit corroborated readiness

[RPC evidence recovery](rpc-evidence-recovery.md) adds a separate readiness path
for explicit log truncation with complete matching archive evidence. Native
`overlap_verified` stays false in that case; `overlap_recovered` and `boundary_ready`
identify recovered readiness. The native RPC source audit stays unknown. Proofs
still bind to the current attempt generation, and the live tail stays unverified.
The earlier mainnet snapshot above records the behavior before this recovery path;
it is not relabeled retroactively.
