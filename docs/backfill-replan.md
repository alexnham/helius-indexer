# Archive, historical replay and live ingestion: corrected implementation plan

Status: implementation plan, 2026-11-13. This corrects source routing to match the
original brief and the user's explicit direction: **recent history uses LaserStream
historical replay**. The existing RPC backfill is a validation/fallback tool, not
the normal recent-history pipeline. This plan does not claim that the corrected
runtime is implemented or authorize a full 30-day run or paid infrastructure.

## Source contract

```text
Old Faithful / maintained Jetstreamer ── older available history ─┐
                                                               ├─ shared durable input
LaserStream historical replay ── recent history / outage repair ─┤  → shared decoder
                                                               │  → deduplicated storage
LaserStream live subscription ── current finalized intake ──────┘  → queries + coverage

Coordinator: durable jobs, availability measurements, source handoff,
            retries, resource budgets, processing receipts and coverage
```

Use `finalized` for all three paths, as agreed in `PROPOSAL.md`. Use the maintained
pinned Jetstreamer adapter already present; do not copy the older sample runner
interface from the brief without checking its current API. Keep one Rust package.
Kafka, PostgreSQL, ClickHouse and object storage remain candidates, not new
prerequisites for this validation milestone.

| Work | Normal source | Evidence and limits |
| --- | --- | --- |
| Older requested history | Old Faithful CAR ranges through Jetstreamer | Measure available archives; retain integrity receipts and execution metadata; unsupported input stays pending |
| Recent requested history | LaserStream `fromSlot` replay with `matchMints` | Check actual replay availability before each subscription; durably retain inputs before acknowledging processing |
| Current activity | LaserStream live intake | Start/restore promptly; archive work must not delay or monopolize live writes |
| Corroboration or exceptional repair | Bounded full-block RPC checks | Explicitly report verification/fallback use; do not silently replace normal replay with a block-by-block RPC scan |

The brief's 10–20-hour archive delay and roughly 48-hour replay window are not
hard routing constants. Measure the last available archive position and
`SubscribeReplayInfo.first_available`, record observation times, and recheck before
network work. An absent replay-availability value means unavailable; zero is a
valid slot. Availability can change between checking and subscribing.

For each finite requested range, identify its archive and replay portions from
those observations. Prefer an overlap when both can supply it. If neither source
can supply an interval, persist and display that gap; keep live intake running.
Do not skip ahead and call the gap repaired. RPC fallback requires a separately
visible, bounded action rather than an automatic unbounded scan.

Expect that gap routinely, not as an exception: archives publish at epoch end
plus a delay, while replay holds roughly 48 hours, so for part of each epoch
neither source supplies the interval between them. **Unresolved decision:** whether
a bounded RPC fill of exactly that measured interval may run automatically, or
always needs an operator action. Until decided, it needs the operator action.

Schedule the oldest replayable pending interval first, ahead of archive work.
Replay availability only moves forward; archive availability does not expire.

The first recent-only test remains **50 slots**, supplied by replay. It need not
fetch archive data merely to exercise an archive code path. Validate archive
routing separately against a small older range and the measured source handoff.

## Supervision and restart behavior

Run the live worker and historical scheduling as independent tasks within the
existing package. Start live first; allow at most one historical worker initially.
The historical worker can run an archive batch or replay job without waiting for
the first live cycle to finish, and can resume after any live-cycle boundary.
Reuse live-priority write admission, bounded buffers and explicit cancellation.
Do not turn this into separate ingestion processes sharing only database locks and
then claim the existing in-process priority guarantee still applies.

Each job durably records:

- Its ID, mint/filter scope, decoder revision, immutable requested start/end and
  creation time. Restart must not substitute a newly measured tip for its range.
- Source-availability observations and routing revision, pending intervals,
  failures, retry eligibility, scheduling position and processing receipts.
- Separately acknowledged raw input, decoded output and any verified coverage.
- Consumed requests/bytes/attempts and elapsed budgets. Claim bounded work before
  issuing requests so a crash/restart cannot reset its resource allowance.

Rules the abandoned RPC experiment broke, which the job model must enforce:

- Reject a scope whose requested range cannot finish within its maximum budget,
  or let an explicit, recorded operator action replenish the budget. A job must
  not be accepted and then end only in permanent exhaustion.
- Compare a request with the existing job before writing anything. A rejected or
  conflicting request leaves budgets, job rows and other plans unchanged.
- Re-attempting or repairing a slot under the same decoder invalidates its
  processing receipt; a receipt names the attempt it describes.

Adding a mint or deliberately requesting a different range creates a new scope.
A decoder correction invalidates applicable evidence rather than inheriting
completion from the old decoder. Retained unresolved inputs retry without a
provider when possible; deleted inputs explicitly require refetch.

A replay subscription can catch up and transition to current activity. Sharing it
with the live role is allowed only if doing so preserves live priority and durable
recovery. Otherwise keep live intake separate while a bounded replay worker fills
the recent gap. Measure extra subscription traffic and duplicates; stable transfer
identities make storage idempotent, not provider usage free.

## Coverage and retention rules

The implementation must distinguish four different facts:

1. A source delivered input and it was durably retained.
2. Decoding/output commits succeeded for those inputs.
3. A range has valid completion evidence for the active mint/filter/decoder.
4. That verified output is still retained and eligible for a query.

The largest observed transfer slot, a finalized slot notification, an SDK's
in-memory cursor or a quiet filtered stream is not sufficient slot-completion
evidence. Before implementing a replay checkpoint, document the actual provider
ordering/completion contract and test it. If it cannot establish full filtered
slot completion, expose processing/replay progress while leaving verified
coverage unknown. Empty produced slots, skipped slots and unobserved slots must
remain distinct. A small successful comparison is not full-range reconciliation.

Resume from durable, defensible recovery state with inclusive replay/deduplication;
never advance a complete checkpoint from `MAX(received_slot)`. Missing evidence
must remain visible, even if it makes repeated recovery more conservative.

Preserve the approved local-testing cap: **the newest 1,000 transfers across all
mints after each ingestion cycle**, configurable; counts may grow during a cycle.
Keep compact job/scheduling receipts independently of prunable transfer/raw data.
An old job must not disappear or endlessly reprocess successful work just because
its output was intentionally pruned. A processing receipt may say that an input
was handled; it must not certify coverage for data that no longer exists.
Pruning invalidates affected retained coverage, including a partial boundary slot.
The production 30-day query window and provisional 37-day raw-retention requirement
remain unchanged and cannot be demonstrated with this small count cap.

## Implementation milestones and acceptance

Deliver three coherent milestones, with checks alongside their behavior. Do not
add more prerequisites to the existing open PR stack before reconciling its bases.
Existing RPC/archive validation commands and evidence remain available.

1. **Replay recovery and durable jobs.** First, as its own small change, establish
   the pinned provider contract: whether filtered replay can distinguish an empty
   slot from an unobserved one. If it cannot, record which source supplies
   coverage evidence before building the job model. Then, as separate PRs,
   implement bounded replay for recent requested ranges, immutable job scopes,
   durable recovery/processing state and independent live/history scheduling.
   Verify with real PostgreSQL: duplicates, out-of-order input, decoder failures,
   crashes around input/output/job acknowledgments, restart of the same 50-slot
   scope, expiry of replay availability and provider outage without losing live
   priority. Tests must reject a fake complete checkpoint based on a later slot.
2. **Archive routing and measured handoff.** Connect the maintained archive adapter
   to those jobs; route older ranges to archives and recent ranges to replay.
   Verify overlapping transaction identities/semantics, failed archive availability,
   a gap between source windows, retries that survive restart, budget exhaustion,
   and processing receipts that survive pruning without retaining false coverage.
   Existing RPC corroboration is optional evidence, not the primary replay source.
3. **Docker operation and inspection.** Add a separate archive-enabled image target
   with its compatible pinned toolchain/native build dependencies. Preserve the
   offline console, runtime-only credentials and retained session data. Ship query
   and diagnostic APIs plus UI showing archive/replay/live state separately,
   frozen requested range, pending intervals, failures, processing receipts,
   retained coverage and exhausted budgets. Test shutdown/restart, missing secrets,
   persisted job identity and queries against both native and container builds.

Every milestone ships applicable events and metrics from `observability.md`:
source/replay availability and age, retry/exhaustion reasons, raw versus decoded
progress, unresolved input, pending age, scoped coverage, retention invalidation,
resource usage and unknown freshness. Mint/slot/signature values belong in scoped
diagnostics, not unbounded metric labels. Provider exceptions must not expose
credentials or endpoints. A failed exporter must not undo an input/output commit.

Ordinary tests remain offline. First provider checks are explicitly opt-in and
bounded: a 50-slot recent replay with a 60-second/10,000-message ceiling, and a
separate archive check of at most eight slots, 32 requests, 128 MiB response bodies
and 120 seconds. Bound total attempts as well as per-request work; stop at the
budget and persist the remainder. These are functional checks, not a full-window
backfill, cost benchmark or authorization for indefinite historical downloads.
Existing continuous live operation has its separate user authorization.

Run required formatting, default/archive Clippy and offline test suites for code
changes; include storage failure injection and meaningful query results. Record
actual counts, failures and unknowns. Verify container image architecture/build
prerequisites before presenting Docker support as ready.

## Current implementation and migration

The recent-only runtime now uses frozen PostgreSQL replay jobs with independent
live and historical workers. Issue #31 adds measured archive/replay routing, charged
archive batches and overlap reconciliation to those jobs ([replay jobs](replay-jobs.md));
Docker wiring of that path is #32. The existing archive/
RPC bridge validates full-block evidence and remains useful for optional checks.
The unfinished uncommitted runtime experiment using RPC as the normal recent
source was removed from build paths; a local ignored copy is retained for reference.
No corrected runtime, schema migration or backfill was deployed as part of this
replan. Preserve other work in the checkout and keep credentials/data out of commits.

The next code change is milestone 1. It must be designed against this source
contract, rather than expanding the RPC validation command again. `src/live.rs`
already subscribes with `from_slot` and `match_mints`, checks
`SubscribeReplayInfo.first_available` and records it in `live_runs`; extend that
intake with a finite end instead of adding a parallel replay path. Ship additive
job state with an explicit backup/rollback policy; older binaries cannot enforce
new replay-job/receipt semantics merely because they can open the database.

## Primary references and unresolved evidence

Rechecked 2026-11-13 using the Helius documentation MCP and the original pasted
brief. [Historical replay](https://www.helius.dev/docs/laserstream/historical-replay)
describes `fromSlot` and roughly 48 hours of availability;
[SubscribeReplayInfo](https://www.helius.dev/docs/api-reference/laserstream/grpc/subscribereplayinfo)
defines actual earliest availability and its race with subscribing;
[mint filtering](https://www.helius.dev/docs/laserstream/mint-filtering) is honored
in replay. [Delivery guarantees](https://www.helius.dev/docs/laserstream/delivery-guarantees)
need explicit completion/ordering interpretation for the pinned client before
checkpoint implementation. [Old Faithful](https://docs.old-faithful.net/) describes
epoch-end archive publication and its expected delay; actual availability must be
measured. [Existing archive validation](archive.md) records the pinned maintained
Jetstreamer adapter and measured metadata limitations.

Still unverified for the corrected runtime: replay slot-completion evidence,
measured archive/replay overlap at execution time, sustainable processing and
catch-up rates, replay/live subscription overhead, production retention sizing,
and monthly total cost. These are evidence gaps, not reasons to silently choose
RPC as the recent-history default.
