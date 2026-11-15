# Recovery with the provisional PostgreSQL pipeline

Restore live intake before repairing retained work. Preserve the session schema
with a consistent PostgreSQL snapshot taken while writers are stopped (for example
`pg_dump --schema=SESSION`). Do not delete raw input because a cursor has advanced.
Committed PostgreSQL raw input (`live_raw`) is the durable handoff that `live-retry`
and historical repair read. With `INDEXER_KAFKA_BROKERS` configured, live input is
also published to Kafka; the producer cursor follows delivery confirmation, and the
topic does not replace the PostgreSQL raw records.

## First response

1. Inspect `live-status SESSION` and `coverage SESSION MINT START END`. A run left `running`
   after process exit is unfinished. Check the observed reference/replay ages,
   requested start, replay margin, unresolved age, and historical holes. None of
   these establishes verified live coverage by itself.
2. Restart the bounded `live` command from an explicitly selected available replay
   slot, or use `overlap` for live intake plus bounded historical repair with
   live-priority write admission. These network commands remain opt-in. If replay
   no longer contains a gap, preserve it as unknown and recover through retained
   raw inputs or a verified historical source; never advance past it to look healthy.
3. Run `live-retry SESSION` to process at most 100 unresolved retained protobuf records
   without a network. Repeat bounded batches as needed while observing reports.
   This is manual repair. A separately launched retry process relies on PostgreSQL
   transaction locking; it does not inherit the combined coordinator's in-process
   write gate. Avoid parallel retry operators.
4. Rerun a bounded historical range. Completed/skipped slots are bypassed. Raw
   blocks retained before cancellation or interrupted acknowledgments can be
   reused. Other source/decode failures may refetch; raw persistence is not a
   declaration of successful decoding or complete coverage.
5. Compare unique queries, exact summary volume, unresolved counts/ages, and the
   contiguous historical checkpoint. Only successfully decoded, committed block
   work or verified skip evidence can complete a historical slot.

## Retry rotation and diagnostics

Previously, every retry batch selected the oldest 100 unresolved records; an
unsupported prefix could indefinitely hide newer recoverable input. The additive
`live_retry_cursor` table retains the last selected raw ID across process restarts.
Selection begins after that ID and wraps to earlier unresolved records when it
reaches the end of the finite backlog. It is a scheduling cursor, never a source,
output, or coverage acknowledgment. Continuous arrivals faster than repair can
still prevent catch-up; there is no production scheduling/latency guarantee.

The cursor commits before attempting an item. A process exit there can delay that
item until wraparound; its raw bytes and pending/failure status remain intact.
Output still commits atomically before raw status becomes decoded. Repeating
that work deduplicates immediately. Original `received_at`/`first_pending` ages
are never refreshed by retries.

`live-retry` reports attempted, decoded, failed, cursor, run ID, and unknown
coverage. Counts are attempts; use `summary` for unique transfer totals. The
`retry_batch_started` and `retry_batch_completed` events share recovery context
and run ID. `live-status` reports the scheduling cursor and batch limit separately
from verified boundaries. Invalid stored protobuf/schema can terminate a batch
with a bounded error; the advanced fairness cursor lets a subsequent invocation
reach later items without erasing the failing input.

Opening a pre-change database adds only the cursor table. Read-only status still
works before that table exists. Older binaries ignore it; rollback restores their
oldest-first retry behavior. No transfer records or decoder revisions migrate.

## Verified failure boundaries and limits

Offline tests launch child processes which exit without Rust destructors:

- Before raw commit: no durable acknowledgment exists; the source must redeliver.
- After raw commit: retained pending input decodes on restart.
- After output commit, before raw acknowledgment: restart repeats the input and
  preserves one logical transfer and exact volume.
- After raw acknowledgment: replay does not add another logical transfer.
- After retry-cursor commit: pending input remains discoverable on wraparound.
- Historical raw persistence and partial transaction output: restart reuses
  retained blocks without an available source, preserves pending age, and does
  not move the checkpoint across unfinished work.
- After historical completion: restart skips finished work and keeps queries stable.

PostgreSQL trigger-injected tests in `tests/backfill.rs` additionally cover failed
raw writes and failed coverage acknowledgments. A closed diagnostics
pipe is exercised in a child process; retry still commits its transfers. There
is no exporter queue to exhaust. A stalled synchronous stderr reader can still
block logging; no asynchronous telemetry delivery or latency guarantee is claimed.

These checks do not simulate power loss, storage hardware corruption, SIGKILL
inside PostgreSQL's own commit code, or production failover. This is a partial slice
of ticket #5. A real provider replay-boundary outage recovery, independent source
bridge (#17), automated repair supervision, and continuously refreshed recovery
monitoring remain open. Decoder revision replacement is handled by the explicit
[offline repair commands](decoder-repair.md); it does not certify full live coverage.

## Kafka crash matrix

Each crash point below has a test in `src/kafka.rs`. Consumer cases kill a real child process with SIGKILL.
Producer cases reproduce the state a crash leaves (stored cursor and topic contents),
because the stream loop has no offline source yet (#59).

| Crash point | What survives | Restart behavior |
| --- | --- | --- |
| Producer, before publish | Stored cursor below the input | Resume from `resume_from` (cursor − 150) re-receives it |
| Producer, after publish, before its cursor is stored | The input in the topic, old cursor | The input is produced again; the consumer stores it once |
| Consumer, during its batch write | Nothing: output and positions commit in one transaction | The batch is consumed again once the session timeout (10 s) moves the dead member's partitions |
| Consumer, after its batch committed | Output and positions | Nothing already stored is consumed again |
| Consumer group rebalance mid-run | Stored positions; a revoked member drops any unwritten batch | The joining member consumes from stored positions |

In every case each transfer is stored once and live coverage stays `unknown` with no
verified boundary. Delivery and stored positions are processing progress, never
coverage, as the [replay contract](replay-jobs.md) requires of filtered input. A clean
shutdown leaves the group at once; only a crash waits for the session timeout.
