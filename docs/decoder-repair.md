# Decoder revision repair

Revision changes are explicit, offline operations. Ordinary ingestion still
rejects a same-signature observation from a different stored decoder revision.
The repair commands always use the decoder compiled into the current binary;
there is no user-supplied output or arbitrary replacement revision.

## Procedure

Make a consistent SQLite backup before in-place repair, using the backup API or
stopping writers and preserving WAL state. Successful corrections delete obsolete
transfer rows. Restoring that backup is the supported data rollback; simply
running an older binary is unsafe because it ignores unresolved-repair visibility.
Do not run old and new writer binaries against the same database. Recognized
newer numbered decoder revisions reject downgrade attempts without changing data.

```sh
cargo run --offline --locked -- repair-status DB START_SLOT END_SLOT
cargo run --offline --locked -- repair DB START_SLOT END_SLOT
# Repeat bounded capture batches while observing progress.
cargo run --offline --locked -- repair-history DB MINT START_SLOT END_SLOT
# Select normalized archive receipts instead of RPC receipts:
cargo run --offline --locked -- repair-history-archive DB MINT START_SLOT END_SLOT
cargo run --offline --locked -- live-retry DB
cargo run --offline --locked -- coverage DB MINT START_SLOT END_SLOT
```

Both repair paths accept inclusive ranges of at most 128 slots and check
`REPAIR_CANCEL_FILE` between units of work. Capture repair selects at most 100
captures per invocation, preferring fewer-attempted records so unresolved items
cannot repeatedly hide untouched work in a finite backlog. A current transaction
already verified by capture repair is skipped by later capture batches. Current-
revision unresolved markers remain eligible: compatible metadata arriving through
ordinary ingestion does not remove the marker until explicit repair reverifies it.

`repair-history` processes the selected full retained RPC blocks.
`repair-history-archive` selects full normalized archive blocks in the same
SQLite history, preserving archive provenance. It requires neither the optional
archive SDK feature nor a CAR cache; missing archived blocks remain unknown.
Source selection is explicit, so missing RPC input does not silently fall back
to a different source. It can import
transactions rejected by an earlier decoder that never reached the capture table.
It can also use compatible richer block metadata to resolve old captures. It has
no network fallback: missing retained blocks remain unknown with
`retained_block_unavailable`. Existing block response/transaction-count bounds
apply, and one historical worker processes the range serially. History can reuse
retained block bytes across revisions; reports distinguish cached blocks from
source attempts. This is a separate full-block workload, not a 100-capture batch.
Since 2026-11-18, retained blocks keep only signatures for transactions that do not
touch the mint, so this replay covers decoder changes for the mint's candidates; a
correction to the mint filter itself needs those blocks fetched again from the source.

A capture-only repair does not acknowledge `live_raw` input statuses. Existing
`live-retry` replays retained protobuf through the current decoder and updates
those acknowledgments separately. Inputs needing richer metadata can remain
unresolved; missing historical account/extension state is never invented.

## Atomic replacement and visibility

For each capture, one immediate SQLite transaction reads retained canonical
metadata and compatible audited observations, runs the pure decoder, and commits
replacement output, the capture revision/status, and repair progress together.
The whole transfer set is replaced: records can be added, amounts/fees changed,
false records removed, or all output deleted. Stable IDs remain based on the
same chain signature and instruction positions; revision is not part of identity.
No new chain requests or current account lookups occur.

At most 32 audited observations totaling 64 MiB are considered per capture, each
bounded to 16 MiB; canonical capture/reference bounds also apply. Already
quarantined `capture_conflict` receipts are not accepted as repair evidence.
Other incompatible known fields leave the correction unresolved. Longer available
log evidence is preserved, and known runtime failure evidence is not discarded.
Unsupported schemas, formats, and evidence remain explicit failures. These input
bounds are not a production throughput or peak-memory guarantee.

If decoding cannot complete, original raw inputs and physical prior transfer rows
remain retained, but the unresolved repair marker hides those rows from `query`
and `summary`. This applies to failed block repair too. Later successful repair
replaces the rows and supersedes older unresolved markers so they cannot resurrect
stale output after a revision change. Reconciled revision-conflict receipts are
marked `revision_reconciled`; genuine quarantined conflicts remain quarantined.
A storage transaction failure rolls everything back, including visibility and
progress. Original pending timestamps survive repeated failed attempts.

Each transaction commits independently. During a partial range repair, untouched
older records may still be visible with their stored decoder revision. Summaries
expose `target_decoder` and `other_decoder_transfer_count`; use `repair-status`
for scope completion. This is not an atomic switch of the entire range.

## Coverage and diagnostics

`repair-status` reports target revision, selected slot range, retained captures,
remaining other-revision captures, corrected/unresolved counts, first unresolved
time and age, and up to 16 failure-class counts. `repair_complete` describes only
retained captures and remains false for empty/unproven capture scopes. It never
certifies a full slot range. Older databases can be inspected read-only before
the new progress table exists.

Correction invalidates any existing current-revision completed historical slot
at that position, across mint scopes. Only a full retained-block replay can
verify produced/empty/skipped coverage again. The repaired stream tail stays
unverified even when captured transfers decode successfully.

Capture batches return attempted/corrected/unresolved and replaced/written row
counts, cancellation, a run ID, and scoped progress. Structured correction and
batch events include bounded classes, revision, slot, source/mode, and matching
run correlation, without signatures, raw payloads, credentials, or metric labels
per transaction. Block replay reports cached blocks, source attempts, decode/write
failures, and scoped coverage using the same run correlation.

## Compatibility and verification

The additive `decoder_repairs` table records prior/target revision, status,
error class, attempts, and original pending time. Existing raw inputs/audits are
preserved; compatible canonical metadata may be enriched. Successful replacement
is in-place and needs a backup to restore exact previous output. There is no
production migration, autonomous scheduler, cross-process write-priority guarantee,
or recovery from missing/corrupt original storage. Coordinate manual repair with
live intake; SQLite writer locks/busy deadlines remain the concurrency boundary.

Offline regressions cover added, changed, removed and empty output, failed
transactions, failed write/revision acknowledgment rollback, process exit between
commits, repeated repair, bounded scheduling, older read-only schemas, compatible
metadata enrichment, retained-block-only replay, missing blocks, failed repair
visibility, CLI event correlation, and newer-revision downgrade rejection.
See [the measured in-place validation](validation/2026-11-12-revision-repair.md).
