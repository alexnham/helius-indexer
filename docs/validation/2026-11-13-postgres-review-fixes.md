# PostgreSQL review fixes

Validation started 2026-11-13 on macOS ARM64 against the local PostgreSQL 18.6
container. No provider connections or paid infrastructure were used.

## Concurrency regression

`concurrent_parent_child_conflict_is_rejected_before_both_anchors_commit` starts
two PostgreSQL connections together, with a trigger delaying anchor insertion.
They observe adjacent slots with incompatible parent hashes. Before the fix both
could commit. Removing the new advisory lock makes the regression fail; restoring
it makes exactly one observation succeed and leaves zero inconsistent parent links.
The lock is held through transaction completion and covers absent anchors too.
It serializes identity observations within a session; throughput was not measured.

## Readiness and summaries

`health_is_read_only_and_never_creates_or_repairs_a_session` checks missing schema,
ready schema and missing transfer table against real PostgreSQL. `health SESSION`
does not call migration code. The console uses a three-second subprocess limit.
Health reports dependency availability only; chain coverage remains unknown.

Summary accumulation uses the PostgreSQL row iterator and existing checked u128
math. Existing exact-amount/unknown-fee tests apply. This removes client-side
materialization of all matching rows; no latency or memory benchmark is claimed.

## Validation state

Before reconciliation with the latest parent commits, formatting, default Clippy
and the full default offline Rust suite passed (95 tests; three ignored helpers).
The Python console/runtime suite passed all 12 tests. An isolated ARM64 image
passed the Docker smoke test, including restart persistence and network-disabled
seeding/replay through the shared PostgreSQL Unix socket. Those Docker results
precede final reconciliation and do not prove the final image.

Final-head default/archive and Python checks are running again after reconciliation
with parent PR #44 at `301023c`. CI supplies the separate Linux/AMD64 checks.
PR #45 remains draft until these results are recorded. No full-window backfill,
production deployment, hosted PostgreSQL TLS validation or performance benchmark
was performed. No schema migration or destructive data operation is introduced
by this follow-up.
