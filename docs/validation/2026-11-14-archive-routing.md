# Archive/replay routing, archive batches and overlap: offline validation

Issue #31, 2026-11-14. Three stacked PRs: routing (#74), archive worker (#75) and
overlap reconciliation with scheduling (this PR). **Offline only.** No LaserStream,
Old Faithful or RPC request was made; `replay-route`, `replay-archive` and
`replay-monitor` were not run against providers. Nothing here measures archive
publication delay, replay retention, throughput, cost or 30-day completeness.

## Environment and commands

macOS ARM64, rustc 1.100.0, PostgreSQL 18.6 and Kafka 4.1.1 in local throwaway
containers (`INDEXER_TEST_DATABASE_URL`, `INDEXER_TEST_KAFKA_BROKERS`).

```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo clippy --all-targets --locked --features archive -- -D warnings
cargo test --locked
cargo test --locked --features archive
```

At the overlap commit (`d7e116c`), all passed: fmt; clippy default and archive;
default suite 82 library tests (3 ignored subprocess helpers) and every
integration suite; archive suite 107 library tests and every integration suite.
The routing commit passed the same checks with 68/93 library tests and the
archive-worker commit with 74/99. A deliberate mutation that made the effects
comparison always agree made `differing_effects_conflict_and_a_decoder_revision_change_is_pending`
fail, so that test is not vacuous.

## Offline matrix

| Case | Where | Test |
| --- | --- | --- |
| Archive-only, replay-only, mixed, overlap, gap and unknown routing | #74 | `route::split_routes_by_measured_availability` |
| Recent-only job does no archive work | #74 | `route::recent_job_skips_the_archive_and_measurements_append_revisions` |
| Unavailable archive is unknown, not empty | #74 | same, third revision |
| Changing or expired replay availability | #74, existing | revision 2 of the route test; `replay_availability_records_absent_zero_expiry_and_unfinalized_end` |
| Source-window gap persisted as uncovered | #74 | route test, revision 2 |
| Batch charged before work; failing source keeps its charge | #75 | `a_failing_source_still_consumes_its_charge_and_exhaustion_needs_a_grant` |
| Restart resumes at the first incomplete slot | #75 | `batches_are_charged_before_work_and_resume_at_the_first_incomplete_slot` |
| Crash keeps the charge; one worker per job | #75 | `a_crash_keeps_the_charge_and_one_worker_owns_the_job` |
| Restart-safe budget exhaustion | #75 | exhaustion test |
| Cancellation | #75 | `cancellation_and_overlap_segments` |
| Only the latest route revision is worked | #75 | `only_the_latest_route_revision_is_worked` |
| Overlap in both arrival orders, with retries | this PR | `both_arrival_orders_and_retries_agree_and_verify_the_overlap` |
| Duplicates | this PR, existing | retried replay delivery above; replay job dedupe tests |
| Replay omission, `replay_not_in_archive`, pending | this PR | `replay_omission_conflict_and_pending_are_distinct` |
| `effects_differ` conflict; decoder revision change is pending | this PR | `differing_effects_conflict_and_a_decoder_revision_change_is_pending` |
| Crash at the outcome write | this PR | `a_failed_outcome_write_verifies_nothing_and_a_rerun_recovers` (trigger-injected failure) |
| Revision change invalidates evidence; pruning of the boundary slot | this PR | `revision_changes_and_pruning_invalidate_coverage_but_keep_receipts` |
| Mint predicate parity with the replay filter | this PR | `predicate_matches_the_replay_filter` |
| Scheduling order; a failed step does not stop the next | this PR | `a_cycle_runs_every_step_in_order_even_after_failures` |
| Archive-only route skips replay; reconcile still runs | this PR | `an_archive_only_route_skips_replay_and_still_reconciles` |
| Live priority and live surviving historical failure | existing | `replay_expiry_and_provider_outage_cannot_stop_the_independent_live_worker`; the archive batch now writes through the same gate |
| Cache reuse and corrupted cache rejection | existing | archive transport tests; `ArchiveSource` is unchanged |

The `effects_differ` case inserts an altered archive observation directly, because
shared storage already rejects a conflicting second observation at write time and
then leaves the archive slot incomplete (`pending`), not `conflict`. The overlap
tests build the archive block from the replay fixture's own normalized capture, so
both sides are the same transaction in each source's form.

## Gaps

- **Measured overlap (#31 criterion 4):** exercised only with offline fixtures; not
  measured against live providers. Not satisfied until a bounded provider run,
  which needs separate authorization.
- **Provider checks:** `replay-route` (replay availability, archive tip),
  `replay-archive` (CAR downloads) and `replay-monitor` with CACHE are unrun.
- **Job size:** the 128-slot job bound still applies, so mixed and overlapping
  routes are small windows.
- **Docker:** the runtime does not pass CACHE or run archive batches; that is #32.
- **Metrics:** new signals are JSON status fields and structured events, like the
  existing replay report, not a continuous Prometheus exporter.

## Recovery and rollback

Migrations 6, 7 and 9 are additive: new tables, plus two defaulted columns on
`replay_jobs`. Existing jobs, receipts, budgets and output are untouched; a job
without a route behaves as before. Each migration raises the schema version, and
earlier binaries refuse a newer session. Roll back by restoring the pre-upgrade
PostgreSQL backup into a separate session and using its matching binary. After a
crash, rerun the same command: archive charges stay consumed, `running` attempts
become `interrupted` on the next batch, and an outcome write that failed leaves
the slot unverified until `replay-overlap` runs again.
