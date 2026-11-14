# Replay jobs: PostgreSQL implementation in progress

Issue #30 uses the [verified replay source contract](research/2026-11-13-laserstream-replay-contract.md).
The current branch implements storage primitives only; the provider worker,
offline retry runner, CLI/API and runtime scheduling are not connected yet.
This does not replace the current runtime's bounded RPC validation backfill.

## Durable boundaries

Migration 0002 adds frozen jobs, attempts and compact input receipts. Job identity
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
work. Duplicate signatures with changed input hashes fail closed. Unresolved raw
input may be refetched if pruning removed it; decoded receipts survive pruning.

Status uses one repeatable-read snapshot and distinguishes unique receipts,
decoded/unresolved receipts and raw inputs still retained. None proves output
retention or range coverage. Observing a transaction past the requested end records
an observation only: verified coverage and boundary remain unknown.

## Compatibility and recovery

Migration 0002 is additive and preserves existing input/output tables. Back up the
session using PostgreSQL tooling before upgrading. Earlier binaries reject schema
version 2; rolling back requires restoring the pre-upgrade backup into a separate
session and using its matching binary. Do not manually decrement the migration
version or delete receipts to make an older binary open an upgraded session.
The new tables currently have no deletion/TTL policy; job receipt retention will
be decided with the completed replay/archive runtime.

## Remaining before issue #30 is complete

Wire the bounded provider stream to these primitives; record replay availability
and its age before every subscription; implement fair offline retries and failure
inspection; prove crash recovery around raw/output/receipt commits with real process
exits; integrate independent live-priority admission, cancellation and console
status; exercise replay expiry/provider outages, out-of-order input and pruning;
ship events/metrics and bounded opt-in validation. No live check has been started
for this storage work. The ordinary tests use real local PostgreSQL without provider
connections.

## Offline evidence (2026-11-13)

On macOS ARM64 with local PostgreSQL 18.6, formatting and default/archive Clippy
passed. Full default/archive offline Rust suites passed with the first four new
regressions (101 and 126 tests respectively, three ignored subprocess helpers
each). After adding the persisted allowance-limit regression, all five replay
regressions and default Clippy passed again. Tests inject a receipt-write failure,
prove raw/receipt atomicity while the received-work charge survives, reject
conflicting scope/input, retain processing receipts after raw deletion, preserve
allowances across connection death/restart and test upgrade compatibility.
Real provider behavior, process-exit boundaries and complete runtime integration
remain pending; these checks do not close issue #30.
