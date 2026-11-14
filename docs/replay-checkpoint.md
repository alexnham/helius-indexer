# Issue #30 checkpoint 4 — 2026-11-14

Status: **implementation and verification complete on `codex/replay-postgres`**.
No push, merge or deployment. The main checkout also has a handoff at
`docs/replay-checkpoint.md`; other ongoing Kafka/decoder work is separate.

The existing native replay/runtime work now has an additive prototype upgrade
(migration 0003, leaving committed migration 0002 unchanged), upgrade/recovery
regressions, real count-pruning checks, explicit quiet/no-transfer coverage checks,
and continued live writes after replay expiry or simulated provider outage.
Docker smoke verifies durable replay-job restart.

Completed final checks:

- Formatting, diff whitespace, default/archive all-target Clippy: passed.
- Full default offline suite: 121 passed, five ignored.
- Full archive offline suite: 146 passed, five ignored.
- Console tests: 14 passed.
- Compose validation, console image build and isolated Docker smoke: passed.
- Changed Markdown relative link targets: present.

Bounded provider check: two 30-second attempts reused a frozen 50-slot USDC scope
469462883–469462932 without refilling the grant. Received 760 messages and retained
525 unique input receipts; 518 decoded, seven unresolved. Queries returned 888
unique USDC transfers; exact volume/fee sums matched the stored summary. Offline
retry without credentials preserved the seven unresolved inputs and allowance.
The job reports exhausted time, unknown coverage and null verified boundary.
It did not complete or independently reconcile the requested range.

See [validation and operating limits](validation/2026-11-14-replay-jobs.md) and
[upgrade/recovery commands](replay-jobs.md). Local provider inputs remain in
PostgreSQL session `replaycheck_b22163ffbbfb`; sanitized reports are under
`/private/tmp/replaycheck_b22163ffbbfb`. No provider payload or key is committed.

For Claude: review/integrate this branch rather than duplicating #30, preserving
separate Kafka/decoder work. The current decoder is v5; correcting the seven
unsupported provider inputs requires explicit decoder/revision recovery, not a
coverage flag or automatic budget refill. Archive routing (#31), full Docker
archive operation (#32) and production capacity remain separate work. This branch
has not been reconciled with parallel changes after its PostgreSQL/TLS baseline.
