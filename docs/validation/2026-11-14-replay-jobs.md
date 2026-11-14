# Bounded replay jobs: validation on 2026-11-14

Issue #30 uses the [replay source contract](../research/2026-11-13-laserstream-replay-contract.md)
and [job/recovery commands](../replay-jobs.md). This check used the native debug
binary, Rust 1.99.0, macOS ARM64 and local PostgreSQL. It is functional evidence,
not a production throughput, cost or full-range completeness measurement.

## Provider-backed frozen scope and restart

The existing ignored Helius credential was loaded into the subprocess environment,
never argv or recorded URLs. A genesis/finalized-reference probe chose a 50-slot
USDC range, **469462883–469462932**. Historical input came from LaserStream
`from_slot` with finalized commitment and `match_mints`; no block scan supplied
historical transactions. The test used an isolated local PostgreSQL session,
`replaycheck_b22163ffbbfb`.

Equivalent native command sequence (with credentials supplied through environment):

```sh
helius-indexer replay-create SESSION recent MINT 469462883 469462932
helius-indexer replay SESSION recent 30
helius-indexer replay SESSION recent 30
helius-indexer replay-retry SESSION recent
helius-indexer replay-status SESSION recent
helius-indexer query SESSION MINT
helius-indexer summary SESSION MINT
```

Both provider attempts stopped at their time limit. Restart preserved the same
scope, decoder and original grant; it did not choose a new finalized tip or refill
the allowance. Availability was observed again before the second subscription.

| Observation | First attempt | After restart |
| --- | ---: | ---: |
| Unique input receipts | 235 | 525 |
| Durable decoded receipts | 228 | 518 |
| Unresolved receipts | 7 | 7 |
| Reserved seconds consumed | 30 | 60 |

Final durable usage was **760 transaction messages, 6,396,740 protobuf bytes and
two connections**. The job correctly reports `budget_exhausted`, zero remaining
seconds, unknown coverage and a null verified boundary. No allowance was added.
The complete wrapper took 64.081 seconds, including local database setup,
acknowledgments, offline retry and status/query work; this is not a 60-second
end-to-end wall-time claim. The per-attempt intake windows were 30 seconds each.

Queries returned **888 USDC rows and 888 unique transfer IDs**, covering output
slots 469462883–469462900. Exact integer accumulation over those query rows matched
the stored summary: instructed volume **179241935307**, known fees **0**, unknown
fee count **0**. This is an internal query/storage consistency check, not an
independent reconstruction of chain transfers. Other-mint output in the same
matched transactions retains the existing storage semantics.

Five unresolved inputs report `incomplete_execution_evidence`; two report
`invalid_transfer_instruction`. Offline retry, repeated with both the API key and
provider opt-in removed from the environment, attempted all seven, decoded none,
preserved their inputs and did not change consumed budgets. Replay did not change
the live source's resume position (`last_received_slot` remained null).

**The requested 50 slots were not completed or independently reconciled.** A
later-slot/end observation was not reached. Unknown coverage and the full pending
interval remain visible, including slots beyond the observed output. Decoder
corrections and independent full-block coverage evidence are separate work.
The stored inputs remain local; no provider payload or credential is committed.

## Upgrade and deterministic verification

Migrations 0004/0005 (renumbered after main's Kafka migrations 0002/0003) upgrade
the earlier local version 2/3 replay prototypes instead of editing their
already-applied migrations. Existing budgets/receipts survive, raw rows referenced
by replay receipts are classified as replay, and unavailable historical provenance
remains null. The regressions retry a prototype version 2 pending input against real
PostgreSQL, checks exact query amount/fee and rejects an older binary's schema
view. Restore a pre-upgrade backup with its matching binary to roll back.

Deterministic regressions also cover raw/output/receipt process death, duplicate
and out-of-order delivery, changed wire metadata versus changed execution
evidence, count pruning without resurrecting deleted output, observed no-transfer
input versus quiet unobserved ranges, and continued live writes after historical
expiry or a simulated provider outage. All leave unsupported coverage unknown.

Completed checks on the final implementation:

- `cargo fmt --check` and `git diff --check`: passed.
- `cargo clippy --offline --locked --all-targets -- -D warnings`: passed.
- The same Clippy command with `--features archive`: passed.
- `cargo test --offline --locked`: 121 passed, five ignored helpers/opt-in tests.
- `cargo test --features archive --offline --locked`: 146 passed, five ignored.
- `python3 -B -m unittest discover -s tests -p '*console_test.py'`: 14 passed.
- Changed Markdown files' relative link targets: present.
- `docker compose config --quiet` and `docker compose build console`: passed.
- `INDEXER_CONSOLE_PORT=18765 INDEXER_SMOKE_POSTGRES_PORT=55433 python3 -B scripts/docker-smoke.py`:
  passed using an isolated disposable Compose project. Checks covered non-root
  operation, health, graceful shutdown, persistent session/binary/query results,
  replay-job scope/allowance/receipt persistence, and network-disabled seed/replay
  through the PostgreSQL Unix socket. The disposable project's volumes were
  removed; existing runtime/development sessions were left intact.

The source-completion limitation, seven unresolved provider inputs and unfinished
50-slot range remain explicit operating limits. This does not demonstrate a full
archive handoff, a production 30-day window or sustainable catch-up throughput.
