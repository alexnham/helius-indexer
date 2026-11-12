# Decoder v5 maintenance validation — 2026-11-12

## Supported change

The previous decoder rejected a whole transaction on SetAuthority or withheld-fee
withdrawal. The retained failures were base CloseAccount authority type 3 and
WithdrawWithheldTokensFromAccounts (26, 3), which create no ordinary instructed
transfer edge. V5 recognizes base authority types 0–3, fee withdrawal from the
mint (26, 2), fee withdrawal from accounts (26, 3), and the existing harvest
(26, 4), with explicit payload/account-count validation. Other extension authority
and fee configuration instructions remain unsupported.

AccountOwner (2) is different from CloseAccount authority (3). V5 conservatively
clears source/destination owner fields for any account with an AccountOwner change
anywhere in the transaction. This includes change-and-restore even when pre/post
owners agree, and inner changes after an earlier transfer. Other accounts retain
the existing pre/post owner evidence. No historical account lookup is added.

Fee withdrawals and harvesting emit no ordinary transfer row and do not resolve
unknown fees on other transfers. Gross amounts, nullable fees and stable
instruction-position IDs keep their existing contract. Runtime failure and
missing/truncated inner execution evidence remain blockers.

Primary program sources checked on 2026-11-12:

- [Token-2022 authority types and encoding](https://github.com/solana-program/token-2022/blob/58ac9918b66ed255b969e9d9d9cb1e930c87fa2c/interface/src/instruction.rs)
- [Authority processor behavior](https://github.com/solana-program/token-2022/blob/58ac9918b66ed255b969e9d9d9cb1e930c87fa2c/program/src/processor.rs)
- [Fee instruction layouts](https://github.com/solana-program/token-2022/blob/58ac9918b66ed255b969e9d9d9cb1e930c87fa2c/interface/src/extension/transfer_fee/instruction.rs)
- [Fee withdrawal processor](https://github.com/solana-program/token-2022/blob/58ac9918b66ed255b969e9d9d9cb1e930c87fa2c/program/src/extension/transfer_fee/processor.rs)
- [Classic SPL instruction types](https://github.com/solana-program/token/blob/main/interface/src/instruction.rs)

## Deterministic evidence

`cargo test --offline --locked --test maintenance` first failed with
`unsupported_authority_change` on slot 468265450 / signature
`3iakngpvBoJGLJA96S2JXV264HxpdqfDP9b9D9ieUhVxrCPDB4DLzNoCzGF3dKdmsg3QGNsHpHYJXAeFCauJFJAM`.
After the change, 11 sanitized captured transactions preserve all 61 independently
inspected ordinary transfer edges, including exact IDs, positions, amounts, fees
and owner evidence. Four fee-withdrawal captures emit zero ordinary edges.
Repeated persistence produces no duplicate rows. A paired retained finalized RPC
observation for the first authority case decodes identically to archive input
and reconciles to nine unique edges in either arrival order. This is a
transaction comparison, not a new claim of full RPC block coverage.

Synthetic tests cover both token programs, outer/inner AccountOwner changes,
change-and-restore, unchanged close/mint/freeze authority owner evidence, null fees
after fee collection, malformed options/payloads/account counts, unknown types,
and caught runtime failures. Repair tests recover formerly rejected captures and
normalized archive blocks offline without inventing source completeness.

## Six-slot retained archive replay and repair

The bounded range was 468265446–468265451, USDC, using already retained immutable
CAR ranges and normalized SQLite inputs. On local Apple Silicon / Rust 1.99,
commands used locked dependencies, development/test debug information disabled,
and incremental compilation disabled. No network access was requested:

```sh
# Prior v4 binary, fresh validation database:
helius-indexer-v4 archive-replay DB CACHE EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v 468265446 468265451
# Current default build; repair the same database from retained normalized input:
helius-indexer-v5 repair-history-archive DB EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v 468265446 468265451
```

| Measurement | v4 replay | v5 retained repair |
| --- | ---: | ---: |
| Verified slots | 3 of 6 | 6 of 6 |
| Unfinished slots | 3 | 0 |
| Successful decoded transactions | 2,817 | 2,824 |
| Decoded/persisted transfer records | 2,395 | 2,425 |
| Source block attempts | 6 offline cache reads | 0 |
| Reused normalized blocks | 0 | 6 |

The v5 operation imported seven formerly rejected transactions and 30 additional
ordinary transfer records. The counters describe this finite replay, not a
production throughput benchmark. Raw captures, normalized blocks, databases,
binaries and command logs stay under ignored local `data/`; only sanitized
transaction fixtures are committed.

`repair-history-archive` is explicit and offline. It reads only archive-kind
normalized receipts and has no network fallback; it does not require the optional
SDK feature or raw CAR cache. Missing receipts remain unknown. It shares the
existing revision replacement/reverification path, bounds ranges to 128 slots,
and accepts `REPAIR_CANCEL_FILE`. Reports retain repair telemetry context, cached
block counts, source kind and scoped coverage. It does not certify a live tail or
acknowledge an archive/RPC boundary proof. Existing boundary plans/proofs are
revision-scoped and are not relabeled as v5 plans.

## Compatibility and remaining gaps

There is no new storage schema or transfer identity. V5 output requires explicit
repair; ordinary ingestion must not relabel prior output. Back up the SQLite
database before in-place repair and stop old writers. Restore that backup for an
exact rollback. Capture-only corrections remain separate from full-block
coverage. The existing repair tests exercise addition, change and removal of
transaction output.

The six-slot archive result does not prove RPC completeness. Previously observed
RPC log truncation remains unresolved and can keep an overlap incomplete, even
when richer archive input decodes. Extension authority types, fee configuration,
confidential cases and missing historical state remain explicit gaps. No fresh
provider fetch, paid live stream, full 30-day backfill or infrastructure provisioning
was run for this decoder correction.

Final checks on the merged source-boundary base: `cargo fmt --check`, default and
archive-feature Clippy (`--all-targets --offline --locked -- -D warnings`), and
both complete offline suites passed: 82 default tests and 103 archive tests, with
four existing opt-in/helper tests ignored in each suite. Independent read-only
review found no actionable correctness or recovery issue.
