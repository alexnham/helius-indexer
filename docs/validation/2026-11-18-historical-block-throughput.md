# Historical block write throughput

Measured 2026-11-18 on an Apple Silicon laptop against the local PostgreSQL 18.6
container over loopback. Single opt-in runs, not a hosted or production benchmark.

## Scope

Branch `codex/backfill-block-batch` against `main` at 462451b. Historical block
processing (`History::run`, shared by RPC and archive sources) previously opened one
PostgreSQL transaction per block transaction, and stored a capture for every one. The
branch writes each block's decoded transactions in batches of up to 1,000 (16 MiB).
It also skips the per-transaction capture for a transaction that produced no
transfers and does not touch the mint; the full block is still retained in
`historical_inputs`. Revision repair keeps its per-transaction path.

Not measured: Old Faithful or RPC download time, hosted PostgreSQL, real mainnet
block composition, or several workers in parallel.

## Command and dataset

`cargo test --release --test backfill historical_block_throughput -- --ignored --nocapture`,
with a separate `CARGO_TARGET_DIR` per checkout. Eight consecutive slots of 1,000
transactions each, from the captured mainnet USDC transfer (`tests/fixtures/mainnet-transfer.json`)
with generated signatures. In the "1 in 20" mix, the other transactions are failed
copies with the mint removed from their keys and token balances. That is a stand-in
for unrelated traffic such as votes, not real block composition.

## Results

Transactions per second, counting every block transaction processed:

| Mix | `main` | Branch (3 runs) |
| --- | --- | --- |
| Every transaction transfers the mint | 118 | 780 / 822 / 884 |
| 1 in 20 transfers the mint | 151 | 2,665 / 2,726 / 3,406 |

Profiling the branch (`sample` plus `pg_stat_activity`) shows the run mostly waiting
on PostgreSQL inserts of captures and the retained raw block. Client CPU goes mostly
to base58 key validation and JSON parsing. The next lever is processing slot ranges
in parallel (#37), not larger batches.
