# Pruned historical blocks

Measured 2026-11-18 on an Apple Silicon laptop against a throwaway local PostgreSQL 18.7
container with no CPU limit, over loopback. The laptop was heavily shared during the
runs (load average 34–88 on 8 CPUs), so the byte figures are the result; the
throughput figures are indicative only.

## Scope

Branch `codex/refetch-historical-blocks` against `main` at 2c92bf2. A historical
block was retained whole in `historical_inputs`. Now every transaction that does not
touch the job's mint is reduced to its signatures before the block is stored
(migration 0014 marks such rows `pruned`). Block identity, skip evidence, retries and
decoder repair still run from the pruned block; a mint-filter correction needs the
affected blocks fetched again. Archive/RPC bridge evidence still retains whole blocks.

## Command and dataset

```
INDEXER_TEST_DATABASE_URL=postgres://indexer:<password>@127.0.0.1:<port>/indexer \
INDEXER_HISTORICAL_WORKERS=8 \
<release backfill test binary> historical_block_throughput --ignored --nocapture
```

Each checkout's binary was built in its own `CARGO_TARGET_DIR`; runs alternated main,
branch. The dataset is 32 consecutive slots of 1,000 transactions per mix, built from
the captured mainnet USDC transfer with generated signatures. In the "1 in 20" mix the
other transactions are full-size failed copies with the mint removed. The branch's
benchmark also prints fetched bytes, retained bytes and the `historical_inputs` size.

## Results

Bytes (identical across the three branch runs):

| Mix | Fetched | Retained | `historical_inputs` on disk |
| --- | --- | --- | --- |
| Every transaction transfers the mint | 169 MiB | 169 MiB | 66 MiB |
| 1 in 20 transfers the mint | 120 MiB | 17 MiB | 7 MiB |

Retained raw bytes fall by 86% on the 1-in-20 mix; nothing is pruned when every
transaction is related. Without pruning, the 1-in-20 table would be about 47 MiB on
disk (the related mix's 66 MiB scaled by fetched bytes). The synthetic transactions
are near-identical and compress unusually well, so real blocks should gain more on disk.

Throughput, tx/s (five alternating runs each, in run order):

| Mix | `main` | Branch |
| --- | --- | --- |
| Every transaction transfers the mint | 311, 548, 480, 792, 678 | 396, 595, 619, 768, 570 |
| 1 in 20 transfers the mint | 2,427, 3,529, 5,148, 5,583, 5,756 | 4,442, 5,633, 3,982, 10,237, 5,687 |

Under this load the 1-in-20 medians are 5,148 (main) and 5,633 (branch). The difference
is within run-to-run noise, so no throughput gain is claimed. The same benchmark on
`main` reached about 11,900 tx/s earlier the same day on a quieter machine.

## Limits

- Pruning parses and re-serializes each block once more before it is retained; that
  CPU cost was not isolated.
- Not measured on a hosted database or on real mainnet blocks.
- `scripts/compare-captured-usdc.py` skips pruned transactions, so it can no longer
  re-check the mint filter for them.
