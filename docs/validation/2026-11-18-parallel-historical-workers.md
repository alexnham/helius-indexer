# Parallel historical block workers

Measured 2026-11-18 on an Apple Silicon laptop (8 CPUs available to Docker) against a
throwaway local PostgreSQL 18.6 container with no CPU limit, over loopback. Single
opt-in runs, not a hosted or production benchmark.

## Scope

Branch `codex/parallel-history` against `main` at 2eb0a8b (after #111). The branch:
- adds `INDEXER_HISTORICAL_WORKERS` block workers per historical run;
- gives the write gate one permit per worker;
- moves raw-block retention into the workers;
- stops re-reading a retained raw block that is byte-identical to the block whose
  identity is being observed. This step took about 45% of worker time in a profile.

Not measured:
- Old Faithful or RPC download time;
- hosted PostgreSQL;
- real mainnet block composition;
- several processes leasing ranges (#37).

The Compose PostgreSQL is capped at one CPU and 512 MiB. Against it, extra workers
made no consistent difference (668–947 tx/s for 1–8 workers on the "every
transaction" mix).

## Command and dataset

```
INDEXER_TEST_DATABASE_URL=postgres://indexer:<password>@127.0.0.1:<port>/indexer \
INDEXER_HISTORICAL_WORKERS=<n> \
cargo test --release --test backfill historical_block_throughput -- --ignored --nocapture
```

Each checkout uses its own `CARGO_TARGET_DIR`. The dataset is 32 consecutive slots of
1,000 transactions each, built from the captured mainnet USDC transfer with generated
signatures. In the "1 in 20" mix, the other transactions are failed copies with the
mint removed, standing in for unrelated traffic.

## Results

Transactions per second, counting every block transaction processed:

| Mix | `main` (2 runs) | 1 worker | 4 workers | 8 workers (3 runs) |
| --- | --- | --- | --- | --- |
| Every transaction transfers the mint | 830 / 762 | 827 | 1,260 | 1,576 / 1,664 / 1,513 |
| 1 in 20 transfers the mint | 3,317 / 3,232 | 4,392 | 7,286 | 8,451 / 8,636 / 8,400 |

With 8 workers the run is about 2× faster on the first mix and about 2.6× on the
second. Scaling flattens beyond 4–8 workers on this laptop, where PostgreSQL and the
workers share the same 8 CPUs. Block identity observation takes one schema-wide
advisory lock, so first observations of blocks are still serialized. It is the next
candidate to examine.
