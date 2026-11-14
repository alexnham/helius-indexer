# Kafka consumer-group throughput

Measured 2026-11-14 on an Apple Silicon laptop: the local Kafka 4.1.1 container (one
broker, 1 CPU / 1 GiB) and the local PostgreSQL 18.6 container, both over loopback.
This is a single opt-in run, not a hosted or production benchmark.

Command: `cargo test --lib measure_consumer_group_throughput -- --ignored --nocapture`.
Dataset: 2,400 distinct one-transfer transactions (the captured mainnet transaction with
generated signatures) produced to a fresh 6-partition topic, drained by a fresh
consumer group into a fresh session. Drain time ends when stored positions cover
every input.

| Consumers | Drain time | Transactions/s |
| --- | --- | --- |
| 1 | 1.79 s | 1,339 |
| 2 | 1.34 s | 1,788 |
| 4 | 3.31 s | 724 |

**These loopback figures say nothing about hosted throughput.** Against Railway one
writer reached 45 transactions/s, so the ~80 transactions/s USDC target on hosted
PostgreSQL stays unproven until an opt-in multi-consumer hosted run is recorded (#36).

Over loopback one consumer already runs at about 17× the ~80 transactions/s USDC load.
Two consumers help; four are slower. This is likely rebalance churn: consumers start
at the same moment, and each join triggers an eager rebalance that revokes every
partition and discards buffered input for replay. Cooperative-sticky assignment, or
staggered starts in long-running services (#38), should remove it. That is not yet
measured.

Not measured: hosted PostgreSQL with several consumers (one hosted writer reached
45 tx/s, see [hosted TLS and batching](2026-11-14-hosted-postgres-tls.md)), sustained
load, LaserStream reorder distance, and rebalance counts.
