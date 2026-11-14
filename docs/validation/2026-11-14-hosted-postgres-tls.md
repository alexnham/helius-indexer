# Hosted PostgreSQL TLS and latency check

Measured 2026-11-14 from an Apple Silicon laptop against the owner's Railway
PostgreSQL 18.6 public proxy. This is a single bounded opt-in run, not a benchmark.

## Server certificate

Railway presents a self-signed chain: leaf `CN=localhost`, issuer `CN=root-ca`
(`openssl s_client -starttls postgres` reports verify code 19). Chain-and-hostname
verification therefore cannot pass, so the connection pins the leaf certificate's
SHA-256 fingerprint (`INDEXER_DB_TLS_SHA256`). The pin was obtained out of band
with openssl and is not committed.

| Case | Outcome |
| --- | --- |
| No pin (verify-full against system roots) | `storage_tls_failed` |
| Wrong pin | `storage_tls_failed` |
| Malformed pin | `storage_tls_pin_invalid`, before connecting |
| `sslmode=disable` | `storage_tls_required`, before connecting |
| Correct pin | connected and migrated |
| Correct pin with `channel_binding=require` | connected (SCRAM-SHA-256-PLUS) |

Channel binding is now required whenever a pin is configured.

## Latency and write throughput

`cargo test --test hosted -- --ignored --nocapture`, with `INDEXER_HOSTED_DATABASE_URL`
and the pin set. The run used a throwaway schema, dropped afterwards; 38 captured
transactions from `mainnet-coverage.json` produced 240 transfers.

| Measure | Value |
| --- | --- |
| First connection (TLS plus schema migration) | 1.77 s |
| Round trip, median / max of 10 | 180 ms / 196 ms |
| `persist` throughput, one writer | 0.98 transactions/s |

Each `persist` issues several sequential statements, so one writer is bounded by
round trips (about 5 × 180 ms). The measured USDC load is about 80 matched
transactions/s ([load measurement](2026-11-13-usdc-load.md)). A single writer
against a remote server is roughly 80× too slow. Batching many inputs per
transaction, or pipelining (#36), and running the writer close to the database
are required before hosted use. Local loopback latency hides this cost.

## Batched writes (#36, first slice)

Same environment, `cargo test --test hosted -- --ignored --nocapture`. The run
used throwaway schemas; the 100-item batch reuses the captured mainnet transaction
with 100 distinct signatures, in the warm epoch's partition.

| Measure | Value |
| --- | --- |
| Round trip, median | 190 ms |
| One transaction per write | 1.8 transactions/s |
| First batch, 19 items (creates the partition, prepares statements) | 1.68 s |
| Next batch, 19 items | 0.61 s, about 3.2 round trips |
| Batch of 100 items | 2.21 s, **45 transactions/s** |

New transactions use set-based statements, so a warm batch costs about four round
trips whatever its size. Statements are prepared once per connection, and the
input payload is copied inside PostgreSQL instead of being uploaded twice. At 100
items the remaining time is mostly uploading about 1.1 MB over this laptop's
connection. That is an environment limit: a writer near the database would see the
round-trip cost only. This is inference from the measurements, not measured.

**45 transactions/s per writer is below the measured ~80 transactions/s of USDC.**
Hosted live USDC is not yet sustainable from this link with one writer: it needs
parallel writers (#36 consumer group), larger batches, or a writer co-located with
the database. Until then, a single hosted writer falls behind during sustained load.
