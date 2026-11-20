# Bounded source overlap validation

The combined command runs one finalized USDC stream worker and one historical
RPC worker against the same provisional SQLite database. It validates source
coalescing and write scheduling; it is not a production scheduler or a claim that
the entire stream/window is complete.

## Run

```sh
# Configure HELIUS_API_KEY securely and opt into a metered mainnet stream.
INDEXER_LIVE_OPT_IN=1 cargo run --locked -- overlap data/overlap.sqlite START_SLOT END_SLOT 5
cargo run --locked -- summary data/overlap.sqlite EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
cargo run --locked -- coverage data/overlap.sqlite EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v START_SLOT END_SLOT
```

Choose an inclusive range of at most 128 recent finalized slots, with START_SLOT
inside current LaserStream replay availability. Duration is 1–300 seconds. The
stream retains its existing connection,
inactivity, message-size, and retry limits. Historical fetching stays serial;
its cancellation check uses the same elapsed budget and optional
BACKFILL_CANCEL_FILE. In-flight HTTP/SQLite/JSON work is synchronous and can extend
shutdown beyond the nominal budget. Genesis validation can take up to 20 seconds;
a block request up to another 20 seconds. No hard production latency guarantee is
made. Invalid bounds, opt-in, or absent credentials fail before either network is
opened. Ordinary tests never open a network.

The JSON report includes each worker's result, linked run identifiers, the gate
snapshot, exact observed counts/volume/fees, and scoped historical coverage.
A reported worker is not proof of complete coverage. Source failures are explicit;
source-independent historical evidence can verify only the requested finite range.
The stream's later tail remains unknown. Unsupported transactions can keep the
historical range incomplete, even if other transfer rows are available. Use the
coverage report when interpreting totals.

## Compatibility and durability

Logical identity is the existing deterministic transfer ID. Within one decoder
revision, observations must agree on signature, slot, transaction version, static
account keys, and outer compiled instructions. Known header, recent blockhash,
configuration, loaded-address and inner-instruction evidence must also agree.
Inner group ordering and diagnostic fields such as stack height/log formatting
are normalized independently of instruction position. Missing metadata is unknown,
not evidence that the transaction contains no inner instructions.

Decoded record sets and all known semantic values must agree. An unknown block
time, owner, or fee may be enriched by compatible known evidence; later sparse
observations cannot erase known values. Conflicting known values, changed
instructions, changed transfer sets, or different decoder revisions are rejected.
This is compatibility of observed data, not independent signature/finality proof.
Cross-provider completeness reconciliation remains a separate validation task.

Raw observations are retained once per distinct source/result in the additive
`capture_inputs` table. A first observation's row holds no payload of its own: NULL
payload columns mean "byte-identical to the current `captures` row" (migration 0013),
so its bytes are stored once. Every write that changes a `captures` payload first
copies the current bytes into those NULL rows, preserving the original raw payload;
`captures` must not be changed any other way. Rows written before migration 0013 keep
their own bytes. Rollback fills NULL rows from `captures` and restores NOT NULL. `captures.result_json`
becomes a compatible reference that retains known optional identity evidence;
it need not be byte-identical to the first raw payload. Raw inputs remain unchanged.
A conflicting observation is durably quarantined without changing transfer rows.
Receipts, reference enrichment, and transfer updates commit atomically. Failure
partway through compatibility checking cannot partially enrich the record set.

A previously undecodable capture may become decodable within the same decoder
revision when compatible execution metadata arrives. This does not implement
automatic revision changes. Deliberate replacement/removal of prior output is
available through [offline decoder repair](decoder-repair.md). Existing captures from an older decoder remain
readable, but new ingestion rejects their revision until deliberately reprocessed.

SQLite primary keys enforce immediate uniqueness. A write transaction acquires
its writer reservation before reading existing records, avoiding deferred
read-to-write upgrade races. Query results do not wait for background deduplication.
`summary DB MINT` streams persisted rows with checked u128 accumulation and decimal
JSON strings. It reports one mint across **all stored data**, not a rolling 30-day
API. It remains coverage-unknown and reports unknown fees separately. Counts and
volumes describe unique stored transfers, unlike decode-attempt counters.

## Admission and telemetry

A shared gate admits only one source write operation at a time. Ready live work
wins over waiting historical work; a historical operation already in flight is
not preempted. History releases admission between raw writes, individual decoded
transaction writes, and coverage acknowledgments. Network reads do not hold the
gate. There is one worker per source and no additional transaction queue. This
priority is within the combined process; separately launched processes rely on
SQLite locking and bounded busy waits, not cross-process scheduling priority.

The public gate snapshot exposes active/pending admissions, oldest in-process
wait age, admission counters, and wait-duration sums. The command includes its
terminal snapshot; it does not add a continuous metrics server. Gate admission
counts are operations, not transaction counts. Source-specific reports separately
retain durable unresolved work, backlog age, stage timings, decoder scope, and
retry attempts. Finalized reference freshness and full-window live coverage are
not fabricated. Long-running monitoring, production thresholds, and dashboards
remain pending.

## Evidence and remaining checks

Offline tests use the captured mainnet transaction and a **synthetic protobuf
view** of its known fields. They are not a captured or credentialed LaserStream
session. Tests exercise both arrival orders, retries, actual concurrent SQLite
source cores, immediate query/summary deduplication, timestamp enrichment,
conflicting input quarantine, transaction-wide rollback, legacy raw preservation,
large integer totals, out-of-order range completion, and injected write slowdown
with live-priority admission.

A bounded public RPC genesis check verified the combined TLS dependency graph.
The credentialed five-second run received 494 candidates. A mixed-token record-set
mismatch exposed by that run is fixed: both sources now persist the complete
supported decoded record set, with mint filtering at query time. The captured
mixed-token regression covers both source orders.

Offline replay of those retained inputs observed 26 decoded transactions from
both sources with zero quarantined conflicts and stable duplicate-free summaries.
An independent Python instruction parser found 97 classic SPL USDC edges in the
three sampled finalized blocks; v3 stored 18, with zero mismatched or
extra stored edges. The other 79 were missing because unavailable Token-2022 fee evidence blocked
whole-transaction decoding. Revision v4 retains known gross amounts and null
unknown fees, recovering all 97 independently checked edges. Retained replay
now has 55 shared decoded transactions, no conflicts, and no historical decode
failures across the three blocks after native unwrapping is recognized as an
excluded SOL movement. All three sampled historical slots are complete.
The stream tail remains unverified. See the
[original validation results](validation/2026-11-12-overlap.md) and the
[v4 coverage validation](validation/2026-11-12-decoder-coverage.md).

No full backfill, production infrastructure, or throughput/cost benchmark was
initiated. Credential values remain local and are excluded from repository and
validation output.

Rollback can use the previous binary for reads; it ignores the additive audit
table. Its former exact-JSON replay rule can reject enriched references. Preserve
raw inputs/backups and use a fresh validation database when reverting ingestion
until a production migration/rollback policy is established. Audit indexing stores
additional raw/reference bytes; measure its cost before selecting production storage.

The former ignored test `replay_retained_network_observations` replayed a retained
SQLite validation database. It was removed with SQLite (#35); retained validation
data from that period must be refetched into a PostgreSQL session to replay.
