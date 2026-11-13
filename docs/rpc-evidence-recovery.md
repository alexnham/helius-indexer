# Recovering explicit RPC log truncation

Verified 2026-11-12 (America/Toronto). This recovery closes supported historical
execution-evidence gaps using matching retained archive evidence. It does not
turn an incomplete native RPC observation into an independently complete source.

## Evidence contract

The ordinary decoder is unchanged. A successful transaction with inner token
instructions still needs complete runtime evidence and rejects caught failures.
A dedicated coordinator compares both full finalized block observations before
any cross-source proof is acknowledged. Block anchors/parent links, ordered
transaction identities, compiled instructions and known execution metadata must
agree. Known token balance mint, owner, program, decimals and amount pairs must
not conflict; absent values are not fabricated.

Recovery applies only when the archive decodes completely and native RPC decoding
fails with `incomplete_execution_evidence`. The affected RPC log array must contain
exactly one literal `Log truncated` marker, after a nonempty exact archive prefix.
Every observed log after that marker must appear in order in the remaining full
archive log array. Some collectors retain later short lines after dropping an
oversized line, so the marker need not be last. The archive must have more log
entries and must not itself carry a truncation marker. Missing markers, conflicting
prefix/suffix lines, incomplete archive logs, caught failures, unsupported
instructions and contradictory known metadata remain unresolved.

Only the affected RPC log arrays are replaced. All other RPC metadata stays as
received. Both reconstructed and original archive transactions run through the
same decoder, and their transfer IDs, exact amounts, fees, positions and known
optional values must reconcile. At most 16 transactions can be repaired in a
block; larger cases remain incomplete. No balance-delta reconstruction, current
account lookup or relaxed runtime-success rule is introduced.

## Durable provenance and readiness

The original archive and RPC normalized blocks remain separate immutable input
observations. A third input source, `archive-rpc-corroborated`, retains the exact
SHA-256 of each original block, method version, slot and available archive receipt.
Capture provenance carries those references. Native RPC source acknowledgments
remain `unknown`; the third source is acknowledged separately after complete
decode/storage. The existing attempt-generation proof prevents a crash or later
retry from reusing stale boundary readiness.

Reports distinguish:

- `overlap_verified`: both native source observations decoded completely.
- `corroborated_slots` / `overlap_recovered`: explicit archive-backed recovery.
- `boundary_ready`: native verification or successful corroboration for the
  requested scope, with the required durable proof.
- Native `archive_complete`, `rpc_complete`, `recovery_attempted`, recovered counts,
  stage metrics and bounded `recovery_error` remain visible for every observation.

Frozen bridge plans can become ready through this explicit recovered mode. Their
contiguous checkpoint still stops at unresolved work, and the live tail stays
unknown. A later source attempt invalidates a proof unless its attempt generation
matches. Previously recovered source audit rows alone do not establish readiness.
This is corroboration using archive execution evidence, not independent RPC
execution verification or a cryptographic execution proof. Shared providers,
normalizers and decoder logic can still have correlated omissions.

## Commands and bounds

The normal `archive-overlap` and frozen bridge boundary checks use this recovery
automatically when the exact conditions hold. Existing opt-ins and download,
request, deadline, buffer, range and live-priority write limits still apply.
Recovery performs no additional network requests. It checks cancellation between
transactions and rejects output above the existing 64 MiB normalized-block limit.
Per-transaction capture limits remain 16 MiB. CPU decoding and acknowledged writes
may finish after the network deadline; no hard process wall-time bound is claimed.

For already retained observations, the optional archive feature adds an offline
command. First back up the SQLite database and stop older writers. If its decoder
revision is older, explicitly repair archive inputs before checking the pair:

```sh
helius-indexer repair-history-archive DB MINT START END
helius-indexer archive-overlap-replay DB MINT START END
```

The second command selects the latest bounded original archive/RPC normalized
observations from SQLite. It requires no endpoint, API key or CAR cache and makes
zero network requests. It accepts at most eight inclusive slots and checks
`BACKFILL_CANCEL_FILE` plus a 120-second scheduling/processing budget. Missing or
oversized input fails explicitly; there is no network fallback. Inputs are bounded
before loading their payload from storage. Successful replay is duplicate-free
but may add attempt/audit rows; attempt counts are not unique transfer counts.

`indexer_input_bytes_total` uses the bounded source label `corroborated` for the
third normalized input. These are reconstructed bytes, not additional downloaded
or provider-billed bytes. Reports retain native decode failures and recovery
attempts separately from successful unique stored output. No high-cardinality
metric labels or raw payload dumps are added.

## Reproducible verification

Two sanitized finalized mainnet transaction pairs from slot 468265447 cover 36
independently inspected ordinary edges. Original archive logs had 194 entries;
RPC had 130 with `Log truncated` at index 127 and additional observed lines after
it. Fixtures retain runtime logs and the marker, compiled instructions and token
metadata; application logs/return data are removed. Tests check exact output,
native-RPC unknown status, all three durable inputs, duplicate-free replay,
conflicting/missing evidence, cancellation and the 16-transaction limit. A real
SQLite trigger failure between output and proof leaves bridge readiness false
until a successful restart; the live tail remains unknown afterward.

Copies made with SQLite's backup API of the retained mainnet databases were
repaired to v5, recovered, and replayed again. No original validation database was
modified, and no provider request ran:

| Slot | Native RPC complete | Corroborated readiness | Unique stored rows before/after restart |
| --- | --- | --- | --- |
| 468265447 | false | true; two repaired transactions | 407 / 407 |
| 468265450 | false | true; one repaired transaction | 366 / 366 |

Counts cover all decoded mints in each finite block, not a 30-day API. Reports
kept `overlap_verified=false`, `boundary_ready=true`, `corroborated_slots=1`, scoped
historical coverage complete and live coverage unknown. Raw inputs, database
copies and logs remain under ignored local `data/`. This is bounded correctness
validation, not a production throughput or cost benchmark.

## Provider limitation and compatibility

[Helius log-truncation documentation](https://www.helius.dev/docs/laserstream/log-truncation)
provides untruncated live endpoints, but explicitly excludes historical replay on
those endpoints. It does not supply a retroactive remedy for these retained
samples. This implementation does not change the live endpoint, open a paid
stream, invent missing execution evidence or automatically fill recent gaps whose
archive is unavailable. Continuous plan extension and provider/capacity selection
remain separate work.

The decoder revision and transfer IDs stay at v5: this adds evidence orchestration,
not new transfer semantics. The new source audit values are additive; the existing
overlap-proof table is initialized for direct overlap/replay as well as bridge
plans. No transfer rows are deleted by the schema initialization. Older binaries
cannot validate the new evidence source. Restore a consistent backup for exact
rollback; do not mix old/new writers. Native-RPC-only completeness remains an
explicit stronger criterion, and missing historical state/unsupported instructions
remain explicitly unresolved for #17 rather than silent success.

Final local checks passed on the merged #22 base: formatting, default and archive
Clippy (`--all-targets --offline --locked -- -D warnings`), 82 default and 107
archive offline tests, with four existing opt-in/helper tests ignored in each
suite. Fixture sanitation/content checks passed. Independent read-only review
found no remaining correctness, provenance, recovery or resource blocker.
