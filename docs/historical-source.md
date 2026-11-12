# Historical source validation

Verified 2026-11-11 (America/Toronto). This slice selects bounded finalized JSON-RPC
`getBlock` as a fallback, without selecting production archive infrastructure.

## Archive findings

Update 2026-11-12: the [maintained optional adapter](archive.md) now reads bounded
mainnet CAR ranges and measures archive freshness. The findings below describe
the earlier RPC selection; the newer validation note supersedes its untested
archive compatibility and no-download statements.


[Old Faithful's introduction](https://docs.old-faithful.net/) describes CAR
snapshots produced at epoch end, a genesis archive through the previous epoch,
and expected publication 10–20 hours after epoch end. That is a documented
expectation, not measured availability or a recovery guarantee. It identifies
`files.old-faithful.net` as a public mirror. A bounded HEAD request to that root
returned HTTP 400 during this verification; it did not establish any epoch's
availability. No archive was downloaded and no publication lag was measured.

[Jetstreamer's upstream README](https://github.com/anza-xyz/jetstreamer) now says
maintenance moved to Vaultic; Anza's repository was archived October 15, 2026.
Its documented adapter is `JetstreamerRunner` plus `Plugin`, including slot-range
replay. It describes executed transactions, status metadata with logs, and no
account updates. These facts do not establish execution-time account/extension
history or complete metadata for any selected range. Its throughput claims are
not measurements of this indexer.

The [archive schema](https://github.com/rpcpool/yellowstone-faithful/blob/main/ledger.ipldsch)
contains transaction data and metadata frames, slot/index, and block parent/time.
This establishes representational capacity, not field completeness in archived
transactions. [Jetstreamer's manifest](https://github.com/anza-xyz/jetstreamer/blob/main/Cargo.toml)
currently uses Agave/Solana 4 transaction/status dependencies. An end-to-end check
of legacy/v0/v1 decoding through the current Vaultic adapter is still required;
no archive transaction-format compatibility is claimed here.

## Selected fallback and overlap

[Solana's getBlock contract](https://solana.com/docs/rpc/http/getblock) supplies
full transaction records, block time, hashes and parent slot, with explicit
encoding, commitment and maximum transaction version. This adapter requests
`finalized`, `json`, `full`, and maximum version 1; normalizes each transaction
into the existing capture contract; and checks the endpoint's mainnet genesis
hash before fetching blocks. Missing metadata, unsupported formats and null/error
responses leave coverage unknown. Successful zero-transaction blocks establish
observed empty coverage. A finalized child's direct parent link supplies skip
evidence for intermediate slots; RPC errors alone never establish skipped slots.

A viable archive/recent bridge must exhibit a real shared finalized range with
matching block identities and decodable execution metadata. No bridge has been
measured: archive tip, RPC history depth, sustainable throughput, transport usage,
provider cost and actual archive format compatibility remain unverified. RPC
providers may prune history, rate-limit requests or reject version 1; these cases
remain unknown and retryable. The public endpoint needs no new credentials, but
is not a production historical availability commitment. One public-mainnet smoke request for slot 468265447 succeeded using this CLI
(with one genesis-hash check). The report retained 3,565,284 response bytes and
left coverage unknown because the shared decoder rejected Token-2022 candidates.
A separate SQLite comparison matched both previously captured transfer records
exactly. This is a functional check with a correlated public RPC source, not an
independent completeness reconciliation, throughput benchmark, historical-depth
or bridge guarantee. No paid provider or full backfill was used.

## Retained-block re-verification (2026-11-12)

Historical retries can reuse full retained RPC responses across decoder revisions.
The explicit `repair-history` command enables revision correction while replaying
those blocks with no network fallback. It imports newly supported candidates,
uses compatible audited execution metadata, and verifies slot completion only
when the full block succeeds. Missing retained blocks stay unknown. Reports
separate cached blocks from source attempts. See [revision repair](decoder-repair.md)
and its [measured validation](validation/2026-11-12-revision-repair.md); this does not
implement the Old Faithful/Jetstreamer archive bridge tracked in #17.
