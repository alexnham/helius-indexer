# Decoder v4 coverage validation — 2026-11-12

Revision v3 rejected an entire transaction when an ordinary Token-2022 checked
transfer lacked historical fee configuration. That suppressed unrelated classic
SPL transfers even though their instructed amounts and zero fees were known.
Revision v4 preserves the known gross amount and represents the unavailable fee
as null, consistent with the proposal. No current mint state, balance delta, or
assumed zero is substituted for historical fee evidence.

Two other captured non-transfer cases are now explicitly excluded: native SOL
UnwrapLamports (45) and Token-2022 HarvestWithheldTokensToMint (26, 4).
Their known instruction shapes are validated. Unknown opcodes, malformed shapes,
authority changes, and insufficient execution evidence remain unresolved.

## Results

| Check | Original retained sample | Fresh five-second sample |
| --- | --- | --- |
| Finalized historical slots | 468377243–468377245 | 468522222–468522224 |
| Received live candidates | 494 | 371 |
| Final replay live decode failures | 39 | 17 |
| Shared decoded transactions | 55 | 39 |
| Quarantined source conflicts | 0 | 0 |
| Historical decode failures after final replay | 0 | 0 |
| Historical slots complete after final replay | 3 / 3 | 3 / 3 |
| Independently expected classic SPL USDC edges | 97 | 65 |
| Stored edges in sampled blocks | 97 | 65 |
| Missing / extra / wrong-amount edges | 0 / 0 / 0 | 0 / 0 / 0 |

The original sample improved from 18/97 to 97/97 independently identified edges.
The new live check ran for approximately 5.25 seconds including shutdown. Its
initial historical worker encountered fee harvesting and cancelled during the
third block at the shared deadline. The final native-unwrap/harvest exclusions
were then validated by replaying both sets of retained protobuf and RPC inputs
into fresh databases offline, with no further provider calls. Completion above
refers to final offline historical replay, not the initial timed run.

The regression corpus contains 38 original captured transactions and 97 expected
edges. `tests/coverage.rs` checks identities, accounts, exact amounts, known
classic fees, immediate uniqueness, and exact aggregate volume on duplicate
replay. The complete offline suite passes 57 tests; one additional retained-data
replay test is opt-in. Formatting and Clippy pass.

## Reproduction and limits

```sh
cargo test --offline --locked --test coverage
python3 scripts/compare-captured-usdc.py RAW_INPUT_DB FRESH_REPLAY_DB
INDEXER_VALIDATION_SOURCE=RAW_INPUT_DB \
INDEXER_VALIDATION_DESTINATION=NEW_DB \
cargo test --offline --locked --test overlap replay_retained_network_observations -- --ignored --nocapture
```

The Python checker independently parses compiled classic SPL instructions using
base58 and integer operations and compares stored IDs, amounts, and token
accounts. It reports and fails for missing, extra, or mismatched records. Against
the v3 database it reproduces 79 missing edges; against each final v4 replay it
passes. Its subset excludes transactions containing runtime failure evidence.
The checker and Rust historical ingestion share the same public RPC source;
correlated provider omissions remain possible. These finite checks certify
neither the entire live tail nor a 30-day window, nor all Token-2022 variants.

Unknown Token-2022 fees remain null and contribute to `unknown_fee_count` in
per-mint summaries. A complete instruction range does not mean complete optional
fee/owner metadata. Historical extension-state enrichment, confidential support,
and automated replacement across decoder revisions remain pending.

Revision v4 keeps stable instruction IDs but rejects ingestion into older-revision
captures. Existing records remain readable. Use a new validation database and
replay retained raw inputs; do not relabel old records or delete the original
validation databases. Automated correction/migration is ticket #8. Credentials,
raw databases, and transient live reports remain ignored local artifacts.

Program behavior was checked on 2026-11-12 against the primary
[Token-2022 processor](https://github.com/solana-program/token-2022/blob/main/program/src/processor.rs),
[fee instruction interface](https://github.com/solana-program/token-2022/blob/main/interface/src/extension/transfer_fee/instruction.rs),
and [classic SPL instruction interface](https://github.com/solana-program/token/blob/main/interface/src/instruction.rs).
The connected Helius documentation was also consulted for
[Token-2022 fee response semantics](https://www.helius.dev/docs/rpc/gettransfersbyaddress#token-2022-transfer-fees);
its endpoint reports recipient amount separately from fees, while this index
preserves the instructed gross amount. No enhanced-provider fee inference was used.
