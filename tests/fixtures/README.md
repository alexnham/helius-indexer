# Transaction fixtures

## Captured mainnet transaction

`mainnet-transfer.json` was captured from the public Solana mainnet JSON-RPC
endpoint on **2026-11-11**, with finalized commitment, JSON encoding, and
`maxSupportedTransactionVersion: 1`.

- Signature: `5HaYHWnniYgjv3Yv4Qv712sUnhNwhNVb9x3TLkgeF7g1yCZ3zwMLEYwvkRQiymmTwFMt4QGjusS8PdZX7kDxbX91`
- Slot: `468265447`
- The two classic SPL Token transfers are at outer/inner positions `(2, 1)` and
  `(3, 0)`, each with an instruction amount of `23360000` USDC base units.
- Public keys, signature, instruction data, ordering, token metadata, and runtime
  execution evidence are preserved. Free-form application logs and return data
  were removed. Runtime failure reasons, if present, are stripped after `failed:`.
- No credentials, private keys, or provider authentication URLs are included.
- `mainnet-expected.json` records independently inspected instruction positions,
  amounts, account references, and metadata, not the Rust decoder's output.

This is evidence for one captured transaction, not a full completeness benchmark.
Tests additionally construct synthetic, schema-compatible captures for outer and
checked transfers, u64 limits, zero/self transfers, failures and corrupt metadata.
Those synthetic variants are not represented as on-chain transactions.

## Format references

Verified **2026-11-11** against primary sources:

- [Solana RPC JSON structures](https://solana.com/docs/rpc/json-structures):
  compiled instructions, token balance account indices, inner groups, loaded keys.
- [SPL Token instruction implementation](https://github.com/solana-program/token/blob/main/interface/src/instruction.rs):
  Transfer opcode 3, TransferChecked opcode 12, account order, little-endian u64,
  and the checked decimal byte.
- [Helius getTransaction guide](https://www.helius.dev/docs/rpc/guides/gettransaction):
  response metadata, commitment, transaction versions, and encoding.
- [Solana CPI documentation](https://solana.com/docs/core/cpi):
  inner calls and execution behavior. Missing or failed runtime evidence is
  conservatively unsupported in this offline decoder.

Capture acquisition is a bounded, manual development step. Normal tests read
committed files and never contact an RPC provider.

The sanitized Token-2022 mainnet capture and its supported/unsupported evidence
are documented in [the support matrix](../../docs/token-2022-support.md).

## Mixed-token regression

`mainnet-mixed-transfer.json` was retained from the bounded finalized RPC check
recorded on 2026-11-12, slot 468377243. It contains a USDC transfer and a related
classic SPL transfer for another mint in one transaction. Public instruction and
execution evidence are preserved; free-form application logs and return data
were removed. It catches the former source discrepancy where live ingestion
filtered records before persistence while historical ingestion stored both.
The wire view used in tests is synthesized from these known fields, not presented
as a raw captured gRPC packet. No credential-bearing transport data is included.

## Three-slot coverage corpus

`mainnet-coverage.json` contains 38 successful transactions from finalized RPC
blocks 468377243–468377245 retained in the bounded 2026-11-12 validation. It includes
97 classic SPL USDC instruction edges identified independently with Python
base58/integer parsing, not Rust decoder output. Each expected edge fixes its
signature/instruction identity, instructed amount, and source/destination token
accounts. The corpus excludes transactions with runtime failure evidence.
Application logs and return data were removed; runtime execution evidence and
compiled instructions remain. This is a fixed regression corpus, not proof of
all-chain completeness or Token-2022 fee configuration. `tests/coverage.rs` checks
decoding, exact values, persisted uniqueness, and summary volume across retries.

`mainnet-unwrap.json` is a finalized classic SPL UnwrapLamports capture from slot
468377243. `mainnet-fee-maintenance.json` is a finalized Token-2022
HarvestWithheldTokensToMint capture from slot 468522222, retained during the
second five-second check. Both preserve runtime evidence with application logs
and return data removed. Neither contains an ordinary instructed transfer edge;
these are captured exclusions, not synthetic successful transfers.


## Authority / fee maintenance corpus

`mainnet-maintenance.json` contains 11 successful finalized public mainnet captures
retained during bounded archive/RPC validation on 2026-11-12. Each case includes
its source kind, signature, slot, compiled instructions, token metadata, runtime
execution evidence and independently enumerated ordinary transfer expectations.
Archive receipts include epoch/range/digest/revision when available. Application
logs and return data were removed; runtime failure reasons were stripped. Raw
CARs, provider endpoints and credentials are not included.

Captured slots: 468265446 (four fee-withdrawal cases), 468265448 (two close-
authority cases), 468265450 (one), 468287999 (two), 468288003 and 468288004 (one
each). The 61 expected edges come from inspecting compiled Transfer / TransferChecked
instructions and metadata, without treating withdrawals or balance deltas as
ordinary transfer edges. The first nine captures came from Old Faithful via the
pinned maintained Jetstreamer adapter; the last two came from finalized mainnet
RPC blocks. AccountOwner changes and mint-fee withdrawals are synthetic tests,
not additional captured examples. Decoder v5 requires `mainnet-expected.json`'s
revision field to change; its two transfer IDs/amounts remain unchanged.

The first maintenance case also includes one sanitized finalized RPC observation
of the same signature, for identical-output and both-arrival-order reconciliation
checks. There are 11 unique transactions and 12 source observations in total.

## Explicit RPC truncation pairs

`mainnet-truncated-rpc.json` contains two matched finalized archive/RPC transaction
pairs at slot 468265447, each with 18 independently enumerated ordinary transfer
edges. Original archive logs had 194 entries and RPC logs 130, including a literal
truncation marker at position 127 and subsequent observed lines. The captures
preserve signatures, compiled bytes, token metadata, runtime evidence and the
marker; free-form application logs/return data were removed. Fixture log counts
are smaller after sanitation; `original_log_lines` records original counts.
Expected IDs, raw amounts, fees and positions come from compiled transfer operands,
not balance deltas or generated Rust output. Synthetic block headers used by
adapter tests are not claimed as captured block anchors. Full-block retained-input
recovery evidence and limitations are in `docs/rpc-evidence-recovery.md`.
