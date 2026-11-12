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
