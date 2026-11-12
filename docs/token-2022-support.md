# Token-2022 support

Verified 2026-11-12 against primary program sources. Decoder revision
`token-transfers-v4` keeps identity encoding unchanged. Amount is instructed gross
base units; fee is withheld base units, separately represented as exact decimal
JSON strings. No floating point or balance-delta reconstruction is used.

| Variant / extension | Support and execution evidence |
| --- | --- |
| Classic SPL Transfer / TransferChecked | Supported, fee zero |
| Token-2022 Transfer (3) | Supported, fee zero: successful unchecked transfers cannot use fee accounts |
| Token-2022 TransferCheckedWithFee (26, 1) | Supported; instruction amount, decimals and expected fee validated by successful execution |
| Token-2022 TransferChecked (12), zero or self | Supported; no tokens withheld |
| Token-2022 TransferChecked (12), other | Known gross amount indexed; fee null without execution-time extension evidence |
| Transfer hooks, permanent delegate, memo / CPI guard | Supported when part of the supported successful transfer variants; extra accounts do not create additional edges |
| HarvestWithheldTokensToMint (26, 4) | Recognized fee maintenance; emits no ordinary transfer edge |
| UnwrapLamports (45), either token program | Recognized native SOL movement; excluded from token transfer edges |
| Other fee configuration / withdrawal | Explicitly unsupported extension instructions; not ordinary instructed transfer edges |
| Confidential transfers, other extension opcodes | Explicitly unsupported pending #7 or further evidence |
| Authority changes | Unsupported, even if transaction-end owners agree |

Both outer and inner variants use the same contract. All inner token program
invocations need complete runtime invocation/success evidence. A runtime failure
anywhere in such a transaction conservatively rejects it, including a caught
parent failure that could roll back an otherwise successful inner transfer.
Failed transactions emit no transfers. Unknown instructions and insufficient
historical evidence retain the full sanitized input and failure status in SQLite;
transaction output is atomic and coverage remains `unknown`.

Successful checked-with-fee execution proves the encoded expected fee matched the
mint's epoch-specific fee at execution. Self-transfers are validated but return
before withholding, so their actual fee is zero. Ordinary checked transfers need
execution-time extension state to resolve their fee. The known instructed amount
is retained while the fee stays null; no current RPC mint state is queried or
substituted. Unknown fees do not invalidate an otherwise fully decoded transfer
set. `summary` reports `known_fees_raw` alongside `unknown_fee_count`; recipient
proceeds cannot be inferred when a fee is unknown. Historical owner resolution retains the existing
conservative pre/post agreement rule and otherwise returns null. There is no
historical extension-state adapter yet. Recipient proceeds are not exposed.

Primary references:

- [Instruction encoding and account order](https://github.com/solana-program/token-2022/blob/main/interface/src/extension/transfer_fee/instruction.rs)
- [Processor fee calculation, expected-fee comparison, unchecked restrictions and self-transfer handling](https://github.com/solana-program/token-2022/blob/main/program/src/processor.rs)
- [Classic SPL native unwrap instruction](https://github.com/solana-program/token/blob/main/interface/src/instruction.rs)
- [Program instruction extension discriminator](https://github.com/solana-program/token-2022/blob/main/interface/src/instruction.rs)
- [Solana transfer fees](https://solana.com/docs/tokens/extensions/transfer-fees)

`tests/fixtures/mainnet-token-2022.json` is a public finalized capture obtained
2026-11-11 from Solana mainnet RPC, slot 468265270, signature
`4N8K6QRBhAESv7SBdxEkEN42PoJ2YDnVbhChMXS6SjTTDC1HH5H5bS553aLEuDUk8JRWzEzRyojkFZeoKdaCNnYG`.
It independently identifies representative Token-2022 mints
`pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn` and
`SPCXxcqXj6e5dJDVNovHN8744zkbhM2bYudU45BimGb` through runtime account keys,
checked mint operands and balance metadata. It does not establish their fee
configuration, so the captured transfers now decode with null unavailable fees. Application
logs and return data were removed; public runtime evidence and compiled bytes
remain. Fee-enabled cases in tests are synthetic instruction-layout regressions,
not claimed on-chain fee captures or independent completeness validation.

Revision v4 removes v3's whole-transaction rejection for unavailable ordinary
checked-transfer fees. The 38-transaction captured corpus in
`mainnet-coverage.json` verifies 97 classic SPL USDC edges formerly suppressed by
this rejection in mixed-token transactions. No previously unknown fee is claimed
as known. Execution-evidence and unknown-instruction failures remain explicit.

Fee support itself does not change storage schema; the overlap slice adds source
auditing separately. The decoder revision intentionally conflicts with
previously ingested same-signature captures during ordinary ingestion. Use
[explicit offline revision repair](decoder-repair.md) to replace prior output
from retained captures and reverify retained historical blocks, or use a fresh
validation database. Unknown fees remain null. Existing records remain readable;
in-place repair requires a backup for exact rollback. No production migration
is implied.
