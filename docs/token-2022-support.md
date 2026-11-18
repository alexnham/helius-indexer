# Token-2022 support

Verified 2026-11-12 against primary program sources. Decoder revision
`token-transfers-v5` keeps identity encoding unchanged. Amount is instructed gross
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
| WithdrawWithheldTokensFromMint (26, 2), WithdrawWithheldTokensFromAccounts (26, 3), HarvestWithheldTokensToMint (26, 4) | Recognized fee maintenance with validated length/account counts; emit no ordinary transfer edge |
| UnwrapLamports (45), either token program | Recognized native SOL movement; excluded from token transfer edges |
| InitializeMintCloseAuthority (25), InitializePermanentDelegate (35) | Recognized mint configuration; no transfer edge (v7) |
| Fee configuration (26, 0 / 5), other extension instructions | Explicitly unsupported pending further coverage; not silently treated as transfers |
| Confidential Transfer (27, 7), TransferWithFee (27, 13), current proof layout | Supported; explicit mint operand, hidden amount/fee null; see v6 boundary below |
| Confidential configuration, deposit/withdraw within one account (27, 0–6 / 8–12 / 14) | Recognized current layouts; no transfer edge |
| Other extension opcodes / confidential layouts | Explicitly unsupported; retained for retry, affected coverage incomplete |
| SetAuthority base types 0–3, either token program | No transfer edge; AccountOwner (2) clears affected account owner fields across the transaction, including change-and-restore; mint/freeze/close authorities preserve the existing owner evidence |
| Extension authority types | Explicitly unsupported |

Both outer and inner variants use the same contract. All inner token program
invocations need complete runtime invocation/success evidence. A runtime failure
anywhere in such a transaction conservatively rejects it, including a caught
parent failure that could roll back an otherwise successful inner transfer.
Failed transactions emit no transfers. Unknown instructions and insufficient
historical evidence retain the full sanitized input and failure status in PostgreSQL;
transaction output is atomic and coverage remains `unknown`.

Successful checked-with-fee execution proves the encoded expected fee matched the
mint's epoch-specific fee at execution. Self-transfers are validated but return
before withholding, so their actual fee is zero. Ordinary checked transfers need
execution-time extension state to resolve their fee. The known instructed amount
is retained while the fee stays null; no current RPC mint state is queried or
substituted. Unknown fees do not invalidate an otherwise fully decoded transfer
set. `summary` reports `known_fees_raw` alongside `unknown_fee_count`; recipient
proceeds cannot be inferred when a fee is unknown. Historical owner resolution retains the existing
conservative pre/post agreement rule and otherwise returns null. Any supported
AccountOwner change also makes all transfer owner fields for that account null,
regardless of whether pre/post observations agree. There is no
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


Revision v5 recognizes base authority changes and withheld-fee maintenance without
suppressing unrelated transfers. Eleven sanitized mainnet captures cover close-
authority changes and account-fee withdrawals, preserving 61 ordinary transfer
edges with exact IDs, positions, amounts, fees and owner evidence. AccountOwner
change-and-restore, mint withdrawal and malformed layouts have synthetic tests;
they are not claimed as additional on-chain captures. The six-slot retained
archive replay and explicit v4→v5 repair are documented in
[maintenance validation](validation/2026-11-12-decoder-maintenance.md).

V5 needs the same explicit revision repair as earlier revisions. Normalized
archive inputs can now be reverified offline with `repair-history-archive`, which
selects archived block receipts instead of RPC receipts and never downloads CAR
files. Back up before in-place repair; this changes decoded output, not storage
schema or transfer identity.


## Revision v6: confidential transfer queries

Verified 2026-11-14 against the [Token-2022 interface at revision
`e8ca680ca8aeb12f9598524b003c776c6a4168f2`](https://github.com/solana-program/token-2022/blob/e8ca680ca8aeb12f9598524b003c776c6a4168f2/interface/src/extension/confidential_transfer/instruction.rs).
The current `Transfer` (27, 7) and `TransferWithFee` (27, 13) instructions
encode source, mint and destination as the first three accounts. Supported data
lengths are 169 and 171 bytes respectively: two discriminators, a 36-byte
[decryptable balance](https://github.com/solana-program/token-2022/blob/e8ca680ca8aeb12f9598524b003c776c6a4168f2/clients/js/src/generated/types/decryptableBalance.ts),
two 64-byte [auditor ciphertexts](https://github.com/solana-program/token-2022/blob/e8ca680ca8aeb12f9598524b003c776c6a4168f2/clients/js/src/generated/types/encryptedBalance.ts),
and three/five proof offsets. Context accounts or the shared instructions sysvar
and authority must fit the equality, validity and range offsets. TransferWithFee's
fee offsets (bytes 168–169) are read only on transfer-fee mints, which the
instruction does not reveal, so they impose no account minimum. Record accounts
and multisig signers may add accounts. Older split-proof layouts and unknown variants remain unresolved.

Successful finalized execution is the evidence for program/proof validation; this
indexer does not verify cryptography, signatures or consensus. Captures must come
from a trusted finalized source. Synthetic fixtures test parsing rather than
establishing valid proofs or on-chain availability. Inner execution evidence and
atomic whole-transaction rejection follow the ordinary transfer rules above.

`confidential=true`, `amount_raw=null` and `fee_raw=null` survive persistence,
query and replay, including self transfers. Plain `Transfer` (27, 7) records
`fee_raw=0` instead: Token-2022 rejects it on transfer-fee mints, so success
proves no fee. `TransferWithFee` keeps a null fee. No encrypted field or balance delta
is treated as an amount. The explicit mint is sufficient even without token
balance metadata. `decimals` is then null; consistent execution-time token-balance
metadata may enrich it. Conflicting mint, decimals or token-program evidence
rejects the transaction. Owner evidence follows the same conservative rules as
ordinary transfers. Configuration and deposit/withdraw operations within one
account emit no edge; mint involvement alone never creates a transfer.

`summary` adds `unknown_amount_count` and `confidential_transfer_count` alongside
`known_volume_raw` and `unknown_fee_count`. A zero known volume with unknown
amounts is not a zero total volume. Coverage continues to describe instruction
indexing separately from unknown values. Unsupported/malformed candidates are
retained with bounded failure classes and prevent affected historical completion.

Compatibility: PostgreSQL stores transfer JSON as text, so no SQL migration or
rewrite is needed. V6 reads existing decimal-string amounts and numeric decimals;
new confidential records add nulls. Older binaries cannot read those null amounts.
Upgrade readers before ingesting v6 output. Ordinary ingestion rejects mixed
same-signature decoder revisions; use the explicit [repair procedure](decoder-repair.md)
and reverify affected retained blocks. Back up the stopped session before repair;
rollback to an older binary requires restoring the pre-v6 backup, including inputs,
output and coverage. Stable transfer identities are unchanged.

## Revision v7: Token-2022 mint setup

Verified 2026-11-17 against the [Token-2022 instruction
interface](https://github.com/solana-program/token-2022/blob/main/interface/src/instruction.rs)
(`unpack` arms, `main` branch): tag 25 is `InitializeMintCloseAuthority` and tag 35 is
`InitializePermanentDelegate`. Both configure a mint, have no sub-instructions and
move no balances, so v7 emits no edge for them instead of rejecting the transaction.
Found in the #31 overlap run, where three USDC transactions in slot 470015982 created
a Token-2022 mint and a USDC account in one call; see `tests/fixtures/README.md`.
Other standalone setup tags (29, 31, 32) and every extension family stay unsupported
until captured. Repair follows the v6 procedure; identities are unchanged.
