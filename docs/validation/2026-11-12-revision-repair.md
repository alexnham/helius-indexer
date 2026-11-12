# In-place revision repair validation — 2026-11-12

A SQLite backup of the original v3 five-second validation database was repaired
locally. The original database was not modified. No key was read and no provider
request, archive download, full backfill, or paid resource was used for this check.
Local artifacts remain in the ignored data directory.

| Observation | Result |
| --- | --- |
| Original retained captures | 934 |
| Capture repair scope | 468377243–468377271 |
| Bounded capture batches | 10 |
| Captures corrected before block replay | 895 |
| Unresolved before block replay | 39 |
| Retained full-block replay scope | 468377243–468377245 |
| Cached blocks / source attempts | 3 / 0 |
| Whole-block transactions decoded | 801 |
| Whole-block decode/write failures | 0 / 0 |
| Verified historical slots after replay | 3 / 3 |
| Final current-revision captures | 1,201 |
| Final remaining older captures | 36 |
| Independently expected / stored sampled USDC edges | 97 / 97 |
| Missing / extra / wrong-amount sampled edges | 0 / 0 / 0 |
| Unique stored USDC transfers across retained scope | 788 |
| Known gross USDC volume in raw units | 59311804652 |

The original whole-block raw inputs contain candidates previously rejected by v3;
retained-block replay imports those in addition to correcting existing captures.
Compatible block observations also resolve three previously unresolved captures.
The final exact USDC summary also matches a fresh v4 replay of the same retained
inputs, including transfer count, raw volume, fees, and unknown-fee count.
The remaining 36 live-tail captures comprise 26 incomplete-execution-evidence
cases, nine unsupported authority changes, and one unsupported token instruction.
That wider 29-slot capture scope is still incomplete and its live tail is not
verified. The independent comparison covers classic SPL USDC instructions in the
three retained blocks, not all token variants or full-chain/provider completeness.

Reproduction uses `repair`, `repair-history`, and
`scripts/compare-captured-usdc.py` as documented in the
[repair procedure](../decoder-repair.md). Per-batch reports and events were
retained locally. The comparison shares the original RPC source and therefore
cannot exclude correlated provider omissions.

The ordinary offline suite passes 74 tests, with four ignored helper/optional
entries: three process-exit helpers invoked by their normal parent tests, and the
optional retained network-data replay test. Formatting and Clippy pass.
Independent review caught a stale-visibility bug on failed block decoding; a
regression reproduced it before the fix and now passes. The subsequent read-only
review also caught scheduling that excluded unresolved markers already on the
target revision. A regression reproduced the hidden-output lifecycle and now
passes after making those captures explicitly retryable.
