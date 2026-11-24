# QA status — historical investment validation

## Current disposition

The strict contemporaneous-company universe is a **null result pending QA**, not a validated extension of the original curated-map signal.

Accepted strict-universe run: 44163229692.

- 1,146 security-quarter observations
- 393 securities with usable prices
- six signal quarters (2023 Q2-Q4, 2024 Q2-Q4)
- mean quarter-equal-weight 90-day excess vs SPY: +0.289%
- p=0.856
- quarter excesses: -2.776%, +2.344%, -3.307%, +4.499%, +3.973%, -2.998%

## Independent QA findings so far

QA run 45443902654 independently reproduced the result artifact structure and confirmed there are no duplicate ticker-quarter weights.

Identity audit:
- 104,444 LDA client-policy rows
- 10,551 LDA clients
- 14,923 rows with a unique normalized SEC-name match
- 1,260 matched LDA clients
- 529 matched SEC tickers before price availability
- 86,780 unmatched rows
- 2,741 rows whose normalized name maps ambiguously in SEC reference
- 849 matched rows have normalized names shorter than five characters
- 8 rows have nonstandard ticker forms

This confirms the first deterministic identity rule has broad coverage but is not yet sufficiently precise to call the null decision-grade. Short/common normalized names and current SEC ticker identity can introduce false or temporally stale matches. Failed/delisted price retrieval also creates survivorship risk.

## 2022 reconstruction

Run 44162508293 succeeded:
- 93,516 official quarterly filings
- 182,115 activities
- 59,129 regex evidence rows
- zero bill-anchor evidence by design
- 25,009 allocated filings
- conservation violations: 0
- max allocation difference: 9e-12
- allocated amount: $3.06282444234B
- 184 score rows / 46 policies

2022 is activity-text-only and is quarantined from return inference until the activity-text-only methodology is compared with full Quant on 2023-2024.

## QA gate before further investment conclusions

1. Finish activity-text-only comparability test.
2. Tighten historical company identity without consulting returns.
3. Quantify coverage lost under high-precision identity rules.
4. Handle historical/delisted ticker identity explicitly.
5. Re-run strict universe only after the identity rule is frozen.
6. Decompose curated-map vs contemporaneous membership to determine why the original +6.18% disappears.
7. Do not alter signal threshold, horizon, Quant weights or policy mappings to improve results.
