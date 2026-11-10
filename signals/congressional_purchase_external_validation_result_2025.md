# Congressional Purchases External Validation — 2025 Full-Corpus Result

Date: 2026-11-10

## Status

Decision-grade first pass completed against the frozen v1 ticker-policy exposure map.
Congressional trades remain an external validation outcome and were not used to tune Quant.

## Corpus QC

DataDawn government-derived stock_trades extract for transaction dates in calendar 2025:
- raw rows: 8,810
- duplicate transaction keys removed: 569
- unique rows after deduplication: 8,241
- House: 7,325
- Senate: 916
- purchases: 3,984
- sales partial: 1,230
- sales full: 3,004
- exchange: 22
- ticker coverage: 91.09%
- Bioguide coverage: 99.88%
- source URL coverage: 100%
- invalid/out-of-year transaction dates: 0
- QC: PASS

The frozen primary exposure map produced 50 policy-quarter cells containing purchases.

## Pre-registered primary hypothesis

An increase in Quant economic policy pressure is positively associated with the subsequent rate of
purchases in economically exposed public companies.

Primary independent variable:
quarter-over-quarter policy pressure score change.

Primary outcome:
next-quarter unique congressional buyers in frozen Tier-1 exposed tickers.

Because Q1 has no prior score change and 2026 Q1 Quant pressure is not in this 2025 panel, the
primary test has 28 policy-quarter observations (14 policies x Q2/Q3 signal quarters).

## Primary result — FAIL / NULL

Delta pressure -> next-quarter unique buyers:
- Spearman rho = -0.066, p = 0.740
- Pearson r = +0.099, p = 0.618
- within-quarter 20,000-draw permutation p approximately 0.740

Related next-quarter outcomes:
- purchase count: Spearman rho = -0.031, p = 0.876; Pearson r = +0.140, p = 0.477
- disclosed lower-bound dollars: Spearman rho = -0.035, p = 0.860; Pearson r = +0.121, p = 0.541

Conclusion: the pre-registered hypothesis is not supported in the 2025 panel.

## Secondary robustness checks

Raw buying is confounded by how frequently a ticker is traded by Congress, so purchase activity was
normalized against all congressional transactions in the same frozen exposed names.

Delta pressure -> next-quarter purchase share:
- Spearman rho = +0.263, p = 0.177
- Pearson r = +0.313, p = 0.105

Delta pressure -> next-quarter unique-buyer share:
- Spearman rho = +0.274, p = 0.159
- Pearson r = +0.254, p = 0.193

Self-owned purchases only:
- next-quarter self purchase count: Spearman rho = -0.057, p = 0.772
- next-quarter unique self buyers: Spearman rho = -0.044, p = 0.825

Same-quarter exploratory buyer share:
- Spearman rho = +0.293, p = 0.060
- Pearson r = +0.262, p = 0.093

The normalized-share relationships are directionally interesting but NOT statistically sufficient
and are secondary/exploratory. They must not be presented as validation of Quant.

## Decision

1. Do not claim congressional purchases validate Quant based on 2025.
2. Preserve the null primary result; do not change v3 weights or exposure mappings.
3. Preserve normalized purchase/buyer share as a research hypothesis for a larger time-series test.
4. The limiting factor is now Quant history, not congressional transaction availability.
5. A stronger future test requires additional years/quarters of point-in-time Quant pressure or a
   daily/monthly historical reconstruction that is frozen independently of trade outcomes.

This result improves the project by eliminating a plausible but unsupported validation claim.
