# Congressional Purchase External Validation Result — 2024

## Status

**Primary preregistered test: NOT VALIDATED**

This result was generated only after the 2024 Quant reconstruction was frozen and the 2025 replay gate passed exactly. No Quant weights, policy mappings, ticker-policy exposures, or test thresholds were changed after observing 2024 congressional purchase outcomes.

## Outcome extract

GitHub Actions run: 44021330199

- raw source rows: 3,856
- duplicate source keys removed: 382
- normalized rows: 3,474
- House rows: 2,552
- Senate rows: 922
- source URL coverage: 100%
- valid in-year transaction dates: 100%
- purchase rows after purchase/ticker/dedup filtering: 1,412
- mapped policy-quarter cells with purchases: 44
- ticker coverage across normalized rows: 81.81%
- bioguide coverage: 99.86%
- QC: PASS

The ticker-policy exposure map was the frozen `signals/congress_trade_policy_exposure_map_v1.csv` used for the 2025 validation.

## Primary preregistered test

Hypothesis: quarter-over-quarter increase in Quant economic policy pressure is positively associated with the subsequent quarter's unique-member purchase count in exposed public-company tickers.

Because a pressure change requires a prior quarter and a subsequent outcome quarter, the 2024 test uses Q2 and Q3 pressure changes predicting Q3 and Q4 purchases.

Analytical sample:
- 26 policy-quarter observations
- 13 exposed policies

Results:
- Spearman rho: **+0.1337**, p = **0.5150**
- Pearson r: **+0.1862**, p = **0.3623**
- within-outcome-quarter permutation p (100,000 seeded permutations): **0.5001**

Interpretation: direction is positive, but the association is weak and statistically indistinguishable from chance in this sample. The preregistered primary hypothesis is **not validated**.

## Count model

Poisson GLM on unique-member purchase count with standardized pressure change and outcome-quarter control, HC0 robust standard errors:

- standardized pressure-change coefficient: **+0.1393**
- robust SE: **0.1107**
- p = **0.2086**
- incidence-rate ratio: **1.149**
- 95% CI for IRR: **0.925 to 1.428**

The count-model direction is positive but not statistically significant.

## Preregistered robustness outcomes

Raw purchase count:
- Spearman rho: **+0.0561**, p = **0.7854**
- Pearson r: **+0.1755**, p = **0.3913**

Lower-bound disclosed purchase dollars:
- Spearman rho: **+0.0904**, p = **0.6606**
- Pearson r: **+0.2442**, p = **0.2294**

None is statistically significant.

## Decision

The 2024 congressional-purchase outcome does **not** provide decision-grade evidence that increases in Quant policy pressure predict next-quarter congressional purchasing. This is consistent with the previously frozen 2025 external validation, which also failed to support the primary predictive congressional-buying claim.

Do not tune Quant weights, evidence definitions, historical policy mappings, or the ticker-policy exposure map to improve these results. Congressional purchasing remains an external validation outcome, not a model input.

The correct next research step is to test other independently motivated outcomes and/or extend the frozen historical reconstruction to 2023, increasing the number of out-of-sample policy-quarter observations without changing the model.
