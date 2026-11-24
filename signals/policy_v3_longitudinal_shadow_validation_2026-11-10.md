# Policy Pressure v3 Longitudinal Shadow Validation — 2026-11-10

Read-only validation against 2025 Q1-Q4 production Neon data. No production writes.

## CI

Resolver integration commit 95bce352 passed GitHub Actions run #195.

## Independent evidence coverage by quarter

- Q1: 1,668 / 21,130 activity-policy pairs = 7.89%.
- Q2: 2,180 / 24,799 = 8.79%.
- Q3: 2,500 / 26,509 = 9.43%.
- Q4: 2,579 / 25,608 = 10.07%.

Coverage rises through the year after adding only the approved institutional_target family.

## Ranking stability

At the 5% quality weight:
- Q1 mean absolute rank change 0.51; max 4.
- Q2 mean 0.69; max 4.
- Q3 mean 0.45; max 4.
- Q4 mean 0.22; max 2.

The quality adjustment is therefore modest and increasingly stable in Q4.

## Signal-state audit

Using the quality-adjusted score itself for momentum initially caused three Q2 state changes:
- STUDENT_LOANS: v2 +6.0 ACCELERATING; quality-adjusted +2.0 PERSISTENT.
- TAX_RD: v2 -2.0 PERSISTENT; quality-adjusted -5.2 COOLING.
- TELEHEALTH: v2 -4.4 PERSISTENT; quality-adjusted -7.8 COOLING.

All three had 0% independent corroboration. These were not contradictory new observations;
they were threshold effects caused by applying a relative evidence-quality penalty to longitudinal
momentum.

## Design correction

v3 now exposes two concepts:
1. economic_pressure_score: original 40/30/20/10 economic/breadth score.
2. pressure_score_v3: 38/28/19/10/5 quality-adjusted ranking score.

Momentum, score_change, and signal_state are derived from economic_pressure_score, not the
quality-adjusted score.

This means:
- evidence quality can modestly change cross-sectional ranking/confidence;
- evidence sparsity cannot manufacture ACCELERATING or COOLING states;
- dollar allocation remains unchanged.

Commit implementing the separation: 7dfa40b9e1eb2b44a5d567cfb20028b986360fdc.

## Conclusion

PASS for continued v3 shadow operation.

Do not replace production v2 yet. The v3 architecture is now stable enough to run as a repeatable
shadow pipeline and accumulate additional quarters/evidence before promotion.
