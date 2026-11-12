# Policy v3 Shadow Validation — 2026-11-10

Read-only validation against production Neon data. No production objects or rows were changed.

## Database state

- Database size at validation: 936 MB.
- activity_policy_evidence rows: 141,346.
- Monthly allowance had reset and database was responsive.

## Resolver validation

- Activity-policy pairs: 98,046.
- Unknown evidence rows: 0.
- Legacy method-corroborated pairs: 3,654.
- Independently corroborated pairs: 3,654.
- Legacy false corroboration removed in current data: 0.
- Independent corroboration coverage: 3.73%.

Evidence-family pair distribution:
- activity_text only: 63,948.
- corpus_context only: 15,830.
- legislative_reference only: 14,614.
- activity_text + legislative_reference: 3,654.

Interpretation: current production evidence does not yet contain bill-provision refinement as a
separate evidence method, so the v2 method-count rule and v3 family-count rule happen to agree
today. v3 still prevents future provision refinements from creating false corroboration.

## Dollar conservation

CTE-only v3 allocation:
- eligible filings: 34,047.
- filing-policy pairs: 60,686.
- conservation violations > $0.01: 0.
- maximum absolute filing difference: 0.000000000015.
- total represented allocation: $4,056,792,898.40.

Exact v2/v3 reconciliation:
- Q1: $1,019,887,672.52 / 8,028 filings / 14,069 pairs / difference $0.00.
- Q2: $1,013,836,743.05 / 8,457 filings / 14,724 pairs / difference $0.00.
- Q3: $998,225,628.20 / 8,781 filings / 15,908 pairs / difference $0.00.
- Q4: $1,024,842,854.63 / 8,781 filings / 15,985 pairs / difference $0.00.

## Score calibration

Initial proposed quality weight of 15% was too strong for current evidence coverage.

Q4, 51 substantive policies:
- 5% corroboration weight: mean absolute rank change 0.33; max 2.
- 10% corroboration weight: mean absolute rank change 0.86; max 4.
- 15% corroboration weight: mean absolute rank change 1.39; max 7.

At 15%, TRADE_TARIFFS moved #1 -> #5 solely because it currently has no second evidence family,
while PATENT_POLICY moved seven positions with a 40.7% corroboration rate. This is too much
influence while independent evidence coverage is sparse.

Decision: calibrate initial v3 shadow score to 5% independent corroboration:
- 38% allocated exposure
- 28% clients
- 19% activities
- 10% filings
- 5% independent corroboration

This preserves v2 behavior while introducing a modest quality premium. Revisit the quality
weight as independent evidence coverage grows.

## Promotion status

Resolver semantics: PASS.
Dollar conservation: PASS.
Unknown evidence-family guard: PASS.
Initial 15% score calibration: REJECTED as too aggressive.
5% score calibration: ACCEPTED for continued shadow evaluation.

No production promotion performed.
