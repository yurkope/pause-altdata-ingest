# 2025 Historical Quant Replay Gate

Status: **PASS**

Purpose: pre-outcome comparability gate for the historical Quant reconstruction. This test was completed before examining 2024 congressional stock-purchase outcomes.

## Replay basis

The replay rebuilt filing-policy allocation and quarterly pressure scoring from the frozen 2025 production evidence families:

- `activity-policy-v1 / activity_regex`
- `bill-policy-v2 / bill_anchor`

The replay used the same canonical policy roles and parent/child fallback logic as the historical scorer, conserved each mapped filing's disclosed lobbying amount across eligible leaf policies, and recomputed the 40/30/20/10 within-quarter pressure score.

## Preregistered gates and results

| Gate | Threshold | Result | Status |
|---|---:|---:|---|
| Overlapping policy-quarter cells | substantive overlap | 204 / 204 | PASS |
| Spearman rank correlation | >= 0.95 | **1.0000** | PASS |
| Mean absolute score error | <= 5.0 | **0.0** | PASS |
| Q1 top-10 overlap | >= 80% | **100% (10/10)** | PASS |
| Q2 top-10 overlap | >= 80% | **100% (10/10)** | PASS |
| Q3 top-10 overlap | >= 80% | **100% (10/10)** | PASS |
| Q4 top-10 overlap | >= 80% | **100% (10/10)** | PASS |
| Dollar conservation violations | 0 | **0** | PASS |
| Maximum allocation difference | <= $0.01 | **$0.000000000015** | PASS |
| Duplicate filing-policy rows | 0 | **0** | PASS |

Replay rows: **204**  
Production `policy.policy_pressure_v2` rows: **204**  
Replay substantive policies: **51**

## Decision

The historical allocation/scoring methodology passes the frozen 2025 comparability gate without threshold relaxation or outcome-driven tuning. The 2024 reconstruction methodology can therefore be frozen for outcome validation.

The next permitted step is to evaluate the already-frozen 2024 Quant scores against the independently withheld 2024 congressional stock-purchase outcome data. No 2024 score or mapping changes should be made in response to those outcomes.
