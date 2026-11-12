# Client Specialization Evidence Audit — 2026-11-10

## Question

Can prior-period client lobbying behavior serve as a genuinely independent evidence family for a
current activity-policy attribution?

## Independence rationale

Client specialization is based on the identity of the filing client and that client's historical
policy distribution. It does not use the current activity text, current bill reference, current
government target, or current-quarter outcome to generate the prediction.

This is independent at inference time only when training is strictly prior-period.

## Candidate rule

For a target quarter:
1. use only earlier quarters;
2. require at least 3 historical activity observations for the client;
3. require a unique dominant policy;
4. require dominant-policy share >=80%;
5. predict that policy for the client's target-quarter activities.

Raw out-of-period precision:
- Q2 from Q1 history: 88.16% (871/988)
- Q3 from Q1-Q2 history: 89.03% (2,460/2,763)
- Q4 from Q1-Q3 history: 92.17% (4,911/5,328)

A blanket rule is rejected because some policies perform poorly.

## Policy qualification

A stricter candidate family requires policy-level historical validation:
- at least 20 prior holdout predictions;
- >=85% precision;
- qualification must be based only on holdouts before the target quarter.

Walk-forward results:
- Q3, policies qualified using Q2 only: 93.27% precision (1,579/1,693).
- Q4, policies qualified using Q2+Q3: 95.09% precision (2,612/2,747).

Policies that satisfy the Q4 two-holdout gate include:
CRITICAL_MINERALS_SUPPLY_CHAIN, CYBERSECURITY, DEFENSE_APPROPRIATIONS, DIGITAL_ASSETS,
DRUG_340B, DRUG_BIOMEDICAL_POLICY, FARM_BILL, MEDICAID, RECYCLING_CIRCULAR_ECONOMY,
TRADE_TARIFFS.

## Coverage potential

Using the temporally qualified policy set as a retrospective shadow test:
- institutional/current independent baseline: 10,692 / 98,046 = 10.91%
- with qualified client-specialization evidence: 13,560 / 98,046 = 13.83%
- incremental: +2,868 activity-policy pairs.

13.83% is a retrospective coverage estimate, not a production claim. Q1 has no prior-quarter
history, and Q2 cannot have policy-level holdout qualification before inference.

## Decision

Client specialization PASSES as a promising independent evidence family concept, but is NOT yet
approved for production or for rewriting historical Q1/Q2 corroboration.

Approved next use:
- shadow walk-forward evidence beginning only after sufficient prior holdout validation;
- maintain point-in-time training;
- never train on the target quarter;
- never count client specialization when policy qualification is not established before inference.

The provisional 15% promotion gate remains unmet. Do not lower evidence standards to hit it.
