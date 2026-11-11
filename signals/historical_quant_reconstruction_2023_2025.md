# Historical Quant Reconstruction — 2023–2025

Date: 2026-11-10

## Finding

Production Neon currently contains LDA filings for 2025 only. The existing ingestion code already
supports arbitrary historical years: sources/lda/ingest.py requires --year and queries the official
LDA API with filing_year=<year>. Filing and activity writes are idempotent/upserted.

Therefore 2023–2024 can be reconstructed with the same source schema and model methodology as 2025.

## Storage-safe architecture

Do NOT permanently append two raw LDA years to the production branch. Production is near the 1 GiB
logical-size limit.

For each historical year:
1. create an ephemeral reconstruction environment/branch;
2. ingest official LDA filings for that year using the existing year parameter;
3. run the frozen policy evidence/classification/resolver logic;
4. run the same dollar-conserving filing-policy allocation;
5. calculate quarterly economic pressure components;
6. export only the compact policy-quarter output plus audit totals;
7. discard the ephemeral raw-data environment after explicit approval.

Permanent historical output is tiny:
policy_code, year, quarter_num, allocated_amount, unique_clients, activity_count, filing_count,
economic_pressure_score, score_change, signal_state, classifier/model version.

Congressional transaction outcomes must never be available to the reconstruction/classification
steps.

## Comparability gates

A reconstructed year is accepted only if:
- official LDA filing counts reconcile to the source extraction;
- filing dollars conserve to <= $0.01 per filing;
- no duplicate filing-policy or activity-policy pairs;
- canonical taxonomy is frozen;
- classifier/evidence version manifest is frozen;
- pressure-score formula is identical to the 2025 economic score;
- quarterly score normalization is performed within the quarter exactly as in 2025;
- no congressional-trade data is used in policy classification, mapping, weights, or QC.

## Important historical-model rule

Use current frozen Quant taxonomy/classifier logic to reconstruct historical source data, but label
the result as a retrospective historical reconstruction. Do not imply that the current taxonomy or
classifier literally existed in 2023/2024.

For external-validation timing, the inputs are historical LDA records and the congressional outcome
is withheld, so the reconstruction remains outcome-independent. Any AI/semantic classification that
could use post-period knowledge must be separately audited or excluded from the primary historical
test.

## Execution order

2024 first, then 2023. 2024 provides the nearest-period comparability check to 2025 before extending
farther back.


## Pre-outcome 2025 replay acceptance gate

Frozen before inspecting any 2024 congressional-trade outcome.

The historical-safe classifier/replay is accepted for external validation only if its 2025 replay, compared with production policy_pressure_v2, satisfies all of the following:
- Spearman rank correlation of quarterly economic pressure >= 0.95 across overlapping substantive policy-quarter cells;
- mean absolute economic-pressure score difference <= 5.0 points;
- quarterly top-10 policy overlap >= 80% in every quarter;
- filing-policy allocation conserves each eligible filing to <= $0.01;
- no duplicate activity-policy or filing-policy pairs;
- the replay uses the same 40/30/20/10 economic score weights and within-quarter percent-rank normalization as production v2.

Failure of any gate means 2024 congressional outcomes remain withheld from model tuning. The replay must be repaired from source/model evidence only, or the historical test must be reported as non-comparable.

### Historical-safe evidence rule

Primary historical reconstruction excludes any semantic/AI evidence that can incorporate post-period knowledge. Congress-specific bill anchors may be used only when the corresponding historical Congress bill corpus and frozen mapping are reproducibly available. A deterministic activity-text baseline will be replayed on 2025 first to quantify how much production v2 behavior it recovers before it is allowed to score 2024/2023.
