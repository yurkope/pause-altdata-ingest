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
