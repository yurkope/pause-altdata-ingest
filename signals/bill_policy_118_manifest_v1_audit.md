# 118th Congress bill-policy mapping v1 audit

Date: 2026-11-11

## Purpose
Outcome-blind historical mapping for Quant reconstruction. Congressional trading outcomes were not used to choose, add, remove, or label bills.

## Source corpus
Production policy.bill contains 345 118th-Congress bills (204 HR, 141 S). High-confidence candidate/verified bill mentions cover 343 distinct bills in the current reference corpus.

## v1 manifest
File: signals/bill_policy_118_manifest_v1.csv

- 81 mapped bills
- 31 canonical policies
- 1,259 high-confidence activity mentions in the current reference corpus
- 0 mapped bills with zero high-confidence mentions

## Mapping standard
Mappings are conservative and deterministic. Bill titles must identify a policy category that already exists in the frozen canonical taxonomy. House/Senate companion bills are mapped consistently when both are present. Broad omnibus and generic appropriations titles are not forced into substantive policy labels unless the canonical policy is itself an appropriations category with a specific title (for example Defense, Energy & Water, or THUD).

The manifest intentionally leaves many bills unmapped. Coverage is not a reason to force attribution.

## Examples
- HR 2670 / S 2226 -> DEFENSE_AUTHORIZATION
- HR 3935 / S 1939 -> FAA_AVIATION
- HR 4763 -> DIGITAL_ASSETS
- HR 8467 -> FARM_BILL
- HR 8818 -> DATA_PRIVACY
- S 2714 -> AI_POLICY
- S 4630 -> CYBERSECURITY
- S 4753 -> PERMITTING_NEPA
- HR 8774 / S 4921 -> DEFENSE_APPROPRIATIONS

## Important limitation
This manifest solves bill-to-policy ownership. Historical 2024 reconstruction still requires bill-reference extraction/resolution against the 2024 LDA activity corpus. The production policy.bill_mention rows are tied to production activity IDs and cannot be copied into the ephemeral 2024 database.

## Next gate
Run the same bill-reference resolver on the ephemeral 2024 activities, join only candidate/verified references with confidence >= 0.85 to this manifest, combine with historical-safe activity-policy-v1 evidence, conserve filing dollars, and score with the frozen 40/30/20/10 economic formula.
