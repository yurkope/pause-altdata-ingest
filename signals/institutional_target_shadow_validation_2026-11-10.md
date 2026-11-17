# Institutional Target Shadow Injection Validation — 2026-11-10

Read-only, in-query validation. No institutional evidence rows were persisted.

## Rules injected

- TRADE_TARIFFS <- USTR OR International Trade Administration OR CBP
- FARM_BILL <- USDA
- MEDICAID <- CMS

Each injected observation uses evidence_family = institutional_target.

## Coverage result

Across 98,046 activity-policy pairs:
- baseline independently corroborated: 3,654 (3.73%)
- shadow independently corroborated: 8,927 (9.10%)
- uplift: +5,273 pairs

New independent pairs:
- TRADE_TARIFFS: +2,484
- FARM_BILL: +1,449
- MEDICAID: +1,340

## Dollar conservation

Institutional evidence changes evidence quality only, never policy eligibility or filing allocation.

v2 vs shadow:
- Q1: $1,019,887,672.52 vs $1,019,887,672.52; difference $0.00
- Q2: $1,013,836,743.05 vs $1,013,836,743.05; difference $0.00
- Q3: $998,225,628.20 vs $998,225,628.20; difference $0.00
- Q4: $1,024,842,854.63 vs $1,024,842,854.63; difference $0.00

Filing counts and filing-policy pair counts are identical in every quarter.

## Q4 ranking stability at 5% quality weight

Key policies:
- TRADE_TARIFFS: v2 rank 1 -> v3 shadow rank 1; corroboration 40.0%.
- MEDICAID: 3 -> 3; corroboration 30.8%.
- FARM_BILL: 8 -> 8; corroboration 60.5%.

Largest observed rank movement among returned policies was two positions (RIGHT_TO_REPAIR 35 -> 33).
Most top policies were unchanged. CRITICAL_MINERALS and CLEAN_ENERGY each moved down one position
because they remain uncorroborated under the deliberately narrow promoted rules.

## Decision

PASS for continued shadow development.

The narrow institutional_target family materially improves independent evidence coverage without:
- changing any allocated dollars;
- changing filing/policy eligibility;
- destabilizing top Q4 rankings.

Do not persist institutional evidence yet. Next engineering step is to version the approved
policy/institution mapping as a source-controlled artifact and wire the resolver to derive this
family reproducibly in shadow mode.
