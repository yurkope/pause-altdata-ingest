# Independent Evidence Coverage Expansion v1

## Goal

Increase the number of important activity-policy pairs supported by genuinely independent
evidence. Do not increase the scoring weight merely because coverage is sparse.

## 2026-11-10 diagnosis

The largest uncorroborated Q4 policies are predominantly activity-text-only. Direct inspection
found no explicit HR/S bill references in the selected text-only populations, so additional bill
parsing is not the missing source.

High-dollar examples:
- TRADE_TARIFFS: 1,827 text-only pairs, 0 currently corroborated.
- MEDICAID: 1,121 text-only.
- CRITICAL_MINERALS_SUPPLY_CHAIN: 773 text-only.
- CLEAN_ENERGY_TAX_IRA: 575 text-only.
- FARM_BILL: 598 text-only.
- DRUG_PRICING: 495 text-only.
- TAX_CORPORATE: 410 text-only.
- DATA_PRIVACY: 304 text-only.

## Candidate family: institutional_target

LDA government entities are reported separately from the activity description and can provide
an independent consistency signal when the targeted institution is policy-specific enough.

Generic targets such as HOUSE OF REPRESENTATIVES, SENATE, White House, and generic executive
offices are excluded.

A conservative hand-curated policy/institution map produced Q4 candidate coverage:
- TRADE_TARIFFS: 730 / 1,827 = 40.0%.
- MEDICAID: 472 / 1,121 = 42.1%.
- FARM_BILL: 362 / 598 = 60.5%.
- CRITICAL_MINERALS_SUPPLY_CHAIN: 355 / 773 = 45.9%.
- CLEAN_ENERGY_TAX_IRA: 244 / 575 = 42.4%.
- DRUG_PRICING: 164 / 495 = 33.1%.
- TAX_CORPORATE: 104 / 410 = 25.4%.
- DATA_PRIVACY: 27 / 304 = 8.9%.

These are candidates, NOT yet independently_corroborated production rows.

## Precision rule

Institutional targeting must not become circular confirmation. A government entity can count as
an independent family only when:

1. the policy-to-institution mapping is defined independently of the individual activity text;
2. generic Congress/executive targets are excluded;
3. the institution has a direct policy jurisdiction/administrative relationship;
4. the mapping is versioned and auditable;
5. precision is tested against negative/control policies before promotion.

## Next validation

Build a confusion/control audit: for each proposed institution, measure how often it appears
across unrelated canonical policies. Promote only mappings with sufficient specificity.
Until that audit passes, institutional_target remains diagnostic and does not affect the v3 score.
