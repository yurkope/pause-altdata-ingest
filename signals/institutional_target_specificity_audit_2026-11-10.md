# Institutional Target Specificity / Control Audit — 2026-11-10

Read-only audit against Q4 2025 production Neon data. No production writes.

## Method

For each proposed policy-to-government-entity relationship:
1. restrict both policy and government-target populations to 2025 Q4;
2. count all activities targeting the institution;
3. count activities also labeled with the intended canonical policy;
4. calculate precision = intended-policy matches / institution-targeted activities;
5. calculate policy recall;
6. calculate lift versus the policy's base prevalence among Q4 activities;
7. inspect policy breadth as a contamination indicator.

Generic House, Senate, White House and broad executive-office targets were excluded a priori.

## Strong candidates

TRADE_TARIFFS:
- USTR: 49.0% precision, 38.5% recall, 12.02x lift.
- ITA: 44.6% precision, 2.5% recall, 10.94x lift.
- CBP: 27.0% precision, 3.0% recall, 6.62x lift.
- union of USTR/ITA/CBP: 45.3% precision, 40.0% recall, 11.12x lift.
Assessment: PROMOTE TO SHADOW INSTITUTIONAL FAMILY.

FARM_BILL -> USDA:
- 19.5% precision, 60.5% recall, 14.60x lift.
Assessment: PROMOTE TO SHADOW. High lift and exceptional recall, but USDA is broad; keep
confidence below trade-specific targets.

MEDICAID -> CMS:
- 24.1% precision, 30.8% recall, 9.66x lift.
CMS+HHS does not improve precision materially (24.3%) and cuts recall to 18.8%.
Assessment: PROMOTE CMS ONLY TO SHADOW; HHS adds little specificity.

## Precision-positive but sparse / review

CLEAN_ENERGY_TAX_IRA -> IRS+DOE:
- 31.8% precision, 1.2% recall, 24.83x lift (22 target activities).
Assessment: REVIEW. Strong conditional specificity but too sparse for broad coverage.

DATA_PRIVACY -> FTC+FCC:
- 16.7% precision, 2.0% recall, 24.60x lift (36 target activities).
Assessment: REVIEW. High lift, low recall.

CRITICAL_MINERALS -> BIS:
- 15.6% precision, 1.8% recall, 9.03x lift.
BIS+DOE: 18.2% precision but only 11 target activities / 2 matches.
Assessment: REVIEW / sparse.

## Reject as standalone corroboration

Broad institutions do not have enough standalone specificity:
- HHS for MEDICAID or DRUG_PRICING.
- Treasury for TAX_CORPORATE or CLEAN_ENERGY_TAX_IRA.
- DOE for CLEAN_ENERGY_TAX_IRA or CRITICAL_MINERALS.
- DOD for CRITICAL_MINERALS.
- FTC alone and FCC alone for DATA_PRIVACY.
- CMS for DRUG_PRICING is useful context but materially less specific than CMS->MEDICAID.

These may remain contextual features but should not independently increment evidence_family_count.

## Shadow promotion rule

Initial institutional_target family should be deliberately narrow:
- TRADE_TARIFFS <- USTR OR ITA OR CBP
- FARM_BILL <- USDA
- MEDICAID <- CMS

Do not persist these rows yet. First recompute v3 coverage and ranking impact with these rules
as an in-query shadow family. If behavior is stable, add a versioned mapping artifact and
migration plan.

## Audit correction

An intermediate combination query accidentally used government-entity activities across all
years against a Q4 policy denominator. Those results were discarded immediately. The final
combination metrics above were rerun with both sides restricted to Q4 2025.
