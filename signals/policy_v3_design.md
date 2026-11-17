# Policy Resolver / Pressure v3

## Objective

v3 separates **economic exposure** from **evidence quality**.

The filing allocation answers: "How much disclosed filing value is analytically associated
with this policy?" The evidence layer answers: "How independently supported is the
activity-policy attribution?" These are related but must not be allowed to multiply one another.

## Resolver

For each unique activity-policy pair, v3 derives:

- evidence_count: raw evidence rows.
- evidence_method_count: distinct classifier methods.
- evidence_family_count: distinct independent evidence families.
- independently_corroborated: evidence_family_count >= 2.
- best_confidence: maximum classifier confidence.
- allocation_eligible: same parent/child/context semantics as v2.

Evidence families are derived at query time for the POC to avoid increasing the large evidence
table's storage footprint.

## Independence

bill_anchor and bill-provision refinement both belong to legislative_reference.
A provision can refine which policy a bill reference means, but cannot corroborate itself.

Current families:
- legislative_reference
- activity_text
- corpus_context
- semantic_model

Before production, semantic evidence must be audited to ensure it is not merely an AI
restatement of the same legislative reference. If it is, its family must be legislative_reference.

## Dollar conservation

v3 allocation is intentionally blind to evidence_family_count and independently_corroborated.

For each filing with N eligible leaf policies:

    associated_allocation = disclosed_filing_amount / N

Evidence rows never enter the denominator. Each filing-policy pair is distinct before N is
calculated. Parent suppression remains the same as v2.

Therefore adding another classifier, another bill mention, or a provision refinement cannot
create additional dollars.

## Pressure score

v2 weights:
- 40% allocated exposure
- 30% clients
- 20% activities
- 10% filings

Proposed v3 shadow weights:
- 35% allocated exposure
- 25% clients
- 15% activities
- 10% filings
- 15% independent corroboration quality

Independent corroboration quality is the within-quarter policy share of resolved activities
with at least two independent evidence families, percentile-ranked against other substantive
policies in that quarter.

This is intentionally a ranking-quality input, not an allocation multiplier.

## Promotion criteria

Do not replace v2 until a shadow audit shows:

1. zero unknown evidence methods;
2. zero provision evidence incorrectly treated as independent;
3. every represented filing conserves dollars within $0.01;
4. unique filing-policy and activity-policy pairs;
5. corroboration flag exactly equals evidence_family_count >= 2;
6. v3 and v2 quarterly allocation totals reconcile when the eligible policy universe is unchanged;
7. ranking movement is explainable and not dominated by sparse-policy artifacts.

## Files

- signals/activity_policy_resolver_v3.sql
- signals/filing_policy_allocation_v3.sql
- signals/policy_pressure_v3.sql
- signals/policy_v3_audit.sql

All are shadow/design artifacts. No production tables are changed by committing these files.
