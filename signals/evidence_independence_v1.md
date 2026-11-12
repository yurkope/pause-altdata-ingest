# Evidence Independence Model v1

## Purpose

The Quant policy signal must distinguish **evidence volume** from **independent corroboration**.
Multiple observations derived from the same underlying legislative reference must not make a
policy attribution look independently confirmed.

## Evidence families

| evidence_method | evidence_family | independent meaning |
|---|---|---|
| bill_anchor | legislative_reference | Bill identity mapped to canonical policy |
| bill_provision refinement | legislative_reference | More specific interpretation of the same bill reference; NOT independent of bill_anchor |
| activity_regex | activity_text | Direct policy language in the LDA activity text |
| corpus_anchor | corpus_context | Repeated/contextual corpus evidence |
| semantic | semantic_model | Model-derived semantic classification |

## Corroboration rule

A resolved activity-policy pair is independently corroborated only when it has at least two
distinct **evidence families**, not merely two evidence methods.

Examples:

- bill_anchor + bill_provision = 1 family -> not independently corroborated.
- bill_anchor + activity_regex = 2 families -> corroborated.
- activity_regex + corpus_anchor = 2 families -> corroborated.
- bill_provision + semantic = 2 families only if semantic classification is generated from
  information independent of the provision/bill derivation. If the semantic model is merely
  adjudicating that same provision reference, it remains legislative_reference.

## Provision semantics

Provision mapping is a refinement layer:
1. preserve the validated bill/provision identity;
2. map the specific provision to a canonical policy;
3. refine a broad or missing bill-level policy attribution;
4. do not add filing dollars;
5. do not create independent corroboration by itself.

## Implementation direction

Do not change production until migration audit passes.

Future resolver should calculate:
- evidence_method_count
- evidence_family_count
- independently_corroborated = evidence_family_count >= 2

For backward compatibility, the existing corroborated field can be retained during migration,
but policy-pressure scoring should eventually use independently_corroborated.

## POC storage rule

Prefer deriving evidence_family in SQL from evidence_method/classifier metadata rather than
adding another large column to policy.activity_policy_evidence while Neon storage is constrained.
