# QUANT PROJECT — CURRENT HANDOFF / RESTART

Last updated: 2026-10-29
Purpose: Fast, safe continuation after a lost chat/session.

## READ THIS FIRST

This file is the current continuation guide.

Source-of-truth order:
1. Current GitHub code on the active working branch + live Neon production state.
2. This CURRENT_HANDOFF.md for project state, decisions, and resume point.
3. restart/quant_clean_restart.txt for the validated April 1 baseline.
4. restart/quant_full_framework.txt for modeling philosophy and long-term design.
5. restart/quant.txt is historical development notes only. It contains contradictions and MUST NOT be used as current implementation truth.

Do not roll the project back to the April resume point just because the older restart documents say daily aggregation/momentum was next. The project has advanced substantially.

## PROJECT OBJECTIVE

Build a forward-looking policy-pressure system from U.S. lobbying data.

Core mental model retained from the original framework:
- lobbying spend = capital flow
- policy issues = markets/assets
- changing attention/pressure = momentum
- goal = detect policy pressure before outcomes become obvious

Current implementation has evolved beyond raw issue codes into:
lobbying filing/activity
→ bill/reference resolution
→ bill/provision validation
→ canonical policy attribution
→ policy-level allocation/activity breadth
→ pressure/momentum/convergence signals.

## OPERATING PRINCIPLES

- POC is zero/near-zero incremental cost. Do not add paid infrastructure/services without explicit approval.
- Prefer deterministic parsing/validation first; AI is an exception handler for semantic ambiguity.
- Do not silently promote AI classifications.
- Preserve total filing-dollar allocation; policy attribution must not multiply disclosed spend.
- Provision evidence must not automatically cause a second dollar allocation.
- Do not force ambiguous or omnibus bills into a single substantive policy.
- No destructive production SQL without explicit approval.
- Production schema changes should use a Neon migration/temp branch, test first, then approval before completion.
- Read-only production analysis is acceptable.
- Staging/audit tables may be intentionally absent: stable import staging was removed to conserve Neon storage.
- Prefer local/temp/transient artifacts where durable storage adds little POC value.

## LIVE DATABASE

Neon org: PolicySignal
Project ID: wispy-bird-56636783
Production branch ID: br-patient-math-aqzqyzxv
Database: neondb
PostgreSQL: 17

At 2026-10-29 inspection:
- project logical size ~959 MB
- branch logical size limit 1 GiB
Storage is tight. Avoid unnecessary durable tables/evidence duplication.

Important production objects:
- lda.activity
- lda.filing
- policy.bill
- policy.bill_mention
- policy.bill_committee
- policy.bill_member
- policy.canonical_policy
- policy.activity_policy_evidence
- policy.activity_policy_resolved_v2
- policy.filing_policy_allocation_v2
- policy.policy_pressure_v2
- policy.policy_committee_convergence_v1

Current allocation remains conserved. Do not reinterpret allocated policy dollars as direct spend on a bill or policy.

## GITHUB

Repository: yurkope/pause-altdata-ingest
Active Congress/policy working branch: vibe-lda-ingestion

Important recent commits:
- 26b2237 — official-heading external-authority guard
- 0748e5b — hierarchical section-reference validation
- 29c749d — include hierarchy statuses in AI adjudication
- db191f8 — storage-light provision bridge design
- 51a329f — compact provision bridge exporter
- 683e8fe — read-only provision-policy bridge auditor
- 79536ad — expanded unmapped provision-bill audit

Important files:
- sources/congress/validate_clause_bill_sections.py
- sources/congress/ai_adjudicate_bill_provisions.py
- sources/congress/export_bill_provision_policy_bridge.py
- sources/congress/audit_bill_provision_policy_bridge.py
- signals/bill_provision_policy_bridge_v1.sql
- signals/bill_policy_v2_manifest.csv
- signals/bill_policy_v2_promotion.sql
- signals/policy_pressure_v2.sql

## BILL-PROVISION VALIDATION — CURRENT STATE

Final deterministic population: 104 observations.

Statuses:
- REJECTED_BILL_PROVISION_CANDIDATE: 53
- UNDERLYING_AUTHORITY_SECTION: 18
- VALIDATED_BILL_PROVISION_CURRENT: 18
- OFFICIAL_TEXT_AMENDS_UNDERLYING_AUTHORITY: 7
- VALIDATED_BILL_PROVISION_HISTORICAL: 7
- SECTION_ROOT_EXISTS_SUBSECTION_UNVERIFIED: 1

Fully promotable deterministic provision pool = 25:
18 current + 7 historical.

Do not promote:
- rejected candidates
- underlying-authority references
- official-text amendments to underlying authority
- root-only subsection-unverified observations.

Hierarchical example:
H.R.5878 §4(a)(B) has root §4 confirmed but nested subsection not independently verified. AI classified BILL_PROVISION below 0.90, so deterministic root-only status was retained. Do not lower the 0.90 threshold merely to promote it.

## AI ADJUDICATION DESIGN

AI labels:
- BILL_PROVISION
- UNDERLYING_AUTHORITY
- OTHER_BILL_PROVISION
- AMBIGUOUS
- NOT_A_PROVISION_REFERENCE

AI is not allowed to infer:
- lobbying contact
- causality
- support/opposition unless separately and explicitly modeled
- legislative impact

Confidence below 0.90 retains deterministic result.
Confidence >=0.90 is review-required, not silent promotion.

Do not send every record through AI. Use it for ambiguity/exceptions.

## PROVISION → POLICY AUDIT RESULTS

Compact exporter produced:
- 104 rows
- 0 skipped

Read-only audit of the 25 fully validated provisions:
- candidate policy rows: 25
- mapped through existing bill-policy-v2: 7
- unmapped: 18
- already resolved: 3
- genuinely new activity-policy rows: 4
- existing but not yet corroborated: 3
- distinct validated activities: 20
- distinct mapped policies: 4

Mapped policy counts:
- FARM_BILL: 4
- ONLINE_SAFETY_PRIVACY: 1
- DEFENSE_AUTHORIZATION: 1
- PERMITTING_NEPA: 1

All 4 genuinely new activity-policy rows are 118th Congress H.R.8467 §12007 → FARM_BILL.

## THE IMPORTANT DISCOVERY: OMNIBUS BILLS REQUIRE PROVISION-LEVEL POLICY

18 unmapped provision observations break down as:
- H.R.1 (119th): 16 observations, 11 activities, 8 sections
- H.R.5878: 1 observation, §7
- H.R.5039: 1 observation, §2

H.R.1 sections observed:
112022, 112025, 112032, 70415, 71112, 71115, 71116, 83001.

This is NOT evidence that H.R.1 should receive one broad substantive bill-policy mapping.

It demonstrates that mega/omnibus bills can span multiple unrelated policy domains and should use:
bill + provision → canonical policy.

Examples found in production activity text:
- §§71112/71115/71116: Medicaid financing/provider taxes/state-directed payments.
- §112025: UBIT provision; activity text explicitly says OPPOSED.
- §112032: limitation on drawback of taxes paid for substituted merchandise.
- §70415: excise tax on investment income of certain private colleges/universities.
- §83001: Pell Grant eligibility.

One §70415 activity had no existing canonical policy evidence, showing real incremental value from provision-level attribution.

Architecture decision:
- focused bills may inherit a deterministic bill-level policy mapping.
- mega/omnibus bills must not be forced into one substantive policy.
- use deterministic bill+section → policy mapping where evidence is strong.
- unresolved provision-policy mappings remain unresolved or go to controlled AI/review.

## OTHER UNMAPPED CASES

H.R.5878, HOME Reform Act of 2025:
- exists in policy.bill
- provision §7 is a genuine housing provision
- bill has no current bill-policy anchor
- likely taxonomy/map coverage gap; evaluate deterministically.

H.R.5039, Wheelchair Right to Repair Act:
- activity text explicitly names H.R.5039 and §2
- bill was absent from policy.bill / bill_mention at inspection
- likely bill-ingestion/resolution coverage gap, not merely a taxonomy gap.

## CURRENT RESUME POINT — DO THIS NEXT

We are building the deterministic provision-policy layer.

Next work:
1. Inspect the exact schema/current codes in policy.canonical_policy before inventing any new policy labels.
2. Build a provision-policy mapping artifact keyed by:
   congress, bill_type, bill_number, section_ref, policy_code.
3. Seed only high-confidence deterministic mappings.
4. Reuse existing canonical policy codes where semantically correct.
5. For H.R.1, map individual sections, NOT the whole bill.
6. Audit incremental activity-policy coverage/corroboration before any production insert.
7. Decide whether the incremental signal value justifies durable bill_provision evidence given Neon storage pressure.
8. If a production evidence-method/schema change is justified, use Neon migration workflow and obtain approval before production completion.

Do NOT run signals/bill_policy_v2_promotion.sql blindly. It rebuilds/swaps production objects.

## IMPORTANT FUTURE CLEANUPS — NOT CURRENT BLOCKERS

- Propagate structured owner_congress through the Congress pipeline instead of reparsing Congress from clause prose.
- Centralize section normalization and regression tests.
- Add tests for:
  - section 4-water → 4
  - explicit 118th Congress
  - external-authority headings
  - nested 4(a)(B)
- Lazy-import OpenAI in ai_adjudicate_bill_provisions.py so dry-run does not require SDK.
- Add OpenAI vars to .env.example.
- Consider structured-output JSON only after checking current official OpenAI API documentation.
- Do not expose or commit secrets. Root .env remains local and ignored.

## CONTINUATION CHECKLIST FOR A FUTURE ASSISTANT

Before changing code:
1. Read this file.
2. Inspect current GitHub branch/file state; commits may have advanced beyond this document.
3. Inspect live Neon state read-only if database state matters.
4. Treat older restart docs as baseline/history according to the hierarchy above.
5. Compare current implementation to this handoff; do not assume this handoff is newer than Git history.
6. Continue autonomously where no product/business decision is needed.
7. Ask before paid services, destructive production actions, or consequential schema promotion.

## USER WORKING PREFERENCE

Act as project engineer/coder/project manager. Keep the project moving autonomously where user input is not needed. Stop for genuine business/product choices, inaccessible local-only artifacts, cost commitments, or destructive production changes requiring approval.


## UPDATE — PROVISION POLICY IMPLEMENTATION (2026-10-29)

Resolver inspection found that production currently defines corroboration as:
COUNT(DISTINCT evidence_method) > 1.

Therefore bill_anchor + bill_provision MUST NOT be treated as independent corroboration when
the provision is a refinement of the same bill evidence. The earlier idea of adding
'bill_provision' to activity_policy_evidence as a new evidence_method was retired before
production changes.

Current design:
- provision policy is a refinement/mapping layer;
- no production schema change;
- no durable provision evidence rows yet;
- no extra filing-dollar allocation;
- omnibus bills use bill + section policy mappings.

New controlled artifact:
- signals/bill_provision_policy_v1_manifest.csv

Initial mappings include deterministic MEDICAID, HOUSING and RIGHT_TO_REPAIR cases.
Imperfect H.R.1 tax/education mappings are explicitly marked REVIEW rather than forced.

Relevant commits:
- 5573168 — add provision-level policy mapping manifest
- 01f7613 — prevent false corroboration from provision evidence

Next:
1. audit the manifest against the 25 validated provision observations;
2. quantify incremental policy coverage under refinement semantics;
3. inspect REVIEW mappings and decide whether existing taxonomy is sufficient or narrowly
   justified new substantive policy codes are needed;
4. keep production unchanged until that audit is complete.


## UPDATE — 2026-10-30: STORAGE + LOW-FREQUENCY BILL RECOVERY

Neon sent a storage alert on 2026-10-29: project wispy-bird-56636783 reached 100% of its
1.07 GB monthly storage allowance. The allowance is expected to reset at the next monthly
period. User chose not to upgrade for the POC. Until reset:
- prefer GitHub/local work;
- avoid unnecessary Neon queries;
- no emergency deletion;
- no paid-plan upgrade;
- production remains unchanged.

H.R.5039 diagnosis:
- official provision validation found a legitimate H.R.5039 section 2 reference;
- H.R.5039 was absent from policy.bill;
- transition resolver only processed ranked[:--top] (default 1000);
- low-frequency bills can therefore validate later yet never enter the bill corpus.

Fix:
- resolve_bills_transition.py now accepts repeatable --bill targets, e.g. --bill HR5039;
- forced targets append to the bounded resolver queue without expanding broad discovery;
- export_validated_bill_targets.py converts fully validated provision observations into
  targeted resolver arguments;
- only VALIDATED_BILL_PROVISION_CURRENT/HISTORICAL qualify;
- regression tests prevent rejected, underlying-authority, or root-only observations from
  forcing ingestion.

Reference normalization:
- sources/congress/reference_normalization.py now centralizes section token parsing/root logic;
- resolver and validator use the shared helper;
- tests cover section 4-water -> 4, nested 4(a)(B), 12007, and 505g(c).

Relevant commits:
- 899e549 — targeted low-frequency bill resolution
- b3badd1 — targeted resolver regression tests
- cb27528 — validated bill target exporter
- 61b60cd — feedback exporter tests
- 1bd22c9 — shared reference normalization
- 2dc6ec4 — normalization regression tests
- ca329ce — resolver uses shared normalization
- 457b3c9 — validator uses shared normalization

Current Congress pipeline:
LDA reference -> bill resolution -> provision ownership -> official validation
-> low-frequency validated-bill feedback -> targeted bill resolution
-> provision-policy mapping.

Next after Neon reset:
1. run targeted resolution for H.R.5039;
2. verify it enters policy.bill/bill_mention correctly;
3. run provision-policy refinement audit;
4. evaluate higher-ed taxonomy promotion only after audit.
