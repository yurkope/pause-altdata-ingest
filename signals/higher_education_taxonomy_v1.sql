-- Higher-education financial-aid taxonomy v1.
-- Design only: DO NOT run directly against production.
--
-- Rationale:
-- Pell Grants, Federal Work-Study, SEOG and general federal student-aid activity are
-- materially distinct from student-loan policy. Existing STUDENT_LOANS is too narrow.
--
-- Parent/child design follows existing allocation semantics:
--   HIGHER_ED_FINANCIAL_AID = substantive parent
--   STUDENT_LOANS          = substantive child
-- When a filing has STUDENT_LOANS, the parent is suppressed from allocation by the
-- existing leaf-selection logic. Pell/general-aid activity can allocate to the parent.
--
-- HIGHER_EDUCATION_ACT remains a statute/context label, not the substantive parent.

BEGIN;

INSERT INTO policy.canonical_policy(
  policy_code,policy_name,policy_level,parent_policy_code,description,is_active,allocation_role
)
VALUES (
  'HIGHER_ED_FINANCIAL_AID',
  'Higher Education Financial Aid',
  'subissue',
  NULL,
  'Federal higher-education financial aid policy including Pell Grants, Federal Work-Study, SEOG and general student-aid eligibility/funding. Loan-specific policy is classified as STUDENT_LOANS.',
  true,
  'parent'
)
ON CONFLICT (policy_code) DO UPDATE SET
  policy_name=EXCLUDED.policy_name,
  policy_level=EXCLUDED.policy_level,
  description=EXCLUDED.description,
  is_active=EXCLUDED.is_active,
  allocation_role=EXCLUDED.allocation_role;

UPDATE policy.canonical_policy
SET parent_policy_code='HIGHER_ED_FINANCIAL_AID'
WHERE policy_code='STUDENT_LOANS';

ROLLBACK;

-- ROLLBACK is intentional. This file documents/tests the proposed migration.
-- Execute through the Neon migration workflow only after audit and approval.
