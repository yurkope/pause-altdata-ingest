-- Bill Provision -> Canonical Policy Evidence Bridge v1
-- Purpose: promote validated bill/provision observations into the existing policy v2
-- evidence model without attributing filing dollars to a specific provision.
--
-- Prerequisite:
--   Load the final validator/adjudicator CSV output into
--   policy.bill_provision_validation_stage with the columns used below.
--
-- Promotion policy:
--   * Fully validated current/historical bill provisions are eligible.
--   * Root-only hierarchical references remain review evidence and are NOT promoted.
--   * Underlying-authority / other-bill / rejected observations are NOT promoted.
--   * The canonical policy comes from the audited bill-policy-v2 map.
--   * Provision evidence strengthens/qualifies activity-policy evidence; dollar allocation
--     remains at filing x policy grain in policy.filing_policy_allocation_v2.

BEGIN;

-- Add a first-class evidence method rather than disguising provision evidence as bill_anchor.
ALTER TABLE policy.activity_policy_evidence
  DROP CONSTRAINT IF EXISTS activity_policy_evidence_evidence_method_check;

ALTER TABLE policy.activity_policy_evidence
  ADD CONSTRAINT activity_policy_evidence_evidence_method_check
  CHECK (evidence_method IN ('bill_anchor','bill_provision','activity_regex','corpus_anchor','semantic'));

CREATE TABLE IF NOT EXISTS policy.bill_provision_validation_stage (
  activity_id bigint NOT NULL,
  validation_congress integer NOT NULL,
  owner_bill_type text NOT NULL,
  owner_bill_number integer NOT NULL,
  normalized_section text NOT NULL,
  official_validation_status text NOT NULL,
  official_heading text,
  clause_text text,
  final_status text,
  final_decision_method text,
  ai_status text,
  ai_confidence numeric(4,3),
  loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS bill_provision_validation_stage_activity_idx
  ON policy.bill_provision_validation_stage(activity_id);
CREATE INDEX IF NOT EXISTS bill_provision_validation_stage_bill_idx
  ON policy.bill_provision_validation_stage(validation_congress,owner_bill_type,owner_bill_number);

-- Audit view: every staged provision observation and whether it is promotable.
CREATE OR REPLACE VIEW policy.bill_provision_policy_bridge_audit_v1 AS
WITH bill_policy_map AS (
  SELECT DISTINCT congress,bill_type,bill_number,policy_code
  FROM policy.activity_policy_evidence
  WHERE classifier_version='bill-policy-v2'
    AND evidence_method='bill_anchor'
    AND source_congress IS NOT NULL
),
classified AS (
  SELECT s.*,
         CASE
           WHEN s.official_validation_status IN (
             'VALIDATED_BILL_PROVISION_CURRENT',
             'VALIDATED_BILL_PROVISION_HISTORICAL'
           ) THEN true
           ELSE false
         END AS deterministic_provision_valid,
         CASE
           WHEN s.official_validation_status IN (
             'SECTION_ROOT_EXISTS_SUBSECTION_UNVERIFIED',
             'SECTION_ROOT_EXISTS_HISTORICAL_SUBSECTION_UNVERIFIED'
           ) THEN true ELSE false
         END AS hierarchical_review_only
  FROM policy.bill_provision_validation_stage s
)
SELECT c.*,
       m.policy_code,
       CASE
         WHEN c.deterministic_provision_valid AND m.policy_code IS NOT NULL THEN 'PROMOTE'
         WHEN c.hierarchical_review_only THEN 'REVIEW_HIERARCHICAL'
         WHEN c.deterministic_provision_valid AND m.policy_code IS NULL THEN 'NO_CANONICAL_BILL_MAP'
         ELSE 'DO_NOT_PROMOTE'
       END AS bridge_status
FROM classified c
LEFT JOIN bill_policy_map m
  ON m.congress=c.validation_congress
 AND m.bill_type=upper(c.owner_bill_type)
 AND m.bill_number=c.owner_bill_number;

-- Idempotent promotion. Confidence is intentionally 1.000 only for official-text
-- validated bill provisions. It represents evidence attribution confidence, not policy impact.
DELETE FROM policy.activity_policy_evidence
WHERE classifier_version='bill-provision-policy-v1';

INSERT INTO policy.activity_policy_evidence(
  activity_id,policy_code,evidence_method,confidence,evidence_key,evidence_text,
  classifier_version,source_congress,source_bill_type,source_bill_number
)
SELECT DISTINCT
  activity_id,
  policy_code,
  'bill_provision',
  1.000::numeric(4,3),
  format('bill-provision:%s:%s:%s:%s',
         validation_congress,upper(owner_bill_type),owner_bill_number,lower(normalized_section)),
  left(concat_ws(' | ',
       format('%s %s section %s',upper(owner_bill_type),owner_bill_number,normalized_section),
       nullif(official_heading,''),
       nullif(clause_text,'')),2000),
  'bill-provision-policy-v1',
  validation_congress,
  upper(owner_bill_type),
  owner_bill_number
FROM policy.bill_provision_policy_bridge_audit_v1
WHERE bridge_status='PROMOTE';

COMMIT;

-- Review before rebuilding the v2 resolver/allocation.
SELECT bridge_status,count(*) rows,count(distinct activity_id) activities
FROM policy.bill_provision_policy_bridge_audit_v1
GROUP BY 1 ORDER BY 1;

SELECT policy_code,count(*) evidence_rows,count(distinct activity_id) activities
FROM policy.activity_policy_evidence
WHERE classifier_version='bill-provision-policy-v1'
GROUP BY 1 ORDER BY evidence_rows DESC,policy_code;
