-- Bill Provision -> Canonical Policy Evidence Bridge v1
-- Storage-light POC design.
--
-- This file intentionally creates NO permanent staging/audit table.
-- The validated provision CSV remains the transient artifact. The importer should create
-- a TEMP table for the current session, COPY only the small fields required below, audit,
-- promote, and let the TEMP table disappear automatically.
--
-- IMPORTANT semantic rule:
-- provision evidence strengthens activity -> canonical-policy attribution.
-- It does NOT mean disclosed filing dollars were spent on a bill section.
-- Dollar allocation remains filing x policy in policy.filing_policy_allocation_v2.

BEGIN;

-- One small durable schema change: make provision evidence first-class.
ALTER TABLE policy.activity_policy_evidence
  DROP CONSTRAINT IF EXISTS activity_policy_evidence_evidence_method_check;

ALTER TABLE policy.activity_policy_evidence
  ADD CONSTRAINT activity_policy_evidence_evidence_method_check
  CHECK (evidence_method IN (
    'bill_anchor','bill_provision','activity_regex','corpus_anchor','semantic'
  ));

-- Require bill identity for both bill-level and provision-level evidence.
ALTER TABLE policy.activity_policy_evidence
  DROP CONSTRAINT IF EXISTS activity_policy_evidence_check;

ALTER TABLE policy.activity_policy_evidence
  ADD CONSTRAINT activity_policy_evidence_check
  CHECK (
    (
      evidence_method IN ('bill_anchor','bill_provision')
      AND source_congress IS NOT NULL
      AND source_bill_type IS NOT NULL
      AND source_bill_number IS NOT NULL
    )
    OR evidence_method NOT IN ('bill_anchor','bill_provision')
  );

COMMIT;

-- ---------------------------------------------------------------------------
-- SESSION-LOCAL IMPORT / AUDIT / PROMOTION
-- Run the remainder in the same psql session after loading the CSV into the
-- TEMP table. No indexes are needed for ~100 rows.
-- ---------------------------------------------------------------------------

CREATE TEMP TABLE bill_provision_validation_stage (
  activity_id bigint NOT NULL,
  validation_congress integer NOT NULL,
  owner_bill_type text NOT NULL,
  owner_bill_number integer NOT NULL,
  normalized_section text NOT NULL,
  official_validation_status text NOT NULL,
  official_section_heading text
) ON COMMIT PRESERVE ROWS;

-- Example psql load after creating a compact CSV with these seven columns:
-- \copy bill_provision_validation_stage(
--   activity_id,validation_congress,owner_bill_type,owner_bill_number,
--   normalized_section,official_validation_status,official_section_heading
-- ) FROM 'bill_provision_policy_bridge.csv' CSV HEADER;

-- AUDIT 1: classification of every staged observation.
WITH bill_policy_map AS (
  SELECT DISTINCT source_congress congress,source_bill_type bill_type,
                  source_bill_number bill_number,policy_code
  FROM policy.activity_policy_evidence
  WHERE classifier_version='bill-policy-v2'
    AND evidence_method='bill_anchor'
),
bridge AS (
  SELECT s.*,m.policy_code,
    CASE
      WHEN s.official_validation_status IN (
        'VALIDATED_BILL_PROVISION_CURRENT','VALIDATED_BILL_PROVISION_HISTORICAL'
      ) AND m.policy_code IS NOT NULL THEN 'PROMOTE'
      WHEN s.official_validation_status IN (
        'SECTION_ROOT_EXISTS_SUBSECTION_UNVERIFIED',
        'SECTION_ROOT_EXISTS_HISTORICAL_SUBSECTION_UNVERIFIED'
      ) THEN 'REVIEW_HIERARCHICAL'
      WHEN s.official_validation_status IN (
        'VALIDATED_BILL_PROVISION_CURRENT','VALIDATED_BILL_PROVISION_HISTORICAL'
      ) THEN 'NO_CANONICAL_BILL_MAP'
      ELSE 'DO_NOT_PROMOTE'
    END bridge_status
  FROM bill_provision_validation_stage s
  LEFT JOIN bill_policy_map m
    ON m.congress=s.validation_congress
   AND m.bill_type=upper(s.owner_bill_type)
   AND m.bill_number=s.owner_bill_number
)
SELECT bridge_status,count(*) rows,count(distinct activity_id) activities
FROM bridge GROUP BY 1 ORDER BY 1;

-- AUDIT 2: exact rows that would become durable evidence.
WITH bill_policy_map AS (
  SELECT DISTINCT source_congress congress,source_bill_type bill_type,
                  source_bill_number bill_number,policy_code
  FROM policy.activity_policy_evidence
  WHERE classifier_version='bill-policy-v2'
    AND evidence_method='bill_anchor'
)
SELECT s.activity_id,s.validation_congress,upper(s.owner_bill_type) bill_type,
       s.owner_bill_number,s.normalized_section,m.policy_code,
       s.official_validation_status,s.official_section_heading
FROM bill_provision_validation_stage s
JOIN bill_policy_map m
  ON m.congress=s.validation_congress
 AND m.bill_type=upper(s.owner_bill_type)
 AND m.bill_number=s.owner_bill_number
WHERE s.official_validation_status IN (
  'VALIDATED_BILL_PROVISION_CURRENT','VALIDATED_BILL_PROVISION_HISTORICAL'
)
ORDER BY s.activity_id,m.policy_code,s.normalized_section;

-- PROMOTION (run only after audits pass).
-- The evidence text is deliberately compact to minimize storage.
-- DELETE makes reruns idempotent.
--
-- DELETE FROM policy.activity_policy_evidence
-- WHERE classifier_version='bill-provision-policy-v1';
--
-- INSERT INTO policy.activity_policy_evidence(
--   activity_id,policy_code,evidence_method,confidence,evidence_key,evidence_text,
--   classifier_version,source_congress,source_bill_type,source_bill_number
-- )
-- SELECT DISTINCT
--   s.activity_id,m.policy_code,'bill_provision',1.000::numeric(4,3),
--   format('bill-provision:%s:%s:%s:%s',
--          s.validation_congress,upper(s.owner_bill_type),
--          s.owner_bill_number,lower(s.normalized_section)),
--   left(concat_ws(' | ',
--     format('%s %s section %s',upper(s.owner_bill_type),
--            s.owner_bill_number,s.normalized_section),
--     nullif(s.official_section_heading,'')),500),
--   'bill-provision-policy-v1',
--   s.validation_congress,upper(s.owner_bill_type),s.owner_bill_number
-- FROM bill_provision_validation_stage s
-- JOIN (
--   SELECT DISTINCT source_congress congress,source_bill_type bill_type,
--                   source_bill_number bill_number,policy_code
--   FROM policy.activity_policy_evidence
--   WHERE classifier_version='bill-policy-v2' AND evidence_method='bill_anchor'
-- ) m
--   ON m.congress=s.validation_congress
--  AND m.bill_type=upper(s.owner_bill_type)
--  AND m.bill_number=s.owner_bill_number
-- WHERE s.official_validation_status IN (
--   'VALIDATED_BILL_PROVISION_CURRENT','VALIDATED_BILL_PROVISION_HISTORICAL'
-- );
