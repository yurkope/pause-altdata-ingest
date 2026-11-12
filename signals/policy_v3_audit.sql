-- Policy resolver/scoring v3 audit gates.
-- READ-ONLY. Run only after creating v3 shadow objects in a migration/temp branch.
-- No production promotion until all hard gates pass.

-- GATE 1: every evidence method must have an explicit independence family.
WITH mapped AS (
    SELECT
        evidence_method,
        CASE
            WHEN evidence_method='bill_anchor' THEN 'legislative_reference'
            WHEN evidence_method='activity_regex' THEN 'activity_text'
            WHEN evidence_method='corpus_anchor' THEN 'corpus_context'
            WHEN evidence_method='semantic' THEN 'semantic_model'
            ELSE 'unknown'
        END family
    FROM policy.activity_policy_evidence
)
SELECT 'unknown_evidence_method' AS gate,
       count(*) AS violations,
       string_agg(DISTINCT evidence_method,',' ORDER BY evidence_method)
           FILTER (WHERE family='unknown') AS detail
FROM mapped
WHERE family='unknown';

-- GATE 2: provision refinement must not masquerade as independent corroboration.
-- Current POC design stores provision mapping outside activity_policy_evidence,
-- so this should remain zero unless a future migration changes that architecture.
SELECT 'provision_as_evidence_method' AS gate,
       count(*) AS violations
FROM policy.activity_policy_evidence
WHERE evidence_method IN ('bill_provision','provision_anchor');

-- GATE 3: each v3 filing allocation must conserve the filing disclosed amount.
WITH chk AS (
    SELECT
        a.filing_uuid,
        SUM(a.associated_allocation)::numeric AS allocated,
        COALESCE(f.income,f.expenses,0)::numeric AS disclosed
    FROM policy.filing_policy_allocation_v3_shadow a
    JOIN lda.filing f USING(filing_uuid)
    GROUP BY a.filing_uuid,f.income,f.expenses
)
SELECT 'filing_dollar_conservation' AS gate,
       count(*) FILTER (
           WHERE abs(allocated-disclosed) > 0.01
       ) AS violations,
       max(abs(allocated-disclosed)) AS max_abs_difference
FROM chk;

-- GATE 4: evidence multiplicity cannot change a filing-policy allocation.
-- Every filing-policy pair must be unique.
SELECT 'duplicate_filing_policy_pair' AS gate,
       count(*) AS violations
FROM (
    SELECT filing_uuid,policy_code
    FROM policy.filing_policy_allocation_v3_shadow
    GROUP BY 1,2
    HAVING count(*) > 1
) x;

-- GATE 5: resolver pair uniqueness.
SELECT 'duplicate_activity_policy_pair' AS gate,
       count(*) AS violations
FROM (
    SELECT activity_id,policy_code
    FROM policy.activity_policy_resolved_v3_shadow
    GROUP BY 1,2
    HAVING count(*) > 1
) x;

-- GATE 6: independent corroboration definition is exact.
SELECT 'bad_independent_corroboration_flag' AS gate,
       count(*) AS violations
FROM policy.activity_policy_resolved_v3_shadow
WHERE independently_corroborated IS DISTINCT FROM (evidence_family_count >= 2);

-- DIAGNOSTIC A: quantify false corroboration removed from v2 semantics.
SELECT
    count(*) AS activity_policy_pairs,
    count(*) FILTER (WHERE legacy_method_corroborated) AS legacy_corroborated,
    count(*) FILTER (WHERE independently_corroborated) AS independently_corroborated,
    count(*) FILTER (
        WHERE legacy_method_corroborated
          AND NOT independently_corroborated
    ) AS legacy_false_corroboration_removed
FROM policy.activity_policy_resolved_v3_shadow;

-- DIAGNOSTIC B: evidence-family distribution.
SELECT
    evidence_family_count,
    count(*) AS pairs,
    count(*) FILTER (WHERE independently_corroborated) AS corroborated_pairs
FROM policy.activity_policy_resolved_v3_shadow
GROUP BY 1
ORDER BY 1;

-- DIAGNOSTIC C: v2 vs v3 allocation totals by quarter.
-- Dollar totals should match if the eligible policy universe is unchanged.
SELECT
    COALESCE(v2.filing_type,v3.filing_type) AS filing_type,
    v2.v2_allocation,
    v3.v3_allocation,
    round((v3.v3_allocation-v2.v2_allocation)::numeric,2) AS difference
FROM (
    SELECT filing_type,round(sum(associated_allocation),2) AS v2_allocation
    FROM policy.filing_policy_allocation_v2
    GROUP BY 1
) v2
FULL OUTER JOIN (
    SELECT filing_type,round(sum(associated_allocation),2) AS v3_allocation
    FROM policy.filing_policy_allocation_v3_shadow
    GROUP BY 1
) v3 USING(filing_type)
ORDER BY 1;

-- DIAGNOSTIC D: policy ranking movement once quality enters the score.
-- Run after materializing policy_pressure_v3_shadow from policy_pressure_v3.sql.
-- Compare score/rank movement; do NOT require equality because scoring intentionally changes.
